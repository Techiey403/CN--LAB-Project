"""
Job Queue Worker
- Connects to the server over TCP + SSL
- Fetches and executes jobs
- Reports results back to server
- Handles ping/keepalive from server
- Usage: python worker.py --host localhost --port 9000
"""

import socket
import ssl
import json
import subprocess
import time
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [WORKER] %(message)s")

CERTFILE = "server.crt"


def send_msg(conn, data: dict):
    raw = json.dumps(data).encode()
    conn.sendall(len(raw).to_bytes(4, "big") + raw)


def recv_msg(conn) -> dict:
    raw_len = _recv_exact(conn, 4)
    if not raw_len:
        return None
    length = int.from_bytes(raw_len, "big")
    raw = _recv_exact(conn, length)
    return json.loads(raw.decode()) if raw else None


def _recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def execute_job(payload: str) -> tuple[bool, str]:
    """
    Execute the job payload as a shell command.
    Returns (success, output).
    """
    try:
        result = subprocess.run(
            payload,
            shell=True,
            capture_output=True,
            text=True,
            timeout=20
        )
        output = result.stdout.strip() or result.stderr.strip()
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "Job timed out"
    except Exception as e:
        return False, str(e)


def connect(host, port) -> tuple[ssl.SSLSocket, str]:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(CERTFILE)
    ctx.check_hostname = False  # self-signed cert
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = ctx.wrap_socket(raw, server_hostname=host)
    conn.connect((host, port))

    # Identify as worker
    send_msg(conn, {"role": "worker"})
    ack = recv_msg(conn)
    if not ack or ack.get("status") != "ok":
        raise RuntimeError(f"Worker registration failed: {ack}")
    worker_id = ack["worker_id"]
    return conn, worker_id


def run_worker(host, port):
    while True:
        try:
            conn, worker_id = connect(host, port)
            logging.info(f"Registered as worker {worker_id}")

            while True:
                msg = recv_msg(conn)
                if msg is None:
                    logging.warning("Server closed connection.")
                    break

                action = msg.get("action")

                if action == "ping":
                    send_msg(conn, {"action": "pong"})

                elif action == "execute":
                    job_id = msg["job_id"]
                    payload = msg["payload"]
                    logging.info(f"Executing job {job_id}: {payload}")

                    start = time.time()
                    success, output = execute_job(payload)
                    elapsed = round(time.time() - start, 3)

                    logging.info(f"Job {job_id} done in {elapsed}s: success={success}")
                    send_msg(conn, {
                        "action": "result",
                        "job_id": job_id,
                        "success": success,
                        "result": output,
                        "elapsed": elapsed,
                    })

                else:
                    logging.warning(f"Unknown action from server: {action}")

        except (ConnectionRefusedError, ssl.SSLError, OSError) as e:
            logging.error(f"Connection error: {e}. Retrying in 5s...")
            time.sleep(5)
        except KeyboardInterrupt:
            logging.info("Worker shutting down.")
            break
        finally:
            try:
                conn.close()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="Job Queue Worker")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    run_worker(args.host, args.port)


if __name__ == "__main__":
    main()
