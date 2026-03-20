"""
Job Queue Client
- Connects to the server over TCP + SSL
- Submits jobs and polls for results
- Usage: python client.py --host localhost --port 9000 --jobs 5
"""

import socket
import ssl
import json
import time
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CLIENT] %(message)s")

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


def connect(host, port) -> ssl.SSLSocket:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(CERTFILE)
    ctx.check_hostname = False  # self-signed cert
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = ctx.wrap_socket(raw, server_hostname=host)
    conn.connect((host, port))
    # Identify as client
    send_msg(conn, {"role": "client"})
    return conn


def submit_job(conn, payload: str) -> str:
    send_msg(conn, {"action": "submit", "payload": payload})
    resp = recv_msg(conn)
    if resp and resp.get("status") == "ok":
        return resp["job_id"]
    raise RuntimeError(f"Submit failed: {resp}")


def poll_status(conn, job_id: str, timeout=60) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        send_msg(conn, {"action": "status", "job_id": job_id})
        resp = recv_msg(conn)
        if resp and resp["state"] in ("DONE", "FAILED"):
            return resp
        time.sleep(1)
    return {"state": "TIMEOUT", "result": None}


def get_metrics(conn) -> dict:
    send_msg(conn, {"action": "metrics"})
    resp = recv_msg(conn)
    return resp.get("metrics", {}) if resp else {}


def main():
    parser = argparse.ArgumentParser(description="Job Queue Client")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--jobs", type=int, default=3, help="Number of jobs to submit")
    parser.add_argument("--payload", default="echo Hello from job", help="Job payload/command")
    parser.add_argument("--metrics", action="store_true", help="Fetch server metrics after jobs")
    args = parser.parse_args()

    conn = connect(args.host, args.port)
    logging.info(f"Connected to {args.host}:{args.port}")

    job_ids = []
    for i in range(args.jobs):
        payload = f"{args.payload} #{i+1}"
        job_id = submit_job(conn, payload)
        logging.info(f"Submitted job {i+1}/{args.jobs}: {job_id}")
        job_ids.append(job_id)

    logging.info("Polling for results...")
    for job_id in job_ids:
        result = poll_status(conn, job_id)
        logging.info(f"Job {job_id}: state={result['state']}, result={result.get('result')}")

    if args.metrics:
        m = get_metrics(conn)
        logging.info(f"Server metrics: {m}")

    conn.close()


if __name__ == "__main__":
    main()
