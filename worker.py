#!/usr/bin/env python3
"""
Distributed Job Queue – Worker Node
- Connects to server via TCP + SSL/TLS
- Registers as WORKER
- Fetches jobs, executes them, reports results
- Sends periodic heartbeats
"""

import socket
import ssl
import json
import time
import threading
import logging
import sys
import os
import math
import random

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WORKER] %(message)s",
    handlers=[
        logging.FileHandler("logs/worker.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("worker")

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9000
CERTFILE    = "certs/server.crt"
HEARTBEAT_INTERVAL = 5  # seconds

# Per-connection send lock to prevent concurrent SSL writes
_send_locks = {}
_send_locks_lock = threading.Lock()


def _get_send_lock(conn):
    cid = id(conn)
    with _send_locks_lock:
        if cid not in _send_locks:
            _send_locks[cid] = threading.Lock()
        return _send_locks[cid]


def send_msg(conn, msg):
    lock = _get_send_lock(conn)
    with lock:
        data   = json.dumps(msg).encode()
        length = len(data).to_bytes(4, "big")
        conn.sendall(length + data)


def recv_msg(conn):
    raw_len = _recv_exact(conn, 4)
    if not raw_len:
        return None
    length = int.from_bytes(raw_len, "big")
    data   = _recv_exact(conn, length)
    return json.loads(data.decode()) if data else None


def _recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ─── Job Execution Engine ──────────────────────────────────────────────────────
def execute_job(payload: str) -> str:
    """
    Simulate real job execution.
    Payload format: <TYPE>:<DATA>
    Supported types: COMPUTE, SLEEP, HASH, FIBONACCI
    """
    try:
        if ":" in payload:
            job_type, data = payload.split(":", 1)
        else:
            job_type, data = payload, ""

        job_type = job_type.strip().upper()

        if job_type == "COMPUTE":
            # CPU-bound: compute sum of squares
            n = int(data) if data.isdigit() else 1000
            result = sum(i * i for i in range(n))
            return f"Sum of squares up to {n} = {result}"

        elif job_type == "SLEEP":
            # I/O simulation
            secs = float(data) if data else 1.0
            secs = min(secs, 10)  # cap at 10s
            time.sleep(secs)
            return f"Slept for {secs}s"

        elif job_type == "FIBONACCI":
            n = int(data) if data.isdigit() else 20
            n = min(n, 40)
            a, b = 0, 1
            for _ in range(n):
                a, b = b, a + b
            return f"Fibonacci({n}) = {a}"

        elif job_type == "HASH":
            import hashlib
            h = hashlib.sha256(data.encode()).hexdigest()
            return f"SHA256({data!r}) = {h}"

        elif job_type == "PRIME":
            n = int(data) if data.isdigit() else 100
            n = min(n, 10000)
            primes = [x for x in range(2, n+1)
                      if all(x % i != 0 for i in range(2, int(math.sqrt(x))+1))]
            return f"Primes up to {n}: count={len(primes)}, largest={primes[-1] if primes else 'N/A'}"

        else:
            # Generic echo
            time.sleep(random.uniform(0.1, 0.5))
            return f"Processed: {payload}"

    except Exception as e:
        raise RuntimeError(f"Job execution error: {e}")


# ─── Heartbeat Thread ──────────────────────────────────────────────────────────
def heartbeat_loop(conn, stop_event):
    while not stop_event.is_set():
        try:
            send_msg(conn, {"action": "HEARTBEAT"})
            resp = recv_msg(conn)
            if not resp or resp.get("status") != "OK":
                log.warning("Heartbeat failed.")
                stop_event.set()
                break
        except Exception as e:
            log.warning(f"Heartbeat error: {e}")
            stop_event.set()
            break
        time.sleep(HEARTBEAT_INTERVAL)


# ─── Main Worker Loop ──────────────────────────────────────────────────────────
def run_worker(worker_id=None):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(CERTFILE)
    ctx.check_hostname = False  # self-signed cert

    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = ctx.wrap_socket(raw_sock, server_hostname=SERVER_HOST)
    conn.connect((SERVER_HOST, SERVER_PORT))
    log.info(f"Connected to server {SERVER_HOST}:{SERVER_PORT}")

    # Handshake: register as worker
    reg_msg = {"role": "WORKER"}
    if worker_id:
        reg_msg["node_id"] = worker_id
    send_msg(conn, reg_msg)
    resp = recv_msg(conn)
    if not resp or resp.get("status") != "OK":
        log.error("Registration failed. Exiting.")
        conn.close()
        return

    my_id = resp.get("node_id", worker_id or "unknown")
    log.info(f"Registered as Worker ID: {my_id}")

    # Start heartbeat thread
    stop_event = threading.Event()
    hb_thread  = threading.Thread(target=heartbeat_loop,
                                  args=(conn, stop_event), daemon=True)
    hb_thread.start()

    try:
        while not stop_event.is_set():
            # Ask for a job
            send_msg(conn, {"action": "FETCH"})
            msg = recv_msg(conn)
            if not msg:
                log.info("Server closed connection.")
                break

            if msg.get("status") == "NO_JOB":
                time.sleep(0.5)
                continue

            if msg.get("action") == "JOB_ASSIGNED":
                job = msg["job"]
                job_id  = job["job_id"]
                payload = job["payload"]
                log.info(f"Executing job {job_id}: {payload!r}")

                try:
                    result = execute_job(payload)
                    send_msg(conn, {"action": "COMPLETE",
                                    "job_id": job_id,
                                    "result": result})
                    ack = recv_msg(conn)
                    if ack and ack.get("status") == "OK":
                        log.info(f"Job {job_id} completed. Result: {result}")
                    else:
                        log.warning(f"Server didn't ACK completion of {job_id}")
                except Exception as e:
                    log.error(f"Job {job_id} failed: {e}")
                    send_msg(conn, {"action": "FAIL",
                                    "job_id": job_id,
                                    "reason": str(e)})
                    recv_msg(conn)

            else:
                log.warning(f"Unexpected message: {msg}")

    except KeyboardInterrupt:
        log.info("Worker interrupted.")
    except Exception as e:
        log.error(f"Worker error: {e}")
    finally:
        stop_event.set()
        conn.close()
        log.info("Worker disconnected.")


if __name__ == "__main__":
    wid = sys.argv[1] if len(sys.argv) > 1 else None
    run_worker(wid)
