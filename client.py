#!/usr/bin/env python3
"""
Distributed Job Queue – Client
- Connects to server via TCP + SSL/TLS
- Submits jobs, checks status, views metrics
- Interactive CLI mode or batch mode
"""

import socket
import ssl
import json
import sys
import time
import logging
import os

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CLIENT] %(message)s",
    handlers=[
        logging.FileHandler("logs/client.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("client")

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9000
CERTFILE    = "certs/server.crt"


def send_msg(conn, msg):
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


def connect():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(CERTFILE)
    ctx.check_hostname = False

    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = ctx.wrap_socket(raw_sock, server_hostname=SERVER_HOST)
    conn.connect((SERVER_HOST, SERVER_PORT))

    send_msg(conn, {"role": "CLIENT"})
    resp = recv_msg(conn)
    if not resp or resp.get("status") != "OK":
        raise ConnectionError("Registration failed")
    my_id = resp.get("node_id")
    log.info(f"Connected as Client {my_id}")
    return conn, my_id


def submit_job(conn, payload):
    send_msg(conn, {"action": "SUBMIT", "payload": payload})
    resp = recv_msg(conn)
    return resp


def get_status(conn, job_id):
    send_msg(conn, {"action": "STATUS", "job_id": job_id})
    return recv_msg(conn)


def list_jobs(conn):
    send_msg(conn, {"action": "LIST_JOBS"})
    return recv_msg(conn)


def get_metrics(conn):
    send_msg(conn, {"action": "METRICS"})
    return recv_msg(conn)


def wait_for_job(conn, job_id, poll_interval=0.5, timeout=60):
    """Poll until job is COMPLETED or FAILED."""
    start = time.time()
    while time.time() - start < timeout:
        resp = get_status(conn, job_id)
        if resp and resp.get("status") == "OK":
            job = resp["job"]
            if job["status"] in ("COMPLETED", "FAILED"):
                return job
        time.sleep(poll_interval)
    return None


# ─── Interactive CLI ───────────────────────────────────────────────────────────
def interactive(conn):
    print("\n=== Distributed Job Queue Client ===")
    print("Commands: submit <payload> | status <job_id> | list | metrics | wait <job_id> | quit")
    print("Job types: COMPUTE:<n> | SLEEP:<secs> | FIBONACCI:<n> | HASH:<text> | PRIME:<n>")
    print("Example:  submit COMPUTE:5000\n")

    while True:
        try:
            line = input(">>> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            continue
        parts = line.split(None, 1)
        cmd   = parts[0].lower()

        if cmd == "quit":
            break

        elif cmd == "submit":
            payload = parts[1] if len(parts) > 1 else "SLEEP:1"
            resp = submit_job(conn, payload)
            if resp and resp.get("status") == "OK":
                print(f"  ✓ Job submitted: {resp['job_id']}")
            else:
                print(f"  ✗ Error: {resp}")

        elif cmd == "status":
            job_id = parts[1] if len(parts) > 1 else ""
            resp = get_status(conn, job_id)
            if resp and resp.get("status") == "OK":
                job = resp["job"]
                print(f"  Job {job['job_id']}")
                print(f"    Status  : {job['status']}")
                print(f"    Payload : {job['payload']}")
                print(f"    Worker  : {job['worker_id']}")
                print(f"    Result  : {job['result']}")
            else:
                print(f"  ✗ {resp}")

        elif cmd == "wait":
            job_id = parts[1] if len(parts) > 1 else ""
            print(f"  Waiting for job {job_id}...")
            job = wait_for_job(conn, job_id)
            if job:
                print(f"  ✓ {job['status']} → {job['result']}")
            else:
                print("  Timed out.")

        elif cmd == "list":
            resp = list_jobs(conn)
            if resp and resp.get("status") == "OK":
                jobs = resp["jobs"]
                print(f"  {'JOB ID':<38} {'STATUS':<12} {'PAYLOAD':<25} RESULT")
                print("  " + "-"*90)
                for j in jobs:
                    print(f"  {j['job_id']:<38} {j['status']:<12} "
                          f"{j['payload'][:24]:<25} {str(j['result'])[:40]}")
            else:
                print(f"  ✗ {resp}")

        elif cmd == "metrics":
            resp = get_metrics(conn)
            if resp and resp.get("status") == "OK":
                m = resp["metrics"]
                print(f"  Jobs Submitted  : {m['jobs_submitted']}")
                print(f"  Jobs Completed  : {m['jobs_completed']}")
                print(f"  Jobs Failed     : {m['jobs_failed']}")
                print(f"  Jobs Re-queued  : {m['jobs_requeued']}")
                print(f"  Queue Depth     : {m['queue_depth']}")
                print(f"  Active Workers  : {m['active_workers']}")
                print(f"  Avg Latency     : {m['avg_latency_sec']}s")
            else:
                print(f"  ✗ {resp}")

        else:
            print(f"  Unknown command: {cmd}")


# ─── Batch Mode ────────────────────────────────────────────────────────────────
def batch_submit(payloads):
    """Submit multiple jobs and wait for all to finish. Returns results."""
    conn, _ = connect()
    job_ids = []
    for p in payloads:
        resp = submit_job(conn, p)
        if resp and resp.get("status") == "OK":
            job_ids.append(resp["job_id"])
            log.info(f"Submitted: {p} → {resp['job_id']}")
    results = {}
    for jid in job_ids:
        job = wait_for_job(conn, jid, timeout=120)
        results[jid] = job
    conn.close()
    return results


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "batch":
        # Usage: python client.py batch "COMPUTE:100" "SLEEP:2" "HASH:hello"
        payloads = sys.argv[2:]
        print(f"Batch submitting {len(payloads)} jobs...")
        results = batch_submit(payloads)
        for jid, job in results.items():
            if job:
                print(f"  {jid}: {job['status']} → {job['result']}")
            else:
                print(f"  {jid}: TIMEOUT")
    else:
        conn, _ = connect()
        try:
            interactive(conn)
        finally:
            conn.close()
