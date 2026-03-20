"""
Distributed Job Queue Server
- Accepts connections from clients (job submitters) and workers
- Maintains a thread-safe job queue
- Assigns jobs to workers exactly once
- Handles worker failures with re-queuing
- All communication over TCP + SSL/TLS
"""

import socket
import ssl
import threading
import json
import uuid
import time
import logging
import queue
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

HOST = "0.0.0.0"
PORT = 9000
CERTFILE = "server.crt"
KEYFILE = "server.key"

# Job states
class JobState(str, Enum):
    PENDING   = "PENDING"
    ASSIGNED  = "ASSIGNED"
    DONE      = "DONE"
    FAILED    = "FAILED"

# --- Shared State (protected by locks) ---
job_queue   = queue.Queue()          # pending job IDs
jobs        = {}                     # job_id -> job dict
jobs_lock   = threading.Lock()

workers     = {}                     # worker_id -> {conn, addr, current_job}
workers_lock = threading.Lock()

# Metrics
metrics = {
    "jobs_submitted": 0,
    "jobs_completed": 0,
    "jobs_failed": 0,
    "jobs_requeued": 0,
}
metrics_lock = threading.Lock()

WORKER_TIMEOUT = 30  # seconds before a worker is considered dead


def send_msg(conn, data: dict):
    """Send a JSON message prefixed with 4-byte length."""
    raw = json.dumps(data).encode()
    length = len(raw).to_bytes(4, "big")
    conn.sendall(length + raw)


def recv_msg(conn) -> dict:
    """Receive a length-prefixed JSON message."""
    raw_len = _recv_exact(conn, 4)
    if not raw_len:
        return None
    length = int.from_bytes(raw_len, "big")
    raw = _recv_exact(conn, length)
    if not raw:
        return None
    return json.loads(raw.decode())


def _recv_exact(conn, n: int) -> bytes:
    """Read exactly n bytes from socket."""
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# --- Client Handler (job submission) ---
def handle_client(conn, addr):
    logging.info(f"Client connected: {addr}")
    try:
        while True:
            msg = recv_msg(conn)
            if msg is None:
                break

            action = msg.get("action")

            if action == "submit":
                job_id = str(uuid.uuid4())
                job = {
                    "id": job_id,
                    "payload": msg.get("payload", ""),
                    "state": JobState.PENDING,
                    "submitted_at": time.time(),
                    "result": None,
                }
                with jobs_lock:
                    jobs[job_id] = job
                job_queue.put(job_id)
                with metrics_lock:
                    metrics["jobs_submitted"] += 1
                logging.info(f"Job submitted: {job_id}")
                send_msg(conn, {"status": "ok", "job_id": job_id})

            elif action == "status":
                job_id = msg.get("job_id")
                with jobs_lock:
                    job = jobs.get(job_id)
                if job:
                    send_msg(conn, {
                        "status": "ok",
                        "job_id": job_id,
                        "state": job["state"],
                        "result": job["result"],
                    })
                else:
                    send_msg(conn, {"status": "error", "message": "Job not found"})

            elif action == "metrics":
                with metrics_lock:
                    send_msg(conn, {"status": "ok", "metrics": dict(metrics)})

            else:
                send_msg(conn, {"status": "error", "message": "Unknown action"})

    except (ConnectionResetError, BrokenPipeError, ssl.SSLError) as e:
        logging.warning(f"Client {addr} disconnected: {e}")
    finally:
        conn.close()
        logging.info(f"Client disconnected: {addr}")


# --- Worker Handler ---
def handle_worker(conn, addr):
    worker_id = str(uuid.uuid4())
    logging.info(f"Worker connected: {addr} -> id={worker_id}")

    with workers_lock:
        workers[worker_id] = {"conn": conn, "addr": addr, "current_job": None}

    try:
        # Registration ack
        send_msg(conn, {"status": "ok", "worker_id": worker_id})

        while True:
            # Wait for a job
            try:
                job_id = job_queue.get(timeout=5)
            except queue.Empty:
                # Send heartbeat ping to keep connection alive
                try:
                    send_msg(conn, {"action": "ping"})
                    resp = recv_msg(conn)
                    if resp is None:
                        break
                except Exception:
                    break
                continue

            with jobs_lock:
                job = jobs.get(job_id)
                if job is None or job["state"] != JobState.PENDING:
                    continue
                job["state"] = JobState.ASSIGNED
                job["assigned_at"] = time.time()
                job["assigned_to"] = worker_id

            with workers_lock:
                if worker_id in workers:
                    workers[worker_id]["current_job"] = job_id

            logging.info(f"Assigning job {job_id} to worker {worker_id}")

            try:
                send_msg(conn, {"action": "execute", "job_id": job_id, "payload": job["payload"]})
                # Wait for result with timeout
                conn.settimeout(WORKER_TIMEOUT)
                result_msg = recv_msg(conn)
                conn.settimeout(None)

                if result_msg is None:
                    raise ConnectionError("Worker disconnected during job execution")

                if result_msg.get("action") == "result":
                    success = result_msg.get("success", False)
                    with jobs_lock:
                        jobs[job_id]["result"] = result_msg.get("result")
                        jobs[job_id]["state"] = JobState.DONE if success else JobState.FAILED
                        jobs[job_id]["completed_at"] = time.time()

                    with metrics_lock:
                        if success:
                            metrics["jobs_completed"] += 1
                        else:
                            metrics["jobs_failed"] += 1

                    logging.info(f"Job {job_id} {'completed' if success else 'failed'} by worker {worker_id}")

                with workers_lock:
                    if worker_id in workers:
                        workers[worker_id]["current_job"] = None

            except (ConnectionError, socket.timeout, ssl.SSLError) as e:
                logging.warning(f"Worker {worker_id} failed during job {job_id}: {e}")
                _requeue_job(job_id)
                break

    except (ConnectionResetError, BrokenPipeError, ssl.SSLError) as e:
        logging.warning(f"Worker {worker_id} disconnected: {e}")
    finally:
        # Re-queue any job the worker was holding
        with workers_lock:
            if worker_id in workers:
                held_job = workers[worker_id].get("current_job")
                if held_job:
                    _requeue_job(held_job)
                del workers[worker_id]
        conn.close()
        logging.info(f"Worker {worker_id} removed")


def _requeue_job(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
        if job and job["state"] == JobState.ASSIGNED:
            job["state"] = JobState.PENDING
            job["assigned_to"] = None
    job_queue.put(job_id)
    with metrics_lock:
        metrics["jobs_requeued"] += 1
    logging.info(f"Job {job_id} re-queued")


# --- Dispatcher: routes incoming connections to client or worker handler ---
def dispatch(conn, addr):
    try:
        msg = recv_msg(conn)
        if msg is None:
            conn.close()
            return
        role = msg.get("role")
        if role == "client":
            handle_client(conn, addr)
        elif role == "worker":
            handle_worker(conn, addr)
        else:
            send_msg(conn, {"status": "error", "message": "Unknown role"})
            conn.close()
    except Exception as e:
        logging.error(f"Dispatch error for {addr}: {e}")
        conn.close()


def main():
    # SSL context
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=CERTFILE, keyfile=KEYFILE)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2

    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    raw_sock.bind((HOST, PORT))
    raw_sock.listen(128)

    server_sock = ctx.wrap_socket(raw_sock, server_side=True)
    logging.info(f"Job Queue Server listening on {HOST}:{PORT} (TLS)")

    try:
        while True:
            conn, addr = server_sock.accept()
            t = threading.Thread(target=dispatch, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        logging.info("Server shutting down.")
    finally:
        server_sock.close()


if __name__ == "__main__":
    main()
