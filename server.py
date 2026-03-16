#!/usr/bin/env python3
"""
Distributed Job Queue Server
- TCP + SSL/TLS secured
- Concurrent clients via threading
- Centralized job queue with synchronization
- Worker failure detection and job re-queuing
- Performance metrics logging
"""

import socket
import ssl
import threading
import json
import time
import uuid
import queue
import logging
import signal
import sys
import os
from enum import Enum
from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, List

# ─── Logging Setup ─────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/server.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("server")

# ─── Constants ─────────────────────────────────────────────────────────────────
HOST = "0.0.0.0"
PORT = 9000
CERTFILE = "certs/server.crt"
KEYFILE  = "certs/server.key"
HEARTBEAT_INTERVAL = 5    # seconds
WORKER_TIMEOUT     = 15   # seconds before worker is considered dead


class JobStatus(str, Enum):
    PENDING    = "PENDING"
    ASSIGNED   = "ASSIGNED"
    COMPLETED  = "COMPLETED"
    FAILED     = "FAILED"


@dataclass
class Job:
    job_id:     str
    payload:    str
    status:     JobStatus = JobStatus.PENDING
    worker_id:  Optional[str] = None
    submitted_at: float = field(default_factory=time.time)
    started_at:   Optional[float] = None
    completed_at: Optional[float] = None
    result:       Optional[str] = None

    def to_dict(self):
        d = asdict(self)
        d["status"] = self.status.value
        return d


# ─── Shared State ──────────────────────────────────────────────────────────────
job_queue:   queue.Queue = queue.Queue()          # pending jobs
jobs_db:     Dict[str, Job] = {}                  # all jobs ever seen
workers:     Dict[str, dict] = {}                 # worker_id → {conn, last_seen, job_id}
clients:     Dict[str, dict] = {}                 # client_id → {conn}

lock = threading.Lock()

# ─── Performance Metrics ───────────────────────────────────────────────────────
metrics = {
    "jobs_submitted": 0,
    "jobs_completed": 0,
    "jobs_failed":    0,
    "jobs_requeued":  0,
    "total_latency":  0.0,   # sum of (completed_at - submitted_at)
}
metrics_lock = threading.Lock()


def send_msg(conn: ssl.SSLSocket, msg: dict) -> bool:
    """Frame and send a JSON message (4-byte length prefix)."""
    try:
        data = json.dumps(msg).encode()
        length = len(data).to_bytes(4, "big")
        conn.sendall(length + data)
        return True
    except Exception as e:
        log.warning(f"send_msg failed: {e}")
        return False


def recv_msg(conn: ssl.SSLSocket) -> Optional[dict]:
    """Read a framed JSON message."""
    try:
        raw_len = _recv_exact(conn, 4)
        if not raw_len:
            return None
        length = int.from_bytes(raw_len, "big")
        data = _recv_exact(conn, length)
        if not data:
            return None
        return json.loads(data.decode())
    except Exception as e:
        log.warning(f"recv_msg failed: {e}")
        return None


def _recv_exact(conn, n: int) -> Optional[bytes]:
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ─── Worker Watchdog ───────────────────────────────────────────────────────────
def watchdog():
    """Periodically check for dead workers and re-queue their jobs."""
    log.info("Watchdog started.")
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        now = time.time()
        dead = []
        with lock:
            for wid, info in list(workers.items()):
                if now - info["last_seen"] > WORKER_TIMEOUT:
                    dead.append(wid)

        for wid in dead:
            with lock:
                info = workers.pop(wid, None)
            if not info:
                continue
            jid = info.get("job_id")
            log.warning(f"Worker {wid} timed out. Re-queuing job {jid}.")
            if jid:
                with lock:
                    job = jobs_db.get(jid)
                    if job and job.status == JobStatus.ASSIGNED:
                        job.status  = JobStatus.PENDING
                        job.worker_id = None
                        job.started_at = None
                        job_queue.put(job)
                with metrics_lock:
                    metrics["jobs_requeued"] += 1


# ─── Client Handler ────────────────────────────────────────────────────────────
def handle_client(conn: ssl.SSLSocket, addr, client_id: str):
    """Handle a client (job submitter) connection."""
    log.info(f"Client {client_id} connected from {addr}")
    with lock:
        clients[client_id] = {"conn": conn, "addr": addr}
    try:
        while True:
            msg = recv_msg(conn)
            if not msg:
                break
            action = msg.get("action")

            if action == "SUBMIT":
                payload = msg.get("payload", "")
                job = Job(job_id=str(uuid.uuid4()), payload=payload)
                with lock:
                    jobs_db[job.job_id] = job
                job_queue.put(job)
                with metrics_lock:
                    metrics["jobs_submitted"] += 1
                log.info(f"Job {job.job_id} submitted by {client_id}.")
                send_msg(conn, {"status": "OK", "job_id": job.job_id})

            elif action == "STATUS":
                job_id = msg.get("job_id")
                with lock:
                    job = jobs_db.get(job_id)
                if job:
                    send_msg(conn, {"status": "OK", "job": job.to_dict()})
                else:
                    send_msg(conn, {"status": "ERROR", "message": "Job not found"})

            elif action == "LIST_JOBS":
                with lock:
                    all_jobs = [j.to_dict() for j in jobs_db.values()]
                send_msg(conn, {"status": "OK", "jobs": all_jobs})

            elif action == "METRICS":
                with metrics_lock:
                    snap = dict(metrics)
                with lock:
                    snap["queue_depth"]    = job_queue.qsize()
                    snap["active_workers"] = len(workers)
                avg = (snap["total_latency"] / snap["jobs_completed"]
                       if snap["jobs_completed"] else 0)
                snap["avg_latency_sec"] = round(avg, 4)
                send_msg(conn, {"status": "OK", "metrics": snap})

            else:
                send_msg(conn, {"status": "ERROR", "message": f"Unknown action: {action}"})

    except Exception as e:
        log.error(f"Client {client_id} error: {e}")
    finally:
        with lock:
            clients.pop(client_id, None)
        conn.close()
        log.info(f"Client {client_id} disconnected.")


# ─── Worker Handler ────────────────────────────────────────────────────────────
def handle_worker(conn: ssl.SSLSocket, addr, worker_id: str):
    """Handle a worker node connection."""
    log.info(f"Worker {worker_id} connected from {addr}")
    with lock:
        workers[worker_id] = {"conn": conn, "addr": addr,
                               "last_seen": time.time(), "job_id": None}
    try:
        while True:
            msg = recv_msg(conn)
            if not msg:
                break
            action = msg.get("action")

            if action == "HEARTBEAT":
                with lock:
                    if worker_id in workers:
                        workers[worker_id]["last_seen"] = time.time()
                send_msg(conn, {"status": "OK", "action": "HEARTBEAT_ACK"})

            elif action == "FETCH":
                # Block until a job is available (timeout so heartbeats keep working)
                try:
                    job = job_queue.get(timeout=2)
                except queue.Empty:
                    send_msg(conn, {"status": "NO_JOB"})
                    continue

                with lock:
                    job.status     = JobStatus.ASSIGNED
                    job.worker_id  = worker_id
                    job.started_at = time.time()
                    if worker_id in workers:
                        workers[worker_id]["job_id"] = job.job_id
                log.info(f"Job {job.job_id} assigned to Worker {worker_id}.")
                send_msg(conn, {"status": "OK", "action": "JOB_ASSIGNED",
                                "job": job.to_dict()})

            elif action == "COMPLETE":
                job_id = msg.get("job_id")
                result = msg.get("result", "")
                with lock:
                    job = jobs_db.get(job_id)
                    if job and job.worker_id == worker_id:
                        job.status       = JobStatus.COMPLETED
                        job.completed_at = time.time()
                        job.result       = result
                        if worker_id in workers:
                            workers[worker_id]["job_id"] = None
                        latency = job.completed_at - job.submitted_at
                        with metrics_lock:
                            metrics["jobs_completed"] += 1
                            metrics["total_latency"]  += latency
                        log.info(f"Job {job_id} completed by {worker_id} "
                                 f"in {latency:.3f}s.")
                        send_msg(conn, {"status": "OK"})
                    else:
                        send_msg(conn, {"status": "ERROR",
                                        "message": "Job not found or not yours"})

            elif action == "FAIL":
                job_id = msg.get("job_id")
                reason = msg.get("reason", "unknown")
                with lock:
                    job = jobs_db.get(job_id)
                    if job and job.worker_id == worker_id:
                        job.status    = JobStatus.FAILED
                        job.result    = f"FAILED: {reason}"
                        if worker_id in workers:
                            workers[worker_id]["job_id"] = None
                        with metrics_lock:
                            metrics["jobs_failed"] += 1
                        log.warning(f"Job {job_id} failed: {reason}")
                        send_msg(conn, {"status": "OK"})
                    else:
                        send_msg(conn, {"status": "ERROR",
                                        "message": "Job not found or not yours"})

            else:
                send_msg(conn, {"status": "ERROR",
                                "message": f"Unknown action: {action}"})

    except Exception as e:
        log.error(f"Worker {worker_id} error: {e}")
    finally:
        with lock:
            info = workers.pop(worker_id, None)
        # Re-queue any in-progress job
        if info and info.get("job_id"):
            jid = info["job_id"]
            with lock:
                job = jobs_db.get(jid)
                if job and job.status == JobStatus.ASSIGNED:
                    job.status    = JobStatus.PENDING
                    job.worker_id = None
                    job_queue.put(job)
            with metrics_lock:
                metrics["jobs_requeued"] += 1
            log.warning(f"Re-queued job {jid} after Worker {worker_id} disconnected.")
        conn.close()
        log.info(f"Worker {worker_id} disconnected.")


# ─── Connection Dispatcher ─────────────────────────────────────────────────────
def dispatch(conn: ssl.SSLSocket, addr):
    """First message determines if it's a client or worker."""
    try:
        msg = recv_msg(conn)
        if not msg:
            conn.close()
            return
        role = msg.get("role")
        node_id = msg.get("node_id", str(uuid.uuid4()))
        if role == "CLIENT":
            send_msg(conn, {"status": "OK", "message": "Welcome, Client",
                            "node_id": node_id})
            t = threading.Thread(target=handle_client,
                                 args=(conn, addr, node_id), daemon=True)
        elif role == "WORKER":
            send_msg(conn, {"status": "OK", "message": "Welcome, Worker",
                            "node_id": node_id})
            t = threading.Thread(target=handle_worker,
                                 args=(conn, addr, node_id), daemon=True)
        else:
            send_msg(conn, {"status": "ERROR", "message": "Unknown role"})
            conn.close()
            return
        t.start()
    except Exception as e:
        log.error(f"Dispatch error from {addr}: {e}")
        conn.close()


# ─── SSL Context ───────────────────────────────────────────────────────────────
def create_ssl_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=CERTFILE, keyfile=KEYFILE)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    ssl_ctx = create_ssl_context()
    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    raw_sock.bind((HOST, PORT))
    raw_sock.listen(50)
    log.info(f"Server listening on {HOST}:{PORT} (TLS enabled)")

    # Start watchdog
    threading.Thread(target=watchdog, daemon=True).start()

    def shutdown(sig, frame):
        log.info("Shutting down server.")
        raw_sock.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        try:
            conn, addr = raw_sock.accept()
            ssl_conn = ssl_ctx.wrap_socket(conn, server_side=True)
            threading.Thread(target=dispatch, args=(ssl_conn, addr),
                             daemon=True).start()
        except ssl.SSLError as e:
            log.warning(f"SSL handshake failed from {addr}: {e}")
        except Exception as e:
            log.error(f"Accept error: {e}")
            break


if __name__ == "__main__":
    main()
