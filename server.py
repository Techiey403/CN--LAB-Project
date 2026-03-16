import socket, ssl, threading, json, uuid, queue, signal, sys, logging
from datetime import datetime

# Fix Windows UTF-8 output
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SERVER] %(message)s")
log = logging.getLogger()

HOST     = "0.0.0.0"
PORT     = 9000
CERT     = "certs/server.crt"
KEY      = "certs/server.key"
MAX_RETRY = 3

# ── Shared state ──────────────────────────────────────────────────────────────
pending_jobs  = queue.Queue()        # Jobs waiting for a worker
all_jobs      = {}                   # job_id -> job record
active_workers = {}                  # worker_id -> {socket, current_job}
running_jobs  = {}                   # job_id -> worker_id
jobs_lock     = threading.Lock()
workers_lock  = threading.Lock()
shutdown_evt  = threading.Event()

# ── TLS setup ─────────────────────────────────────────────────────────────────
def make_tls_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=CERT, keyfile=KEY)
    return ctx

# ── Message helpers ───────────────────────────────────────────────────────────
def send_msg(sock, data):
    try:
        sock.sendall((json.dumps(data) + "\n").encode("utf-8"))
        return True
    except Exception:
        return False

def recv_msg(sock):
    buf = b""
    try:
        while b"\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            buf += chunk
        return json.loads(buf.split(b"\n")[0])
    except Exception:
        return None

# ── Client handler ────────────────────────────────────────────────────────────
def handle_client(sock, addr, msg):
    t = msg.get("type")

    if t == "submit_job":
        payload = msg.get("payload", "").strip()
        if not payload:
            send_msg(sock, {"type": "error", "message": "Payload cannot be empty."})
            return
        job_id = str(uuid.uuid4())
        record = {"job_id": job_id, "payload": payload, "status": "pending",
                  "result": None, "submitted_at": datetime.utcnow().isoformat(), "retries": 0}
        with jobs_lock:
            all_jobs[job_id] = record
        pending_jobs.put(record)
        log.info("Job queued [%s]: %s", job_id[:8], payload[:50])
        send_msg(sock, {"type": "ack", "job_id": job_id, "message": "Job accepted."})

    elif t == "query_job":
        job_id = msg.get("job_id", "")
        with jobs_lock:
            record = all_jobs.get(job_id)
        if record is None:
            send_msg(sock, {"type": "error", "message": f"Job '{job_id}' not found."})
        else:
            send_msg(sock, {"type": "job_status", "job_id": job_id,
                            "status": record["status"], "result": record["result"]})

    elif t == "list_jobs":
        with jobs_lock:
            jobs = [{"job_id": j["job_id"], "payload": j["payload"][:50],
                     "status": j["status"], "submitted_at": j["submitted_at"]}
                    for j in all_jobs.values()]
        send_msg(sock, {"type": "jobs_list", "jobs": jobs})

# ── Worker: push jobs ─────────────────────────────────────────────────────────
def push_jobs(worker_id):
    while not shutdown_evt.is_set():
        try:
            job = pending_jobs.get(timeout=1.0)
        except queue.Empty:
            continue

        with workers_lock:
            info = active_workers.get(worker_id)
        if info is None:
            pending_jobs.put(job)
            return

        job_id = job["job_id"]
        with jobs_lock:
            if job_id in all_jobs:
                all_jobs[job_id]["status"] = "running"
        with workers_lock:
            if worker_id in active_workers:
                active_workers[worker_id]["current_job"] = job_id
        running_jobs[job_id] = worker_id

        log.info("Assigning job [%s] to worker [%s]", job_id[:8], worker_id[:8])
        ok = send_msg(info["socket"], {"type": "assign_job", "job_id": job_id, "payload": job["payload"]})
        if not ok:
            requeue(job_id)
            return

# ── Worker: requeue on failure ────────────────────────────────────────────────
def requeue(job_id, reason="Worker disconnected"):
    with jobs_lock:
        rec = all_jobs.get(job_id)
        if rec is None:
            return
        if rec["retries"] < MAX_RETRY:
            rec["retries"] += 1
            rec["status"] = "pending"
            pending_jobs.put(rec)
            log.info("Job [%s] re-queued (%d/%d): %s", job_id[:8], rec["retries"], MAX_RETRY, reason)
        else:
            rec["status"] = "failed"
            rec["result"] = f"Failed after {MAX_RETRY} retries: {reason}"
            log.error("Job [%s] permanently FAILED", job_id[:8])
    running_jobs.pop(job_id, None)

# ── Worker handler ────────────────────────────────────────────────────────────
def handle_worker(sock, addr, first_msg):
    worker_id = str(uuid.uuid4())
    with workers_lock:
        active_workers[worker_id] = {"socket": sock, "current_job": None}

    log.info("Worker registered [%s] from %s", worker_id[:8], addr)
    send_msg(sock, {"type": "ack", "worker_id": worker_id, "message": "Registered."})

    threading.Thread(target=push_jobs, args=(worker_id,), daemon=True).start()

    try:
        while not shutdown_evt.is_set():
            msg = recv_msg(sock)
            if msg is None:
                break
            t      = msg.get("type")
            job_id = msg.get("job_id", "")

            if t == "job_done":
                with jobs_lock:
                    if job_id in all_jobs:
                        all_jobs[job_id]["status"] = "done"
                        all_jobs[job_id]["result"] = msg.get("result", "")
                running_jobs.pop(job_id, None)
                with workers_lock:
                    if worker_id in active_workers:
                        active_workers[worker_id]["current_job"] = None
                log.info("Job [%s] DONE by worker [%s]", job_id[:8], worker_id[:8])

            elif t == "job_failed":
                log.warning("Job [%s] FAILED on worker [%s]: %s",
                            job_id[:8], worker_id[:8], msg.get("reason", ""))
                requeue(job_id, reason=msg.get("reason", "Unknown"))

            # ping -> update last_seen (no watchdog needed, connection drop handles it)
    except Exception as e:
        log.warning("Worker [%s] error: %s", worker_id[:8], e)
    finally:
        with workers_lock:
            info = active_workers.pop(worker_id, None)
        if info and info["current_job"]:
            requeue(info["current_job"], reason="Worker disconnected")
        log.info("Worker [%s] disconnected", worker_id[:8])

# ── Connection dispatcher ─────────────────────────────────────────────────────
def handle_connection(sock, addr):
    try:
        msg = recv_msg(sock)
        if msg is None:
            return
        t = msg.get("type", "")
        if t == "register_worker":
            handle_worker(sock, addr, msg)
        elif t in ("submit_job", "query_job", "list_jobs"):
            handle_client(sock, addr, msg)
        else:
            send_msg(sock, {"type": "error", "message": f"Unknown type: {t}"})
    except Exception as e:
        log.error("Connection error from %s: %s", addr, e)
    finally:
        sock.close()

# ── Main ──────────────────────────────────────────────────────────────────────
def shutdown(*_):
    log.info("Shutting down...")
    shutdown_evt.set()

def main_loop():
    """Start the server accept loop. Can be called directly or from a thread."""
    tls = make_tls_context()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT))
    server_sock.listen(50)
    log.info("Server listening on port %d (TLS)", PORT)

    while not shutdown_evt.is_set():
        try:
            server_sock.settimeout(1.0)
            raw, addr = server_sock.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        try:
            secure = tls.wrap_socket(raw, server_side=True)
        except ssl.SSLError as e:
            log.warning("TLS handshake failed from %s: %s", addr, e)
            raw.close()
            continue
        log.info("Connection from %s", addr)
        threading.Thread(target=handle_connection, args=(secure, addr), daemon=True).start()

    server_sock.close()
    log.info("Server stopped.")

if __name__ == "__main__":
    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    main_loop()
