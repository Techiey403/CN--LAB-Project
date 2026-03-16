import socket, ssl, json, time, random, argparse, sys, logging

# Fix Windows UTF-8 output
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [WORKER] %(message)s")
log = logging.getLogger()

HOST = "localhost"
PORT = 9000
CERT = "certs/server.crt"

# ── TLS ───────────────────────────────────────────────────────────────────────
def connect(name):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(CERT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED

    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.settimeout(10)
    try:
        raw.connect((HOST, PORT))
        sock = ctx.wrap_socket(raw, server_hostname=HOST)
        log.info("Connected to server")
        return sock
    except Exception as e:
        log.error("Cannot connect: %s", e)
        raw.close()
        return None

# ── Message helpers ───────────────────────────────────────────────────────────
def send(sock, data):
    try:
        sock.sendall((json.dumps(data) + "\n").encode("utf-8"))
        return True
    except Exception:
        return False

def recv(sock):
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

# ── Job execution (simulated) ─────────────────────────────────────────────────
def execute(job_id, payload):
    # 10% random failure to demo retry logic
    if random.random() < 0.10:
        return None, "Simulated failure (10% chance)"
    duration = random.uniform(1.0, 5.0)
    log.info("Executing job [%s] (~%.1fs)...", job_id[:8], duration)
    time.sleep(duration)
    return f"Done: '{payload[:40]}' in {duration:.1f}s", None

# ── Main ──────────────────────────────────────────────────────────────────────
def run(name):
    sock = connect(name)
    if sock is None:
        return

    # Register as a worker
    send(sock, {"type": "register_worker", "name": name})
    ack = recv(sock)
    if not ack or ack.get("type") != "ack":
        log.error("Registration failed")
        sock.close()
        return

    worker_id = ack.get("worker_id")
    log.info("Registered as worker [%s]. Waiting for jobs...", worker_id[:8])

    # Main loop — wait for job assignments
    while True:
        msg = recv(sock)
        if msg is None:
            log.warning("Server connection lost.")
            break

        if msg.get("type") == "assign_job":
            job_id  = msg["job_id"]
            payload = msg["payload"]
            log.info("Received job [%s]: %s", job_id[:8], payload[:50])

            result, error = execute(job_id, payload)

            if error:
                send(sock, {"type": "job_failed", "job_id": job_id, "reason": error})
                log.warning("Job [%s] FAILED: %s", job_id[:8], error)
            else:
                send(sock, {"type": "job_done", "job_id": job_id, "result": result})
                log.info("Job [%s] DONE", job_id[:8])

    sock.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Job Queue Worker")
    parser.add_argument("--name", default="Worker-1", help="Worker name")
    args = parser.parse_args()
    try:
        run(args.name)
    except KeyboardInterrupt:
        print(f"\n[{args.name}] Stopped.")
