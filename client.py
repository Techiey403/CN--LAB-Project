import socket, ssl, json, argparse, sys

# Fix Windows UTF-8 output
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

HOST = "localhost"
PORT = 9000
CERT = "certs/server.crt"

# ── TLS connection ────────────────────────────────────────────────────────────
def connect():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(CERT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.settimeout(10)
    try:
        raw.connect((HOST, PORT))
        return ctx.wrap_socket(raw, server_hostname=HOST)
    except Exception as e:
        print(f"[ERROR] Cannot connect to server: {e}")
        print("        Make sure server.py is running first.")
        raw.close()
        return None

def send(sock, data):
    sock.sendall((json.dumps(data) + "\n").encode("utf-8"))

def recv(sock):
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            return None
        buf += chunk
    return json.loads(buf.split(b"\n")[0])

# ── Actions ───────────────────────────────────────────────────────────────────
def submit(payload):
    sock = connect()
    if not sock: return
    send(sock, {"type": "submit_job", "payload": payload})
    res = recv(sock)
    sock.close()
    if res and res.get("type") == "ack":
        print(f"\nJob submitted!")
        print(f"  Job ID : {res['job_id']}")
        print(f"\n  Check status with:")
        print(f"  python client.py --action query --job-id {res['job_id']}")
    else:
        print(f"[ERROR] {res}")

def query(job_id):
    sock = connect()
    if not sock: return
    send(sock, {"type": "query_job", "job_id": job_id})
    res = recv(sock)
    sock.close()
    if res and res.get("type") == "job_status":
        icons = {"pending": "[PENDING]", "running": "[RUNNING]", "done": "[DONE]", "failed": "[FAILED]"}
        status = res.get("status", "unknown")
        print(f"\n{icons.get(status, '[?]')} Job: {job_id[:8]}...")
        print(f"  Status : {status.upper()}")
        if res.get("result"):
            print(f"  Result : {res['result']}")
    else:
        print(f"[ERROR] {res}")

def list_jobs():
    sock = connect()
    if not sock: return
    send(sock, {"type": "list_jobs"})
    res = recv(sock)
    sock.close()
    if res and res.get("type") == "jobs_list":
        jobs = res.get("jobs", [])
        if not jobs:
            print("\nNo jobs in the queue yet.")
            return
        print(f"\n{'STATUS':<10} {'JOB ID':<12} {'PAYLOAD'}")
        print("-" * 60)
        for j in jobs:
            print(f"{j['status']:<10} {j['job_id'][:8]+'...':<12} {j['payload'][:35]}")
    else:
        print(f"[ERROR] {res}")

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Job Queue Client")
    parser.add_argument("--action", required=True, choices=["submit", "query", "list"])
    parser.add_argument("--payload", default="", help="Job description (for submit)")
    parser.add_argument("--job-id", default="", help="Job ID (for query)")
    args = parser.parse_args()

    if args.action == "submit":
        if not args.payload:
            print("[ERROR] --payload is required for submit"); sys.exit(1)
        submit(args.payload)
    elif args.action == "query":
        if not args.job_id:
            print("[ERROR] --job-id is required for query"); sys.exit(1)
        query(args.job_id)
    elif args.action == "list":
        list_jobs()
