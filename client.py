"""
Distributed Job Queue – Client
================================
This script is used by users / applications to:
  1. Submit a new job to the queue
  2. Query the status of an existing job (by job_id)
  3. List all jobs in the system

All communication is over TLS-encrypted TCP sockets, directly with the server.
The client sends one request and reads one response (request-response pattern).
"""

import socket
import ssl
import json
import argparse
import sys

if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# ─────────────────────────── TLS Setup ───────────────────────────────────────

def create_client_tls_context() -> ssl.SSLContext:
    """
    Creates a TLS context for the client side of the connection.
    We load the server's self-signed certificate as the trusted CA so that
    the TLS handshake can verify we're talking to the real server.
    """
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls_context.load_verify_locations("certs/server.crt")
    tls_context.check_hostname = False
    tls_context.verify_mode    = ssl.CERT_REQUIRED
    return tls_context


def connect_to_server(server_host: str, server_port: int) -> ssl.SSLSocket | None:
    """
    Opens a TLS TCP connection to the job queue server.
    Returns the wrapped SSL socket, or None if the connection failed.
    """
    raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw_socket.settimeout(10)

    tls_context = create_client_tls_context()

    try:
        raw_socket.connect((server_host, server_port))
        secure_socket = tls_context.wrap_socket(raw_socket, server_hostname=server_host)
        return secure_socket

    except (ConnectionRefusedError, OSError) as connection_error:
        print(f"[ERROR] Cannot connect to server at {server_host}:{server_port}: {connection_error}")
        print("        Is the server running?  →  python server.py")
        raw_socket.close()
        return None

    except ssl.SSLError as tls_error:
        print(f"[ERROR] TLS handshake failed: {tls_error}")
        raw_socket.close()
        return None


# ─────────────────────────── Message I/O ─────────────────────────────────────

def send_message(secure_socket: ssl.SSLSocket, message: dict) -> bool:
    """Serialises and sends a JSON message over the TLS socket."""
    try:
        encoded = (json.dumps(message) + "\n").encode("utf-8")
        secure_socket.sendall(encoded)
        return True
    except (OSError, ssl.SSLError) as error:
        print(f"[ERROR] Failed to send message: {error}")
        return False


def receive_message(secure_socket: ssl.SSLSocket) -> dict | None:
    """Reads and parses a newline-terminated JSON message from the TLS socket."""
    raw_bytes = b""
    try:
        while b"\n" not in raw_bytes:
            chunk = secure_socket.recv(4096)
            if not chunk:
                return None
            raw_bytes += chunk
        return json.loads(raw_bytes.split(b"\n")[0].decode("utf-8"))
    except (json.JSONDecodeError, OSError, ssl.SSLError) as error:
        print(f"[ERROR] Failed to receive message: {error}")
        return None


# ─────────────────────────── Client Actions ──────────────────────────────────

def submit_job(server_host: str, server_port: int, job_description: str):
    """
    Submits a new job to the queue and prints the assigned job_id.
    The job_id can be used later to check the job's status.
    """
    secure_socket = connect_to_server(server_host, server_port)
    if secure_socket is None:
        return

    try:
        send_message(secure_socket, {
            "type":    "submit_job",
            "payload": job_description,
        })

        response = receive_message(secure_socket)

        if response is None:
            print("[ERROR] No response from server.")
            return

        if response.get("type") == "ack":
            job_id = response.get("job_id")
            print(f"\n✅ Job submitted successfully!")
            print(f"   Job ID : {job_id}")
            print(f"   Message: {response.get('message')}")
            print(f"\n   To check status, run:")
            print(f"   python client.py --action query --job-id {job_id}")
        elif response.get("type") == "error":
            print(f"[ERROR] Server rejected job: {response.get('message')}")

    finally:
        secure_socket.close()


def query_job_status(server_host: str, server_port: int, job_id: str):
    """
    Queries the current status and result of a job by its job_id.
    Status can be: pending, running, done, or failed.
    """
    secure_socket = connect_to_server(server_host, server_port)
    if secure_socket is None:
        return

    try:
        send_message(secure_socket, {
            "type":   "query_job",
            "job_id": job_id,
        })

        response = receive_message(secure_socket)

        if response is None:
            print("[ERROR] No response from server.")
            return

        if response.get("type") == "job_status":
            status = response.get("status", "unknown")
            result = response.get("result")

            # Show a coloured status indicator based on the current state.
            status_icon = {
                "pending": "⏳",
                "running": "🔄",
                "done":    "✅",
                "failed":  "❌",
            }.get(status, "❓")

            print(f"\n{status_icon} Job Status")
            print(f"   Job ID : {job_id}")
            print(f"   Status : {status.upper()}")

            if result:
                print(f"   Result : {result}")

        elif response.get("type") == "error":
            print(f"[ERROR] {response.get('message')}")

    finally:
        secure_socket.close()


def list_all_jobs(server_host: str, server_port: int):
    """
    Lists all jobs in the system (all statuses).
    Useful for monitoring the overall queue health.
    """
    secure_socket = connect_to_server(server_host, server_port)
    if secure_socket is None:
        return

    try:
        send_message(secure_socket, {"type": "list_jobs"})
        response = receive_message(secure_socket)

        if response is None:
            print("[ERROR] No response from server.")
            return

        if response.get("type") == "jobs_list":
            all_jobs = response.get("jobs", [])

            if not all_jobs:
                print("\n📭 No jobs in the system yet.")
                return

            print(f"\n📋 All Jobs ({len(all_jobs)} total)")
            print("-" * 72)
            print(f"{'STATUS':<10} {'JOB ID':<12} {'SUBMITTED':<22} {'PAYLOAD'}")
            print("-" * 72)

            for job in all_jobs:
                job_id_short  = job["job_id"][:8] + "..."
                submitted_at  = job.get("submitted_at", "")[:19]   # Trim microseconds
                payload_short = job.get("payload", "")[:35]

                status_icon = {
                    "pending": "⏳",
                    "running": "🔄",
                    "done":    "✅",
                    "failed":  "❌",
                }.get(job.get("status", ""), "❓")

                print(
                    f"{status_icon} {job.get('status',''):<8} "
                    f"{job_id_short:<12} "
                    f"{submitted_at:<22} "
                    f"{payload_short}"
                )

            print("-" * 72)

        elif response.get("type") == "error":
            print(f"[ERROR] {response.get('message')}")

    finally:
        secure_socket.close()


# ─────────────────────────── Entry Point ──────────────────────────────────────

if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(
        description="Distributed Job Queue – Client",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Examples:
  Submit a job:
    python client.py --action submit --payload "process report for Q3"

  Check job status:
    python client.py --action query --job-id <uuid>

  List all jobs:
    python client.py --action list
        """
    )

    argument_parser.add_argument(
        "--action",
        required=True,
        choices=["submit", "query", "list"],
        help="Action to perform: submit / query / list"
    )
    argument_parser.add_argument(
        "--payload",
        default="",
        help="Job description / command (required for --action submit)"
    )
    argument_parser.add_argument(
        "--job-id",
        default="",
        help="Job UUID (required for --action query)"
    )
    argument_parser.add_argument(
        "--host", default="localhost",
        help="Server hostname or IP (default: localhost)"
    )
    argument_parser.add_argument(
        "--port", type=int, default=9000,
        help="Server port (default: 9000)"
    )

    args = argument_parser.parse_args()

    if args.action == "submit":
        if not args.payload:
            print("[ERROR] --payload is required when using --action submit")
            sys.exit(1)
        submit_job(args.host, args.port, args.payload)

    elif args.action == "query":
        if not args.job_id:
            print("[ERROR] --job-id is required when using --action query")
            sys.exit(1)
        query_job_status(args.host, args.port, args.job_id)

    elif args.action == "list":
        list_all_jobs(args.host, args.port)
