"""
Performance Evaluation Script
- Spawns multiple concurrent clients
- Submits jobs and measures throughput, latency, and response time
- Prints a summary report
"""

import socket
import ssl
import json
import time
import threading
import argparse
import statistics
import logging

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [PERF] %(message)s")

CERTFILE = "server.crt"


def send_msg(conn, data):
    raw = json.dumps(data).encode()
    conn.sendall(len(raw).to_bytes(4, "big") + raw)


def recv_msg(conn):
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


def connect(host, port):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(CERTFILE)
    ctx.check_hostname = False
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = ctx.wrap_socket(raw, server_hostname=host)
    conn.connect((host, port))
    send_msg(conn, {"role": "client"})
    return conn


def client_task(host, port, num_jobs, results, idx):
    """Single client: submit jobs and measure end-to-end latency."""
    latencies = []
    errors = 0
    try:
        conn = connect(host, port)
        for _ in range(num_jobs):
            t0 = time.time()
            send_msg(conn, {"action": "submit", "payload": "echo perf_test"})
            resp = recv_msg(conn)
            if not resp or resp.get("status") != "ok":
                errors += 1
                continue
            job_id = resp["job_id"]

            # Poll until done
            deadline = time.time() + 60
            while time.time() < deadline:
                send_msg(conn, {"action": "status", "job_id": job_id})
                sr = recv_msg(conn)
                if sr and sr.get("state") in ("DONE", "FAILED"):
                    break
                time.sleep(0.2)

            latency = time.time() - t0
            latencies.append(latency)

        conn.close()
    except Exception as e:
        logging.error(f"Client {idx} error: {e}")
        errors += 1

    results[idx] = {"latencies": latencies, "errors": errors}


def fetch_metrics(host, port):
    try:
        conn = connect(host, port)
        send_msg(conn, {"action": "metrics"})
        resp = recv_msg(conn)
        conn.close()
        return resp.get("metrics", {}) if resp else {}
    except Exception:
        return {}


def main():
    parser = argparse.ArgumentParser(description="Performance Test for Job Queue")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--clients", type=int, default=5, help="Concurrent clients")
    parser.add_argument("--jobs", type=int, default=4, help="Jobs per client")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  Performance Test: {args.clients} clients x {args.jobs} jobs each")
    print(f"{'='*55}")

    results = [None] * args.clients
    threads = []
    start_time = time.time()

    for i in range(args.clients):
        t = threading.Thread(
            target=client_task,
            args=(args.host, args.port, args.jobs, results, i)
        )
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_elapsed = time.time() - start_time

    # Aggregate
    all_latencies = []
    total_errors = 0
    for r in results:
        if r:
            all_latencies.extend(r["latencies"])
            total_errors += r["errors"]

    total_jobs = len(all_latencies)
    throughput = total_jobs / total_elapsed if total_elapsed > 0 else 0

    print(f"\nResults:")
    print(f"  Total jobs completed : {total_jobs}")
    print(f"  Total errors         : {total_errors}")
    print(f"  Total elapsed time   : {total_elapsed:.2f}s")
    print(f"  Throughput           : {throughput:.2f} jobs/sec")

    if all_latencies:
        print(f"  Avg latency          : {statistics.mean(all_latencies):.3f}s")
        print(f"  Min latency          : {min(all_latencies):.3f}s")
        print(f"  Max latency          : {max(all_latencies):.3f}s")
        print(f"  Median latency       : {statistics.median(all_latencies):.3f}s")
        if len(all_latencies) > 1:
            print(f"  Stdev latency        : {statistics.stdev(all_latencies):.3f}s")

    # Server-side metrics
    m = fetch_metrics(args.host, args.port)
    if m:
        print(f"\nServer Metrics:")
        for k, v in m.items():
            print(f"  {k:<25}: {v}")

    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
