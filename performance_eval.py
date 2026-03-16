#!/usr/bin/env python3
"""
Performance Evaluation Script
Tests the job queue under increasing load and measures:
  - Throughput (jobs/second)
  - Average latency (submit → complete)
  - Queue depth over time
  - Scalability with multiple concurrent clients
"""

import threading
import time
import statistics
import sys
import json
import os

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from client import connect, submit_job, wait_for_job, get_metrics


def run_load_test(n_jobs: int, n_clients: int, job_type: str = "COMPUTE:500") -> dict:
    """
    Submit n_jobs spread across n_clients concurrently.
    Returns timing stats.
    """
    latencies = []
    errors     = []
    lock       = threading.Lock()
    barrier    = threading.Barrier(n_clients)

    jobs_per_client = max(1, n_jobs // n_clients)

    def client_worker(client_idx):
        try:
            conn, _ = connect()
            barrier.wait()  # all clients start together
            for _ in range(jobs_per_client):
                t0   = time.time()
                resp = submit_job(conn, job_type)
                if not resp or resp.get("status") != "OK":
                    with lock:
                        errors.append("submit failed")
                    continue
                job_id = resp["job_id"]
                job    = wait_for_job(conn, job_id, timeout=120)
                t1     = time.time()
                if job and job["status"] == "COMPLETED":
                    with lock:
                        latencies.append(t1 - t0)
                else:
                    with lock:
                        errors.append(f"job {job_id} did not complete")
            conn.close()
        except Exception as e:
            with lock:
                errors.append(str(e))

    threads = [threading.Thread(target=client_worker, args=(i,), daemon=True)
               for i in range(n_clients)]
    wall_start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=180)
    wall_end = time.time()

    total_time  = wall_end - wall_start
    total_done  = len(latencies)
    throughput  = total_done / total_time if total_time > 0 else 0

    return {
        "n_jobs":        n_jobs,
        "n_clients":     n_clients,
        "job_type":      job_type,
        "completed":     total_done,
        "errors":        len(errors),
        "total_time_s":  round(total_time, 3),
        "throughput_jps": round(throughput, 3),
        "avg_latency_s": round(statistics.mean(latencies), 4)    if latencies else None,
        "p50_latency_s": round(statistics.median(latencies), 4)  if latencies else None,
        "p95_latency_s": round(sorted(latencies)[int(len(latencies)*0.95)], 4)
                         if len(latencies) >= 20 else None,
        "min_latency_s": round(min(latencies), 4)                if latencies else None,
        "max_latency_s": round(max(latencies), 4)                if latencies else None,
    }


def print_table(results):
    header = (f"{'Clients':>8} {'Jobs':>6} {'Done':>6} {'Errors':>7} "
              f"{'Time(s)':>9} {'Throughput':>12} {'Avg Lat(s)':>12} {'P95 Lat(s)':>12}")
    print("\n" + "="*90)
    print("PERFORMANCE EVALUATION RESULTS")
    print("="*90)
    print(header)
    print("-"*90)
    for r in results:
        print(f"{r['n_clients']:>8} {r['n_jobs']:>6} {r['completed']:>6} "
              f"{r['errors']:>7} {r['total_time_s']:>9.3f} "
              f"{r['throughput_jps']:>12.3f} "
              f"{str(r['avg_latency_s']):>12} "
              f"{str(r['p95_latency_s']):>12}")
    print("="*90)


def main():
    print("Distributed Job Queue – Performance Evaluation")
    print("Make sure the server + at least 2 workers are running.\n")

    # Test matrix: (n_jobs, n_clients)
    scenarios = [
        (5,  1,  "FIBONACCI:30"),
        (10, 2,  "COMPUTE:1000"),
        (20, 4,  "COMPUTE:500"),
        (40, 4,  "HASH:perftest"),
    ]

    all_results = []
    for n_jobs, n_clients, jtype in scenarios:
        print(f"Running: {n_jobs} jobs, {n_clients} clients, type={jtype} ...", flush=True)
        result = run_load_test(n_jobs, n_clients, jtype)
        all_results.append(result)
        print(f"  → Done in {result['total_time_s']}s, "
              f"throughput={result['throughput_jps']} jobs/s, "
              f"avg_lat={result['avg_latency_s']}s")

    print_table(all_results)

    # Save JSON report
    os.makedirs("logs", exist_ok=True)
    report_path = "logs/perf_report.json"
    with open(report_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nDetailed report saved to {report_path}")

    # Fetch server-side metrics
    try:
        conn, _ = connect()
        from client import get_metrics
        resp = get_metrics(conn)
        conn.close()
        if resp and resp.get("status") == "OK":
            m = resp["metrics"]
            print("\nServer-side Metrics:")
            for k, v in m.items():
                print(f"  {k}: {v}")
    except Exception as e:
        print(f"Could not fetch server metrics: {e}")


if __name__ == "__main__":
    main()
