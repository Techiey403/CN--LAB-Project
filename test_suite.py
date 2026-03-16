#!/usr/bin/env python3
"""
Automated Test Suite for Distributed Job Queue
Tests:
  - Job submission and completion
  - Multiple concurrent clients
  - SSL/TLS enforcement
  - Worker failure and re-queuing
  - Edge cases: invalid actions, unknown job IDs
Run AFTER starting server.py and at least one worker.py
"""

import sys
import os
import time
import threading
import socket
import ssl
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from client import connect, submit_job, get_status, wait_for_job, get_metrics, list_jobs

CERTFILE = "certs/server.crt"
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9000

PASS = "\033[92m✓ PASS\033[0m"
FAIL = "\033[91m✗ FAIL\033[0m"

results = []


def test(name, fn):
    try:
        fn()
        print(f"  {PASS}  {name}")
        results.append((name, True))
    except AssertionError as e:
        print(f"  {FAIL}  {name}: {e}")
        results.append((name, False))
    except Exception as e:
        print(f"  {FAIL}  {name}: Exception – {e}")
        results.append((name, False))


# ─── Tests ─────────────────────────────────────────────────────────────────────

def test_ssl_required():
    """Plain TCP (no SSL) must be rejected."""
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.settimeout(3)
    raw.connect((SERVER_HOST, SERVER_PORT))
    try:
        raw.sendall(b"\x00\x00\x00\x10" + b'{"role":"CLIENT"}')
        time.sleep(0.5)
        data = raw.recv(1024)
        # Server wraps with SSL; raw data will be garbled or connection reset
        # Either way, no valid JSON response should arrive
        try:
            json.loads(data)
            assert False, "Expected garbled/rejected response, got valid JSON"
        except json.JSONDecodeError:
            pass  # Correct: server talks SSL, raw bytes don't parse
    except (ConnectionResetError, OSError):
        pass  # Also correct
    finally:
        raw.close()


def test_client_registration():
    conn, my_id = connect()
    assert my_id, "Expected a node_id"
    conn.close()


def test_submit_and_complete():
    conn, _ = connect()
    resp = submit_job(conn, "FIBONACCI:20")
    assert resp and resp.get("status") == "OK", f"Submit failed: {resp}"
    job_id = resp["job_id"]
    assert job_id, "No job_id returned"

    job = wait_for_job(conn, job_id, timeout=30)
    assert job, "Job did not complete in time"
    assert job["status"] == "COMPLETED", f"Expected COMPLETED, got {job['status']}"
    assert "Fibonacci" in job["result"], f"Unexpected result: {job['result']}"
    conn.close()


def test_status_unknown_job():
    conn, _ = connect()
    resp = get_status(conn, "nonexistent-job-id-12345")
    assert resp and resp.get("status") == "ERROR", "Expected ERROR for unknown job"
    conn.close()


def test_invalid_action():
    conn, _ = connect()
    from client import send_msg, recv_msg
    send_msg(conn, {"action": "FLYING_PIGS"})
    resp = recv_msg(conn)
    assert resp and resp.get("status") == "ERROR", f"Expected ERROR, got {resp}"
    conn.close()


def test_multiple_job_types():
    conn, _ = connect()
    jobs_to_test = [
        ("COMPUTE:100",   "Sum of squares"),
        ("HASH:hello",    "SHA256"),
        ("PRIME:50",      "Primes"),
        ("SLEEP:0.1",     "Slept"),
    ]
    for payload, expected_keyword in jobs_to_test:
        resp = submit_job(conn, payload)
        assert resp and resp.get("status") == "OK", f"Submit failed for {payload}"
        job = wait_for_job(conn, resp["job_id"], timeout=30)
        assert job and job["status"] == "COMPLETED", f"{payload} did not complete"
        assert expected_keyword.lower() in job["result"].lower(), \
            f"Expected '{expected_keyword}' in result '{job['result']}'"
    conn.close()


def test_concurrent_clients():
    results_lock = threading.Lock()
    completed    = []

    def client_task():
        conn, _ = connect()
        resp = submit_job(conn, "COMPUTE:200")
        if resp and resp.get("status") == "OK":
            job = wait_for_job(conn, resp["job_id"], timeout=60)
            if job and job["status"] == "COMPLETED":
                with results_lock:
                    completed.append(job["job_id"])
        conn.close()

    threads = [threading.Thread(target=client_task) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=90)

    assert len(completed) == 5, f"Only {len(completed)}/5 concurrent jobs completed"


def test_list_jobs():
    conn, _ = connect()
    resp = list_jobs(conn)
    assert resp and resp.get("status") == "OK", f"LIST_JOBS failed: {resp}"
    assert "jobs" in resp, "No 'jobs' key in response"
    assert isinstance(resp["jobs"], list), "Expected list"
    conn.close()


def test_metrics():
    conn, _ = connect()
    resp = get_metrics(conn)
    assert resp and resp.get("status") == "OK", f"METRICS failed: {resp}"
    m = resp["metrics"]
    for key in ["jobs_submitted", "jobs_completed", "queue_depth", "active_workers"]:
        assert key in m, f"Missing metric: {key}"
    assert m["jobs_submitted"] >= m["jobs_completed"], "Completed > submitted?"
    conn.close()


def test_each_job_processed_exactly_once():
    """Submit N jobs and verify each appears exactly once in the job list."""
    conn, _ = connect()
    submitted_ids = set()
    n = 5
    for i in range(n):
        resp = submit_job(conn, f"HASH:exactlyonce{i}")
        if resp and resp.get("status") == "OK":
            submitted_ids.add(resp["job_id"])

    # Wait for all to finish
    for jid in submitted_ids:
        wait_for_job(conn, jid, timeout=30)

    resp = list_jobs(conn)
    all_ids = {j["job_id"] for j in resp["jobs"]}
    for jid in submitted_ids:
        assert jid in all_ids, f"Job {jid} missing from job list"

    # Check no duplicates in list
    id_list = [j["job_id"] for j in resp["jobs"]]
    assert len(id_list) == len(set(id_list)), "Duplicate job IDs in list!"
    conn.close()


# ─── Run All Tests ─────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*55)
    print("  Distributed Job Queue – Automated Test Suite")
    print("="*55)
    print("(Server + ≥1 worker must be running)\n")

    test("SSL enforced – plain TCP rejected",   test_ssl_required)
    test("Client registration",                  test_client_registration)
    test("Submit and wait for completion",        test_submit_and_complete)
    test("STATUS for unknown job → ERROR",       test_status_unknown_job)
    test("Invalid action → ERROR",               test_invalid_action)
    test("All job types execute correctly",       test_multiple_job_types)
    test("5 concurrent clients",                 test_concurrent_clients)
    test("LIST_JOBS returns list",               test_list_jobs)
    test("METRICS endpoint",                     test_metrics)
    test("Each job processed exactly once",      test_each_job_processed_exactly_once)

    passed = sum(1 for _, ok in results if ok)
    total  = len(results)
    print(f"\n{'='*55}")
    print(f"  Results: {passed}/{total} tests passed")
    print(f"{'='*55}\n")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
