# Distributed Job Queue Service

**Socket Programming Mini Project – Problem 12**  
Implements a TCP + SSL/TLS secured, multi-client, multi-worker distributed job queue.

---

## Architecture

```
  ┌─────────┐         ┌─────────┐         ┌─────────┐
  │ Client1 │─── TLS──│         │──── TLS──│ Worker1 │
  ├─────────┤         │         │          ├─────────┤
  │ Client2 │─── TLS──│  Server │──── TLS──│ Worker2 │
  ├─────────┤         │         │          ├─────────┤
  │ ClientN │─── TLS──│         │──── TLS──│ WorkerN │
  └─────────┘         └────┬────┘          └─────────┘
                           │
                    ┌──────┴──────┐
                    │  Job Queue  │  (thread-safe in-memory)
                    │  Jobs DB    │  (dict, all job states)
                    │  Metrics    │  (throughput, latency)
                    └─────────────┘
```

### Components
| File | Role |
|---|---|
| `server.py` | Central server – accepts clients & workers, manages job queue |
| `worker.py` | Worker node – fetches jobs, executes, reports results |
| `client.py` | Client – submits jobs, queries status, views metrics |
| `performance_eval.py` | Load test – measures throughput, latency, scalability |
| `tests/test_suite.py` | Automated tests – correctness, concurrency, edge cases |

### Protocol (Custom, over TCP + TLS)
All messages are framed as `[4-byte big-endian length][JSON payload]`.

**Client actions:** `SUBMIT`, `STATUS`, `LIST_JOBS`, `METRICS`  
**Worker actions:** `FETCH`, `COMPLETE`, `FAIL`, `HEARTBEAT`

---

## Setup

### Requirements
- Python 3.8+
- OpenSSL (for cert generation)

### 1. Generate SSL Certificates
```bash
mkdir -p certs
openssl req -x509 -newkey rsa:4096 -keyout certs/server.key -out certs/server.crt \
  -days 365 -nodes -subj "/CN=localhost"
```

### 2. Install Dependencies
No external packages required – uses Python standard library only.

---

## Running the System

### Terminal 1 – Start Server
```bash
python server.py
```
Listens on port 9000 with TLS enabled.

### Terminal 2+ – Start Workers (run multiple for concurrency)
```bash
python worker.py          # auto-assigned ID
python worker.py worker1  # custom ID
python worker.py worker2
```

### Terminal 3 – Start Client (interactive)
```bash
python client.py
```

**Available commands:**
```
>>> submit COMPUTE:5000       # CPU-bound: sum of squares up to 5000
>>> submit FIBONACCI:35       # Compute Fibonacci number
>>> submit HASH:hello_world   # SHA-256 hash
>>> submit PRIME:100          # Find primes up to 100
>>> submit SLEEP:2            # Simulate I/O delay of 2 seconds
>>> status <job_id>           # Check a specific job
>>> wait <job_id>             # Block until job finishes
>>> list                      # Show all jobs
>>> metrics                   # Show server performance metrics
>>> quit
```

### Batch Mode
```bash
python client.py batch "COMPUTE:1000" "HASH:test" "FIBONACCI:30"
```

---

## Running Tests
```bash
# Server + at least 1 worker must be running first
python tests/test_suite.py
```

## Performance Evaluation
```bash
# Server + at least 2 workers must be running
python performance_eval.py
```
Produces a report in `logs/perf_report.json`.

---

## Job Types

| Type | Format | Description |
|---|---|---|
| COMPUTE | `COMPUTE:<n>` | Sum of squares 0..n |
| FIBONACCI | `FIBONACCI:<n>` | Nth Fibonacci number |
| HASH | `HASH:<text>` | SHA-256 of text |
| PRIME | `PRIME:<n>` | Primes up to n |
| SLEEP | `SLEEP:<secs>` | Simulates I/O wait |

---

## Key Features

### Exactly-Once Execution
Each job has a UUID. Once ASSIGNED to a worker, it's removed from the queue. The Jobs DB tracks all state transitions.

### Worker Failure & Re-queuing
- Workers send `HEARTBEAT` every 5 seconds
- Server watchdog thread checks every 5 seconds; if a worker's `last_seen` > 15s, it is declared dead
- Any `ASSIGNED` job held by the dead worker is re-queued as `PENDING`
- Abrupt disconnections are also caught and trigger re-queuing

### SSL/TLS Security
- TLS 1.2+ enforced via `ssl.SSLContext`
- Self-signed certificate (swap for CA-signed cert in production)
- Plain TCP connections are rejected at the SSL handshake level

### Concurrency
- Each client/worker connection runs in its own daemon thread
- `threading.Lock` protects all shared state (jobs_db, workers, clients)
- `queue.Queue` provides thread-safe job dispatch

---

## Logs
| File | Contents |
|---|---|
| `logs/server.log` | Server activity, job assignments, errors |
| `logs/worker.log` | Worker activity, job execution |
| `logs/client.log` | Client submissions |
| `logs/perf_report.json` | Performance evaluation results |

---

## Evaluation Criteria Coverage

| Criterion | How it's met |
|---|---|
| Problem Definition & Architecture | Multi-component design; custom framed protocol |
| Core Implementation | Raw `socket`, `ssl`, `bind/listen/accept`, manual framing |
| Feature Implementation (D1) | Multi-client, multi-worker; SSL done; job lifecycle |
| Performance Evaluation | `performance_eval.py` – throughput, latency, P95 |
| Optimization & Fixes | Watchdog re-queuing; edge case handling; error responses |
| Final Demo (D2) | Full GitHub repo with README, all source files |
