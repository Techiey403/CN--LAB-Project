# Distributed Job Queue Service
### Socket Programming – Jackfruit Mini Project (#12)
### Deliverable 1

---

## Overview

A distributed job queue system built using **low-level TCP sockets** with **SSL/TLS** encryption.  
Multiple clients submit jobs, multiple worker nodes execute them, and a central server manages everything.

```
┌──────────┐        TLS/TCP         ┌─────────────────────┐        TLS/TCP        ┌──────────────┐
│ Client A │ ──── submit_job ──────▶│                     │──── assign_job ──────▶│  Worker Alpha│
│ Client B │ ──── submit_job ──────▶│   Job Queue Server  │◀─── job_done ─────────│  Worker Beta │
│ Client C │ ──── query_job  ──────▶│   (Central Brain)   │──── assign_job ──────▶│  Worker Gamma│
└──────────┘                        └─────────────────────┘                        └──────────────┘
                                           │
                                    ┌──────┴───────┐
                                    │  Job Queue   │  Thread-safe FIFO
                                    │  ──────────  │  pending_jobs queue
                                    │  [ job1 ]   │
                                    │  [ job2 ]   │
                                    │  [ job3 ]   │
                                    └─────────────┘
```

---

## Project Structure

```
job_queue_service/
├── server.py            ← Central server (TCP listener, job queue, worker management)
├── worker.py            ← Worker node (connects to server, executes jobs)
├── client.py            ← Client CLI (submit jobs, query status, list all jobs)
├── test_integration.py  ← Full end-to-end integration test (Deliverable 1 demo)
├── certs/
│   ├── server.crt       ← Self-signed TLS certificate
│   └── server.key       ← TLS private key
└── logs/
    └── server.log       ← Server activity log
```

---

## Requirements

- Python 3.10+
- No external libraries needed (uses only standard library)

---

## Quick Start

### 1. Generate TLS Certificates (already done if you cloned with certs/)
```bash
openssl req -x509 -newkey rsa:4096 -keyout certs/server.key -out certs/server.crt \
  -days 365 -nodes -subj "/C=US/ST=State/L=City/O=JobQueue/CN=localhost"
```

### 2. Run the Integration Test (easiest way to see everything)
```bash
cd job_queue_service
python test_integration.py
```
This starts the server + 3 workers + 5 clients automatically.

---

### Manual Mode (run each component separately)

**Terminal 1 – Start the server:**
```bash
python server.py
```

**Terminal 2 & 3 – Start workers:**
```bash
python worker.py --name Worker-1
python worker.py --name Worker-2
```

**Terminal 4 – Submit jobs from a client:**
```bash
# Submit a job
python client.py --action submit --payload "process the monthly sales report"

# Query its status (use the job_id returned above)
python client.py --action query --job-id <paste-job-id-here>

# List all jobs
python client.py --action list
```

---

## Architecture & Design Decisions

### Protocol
- **Transport**: TCP (raw sockets, `socket.SOCK_STREAM`)
- **Security**: SSL/TLS 1.2+ (`ssl.SSLContext`, self-signed certificate)
- **Message Format**: Newline-delimited JSON
- **Connection Model**: 
  - Clients → request/response (short-lived connection per request)
  - Workers → long-lived persistent connection (server pushes jobs)

### Message Types

| Direction | Message | Purpose |
|-----------|---------|---------|
| Worker → Server | `register_worker` | Worker announces itself |
| Client → Server | `submit_job` | Client submits a new job |
| Client → Server | `query_job` | Client asks for job status |
| Client → Server | `list_jobs` | Client fetches all jobs |
| Server → Worker | `assign_job` | Server pushes a job to a worker |
| Worker → Server | `job_done` | Worker reports success |
| Worker → Server | `job_failed` | Worker reports failure |
| Server → Client | `ack` / `job_status` / `jobs_list` | Server responses |

### Concurrency
- Each connection runs in its own **Python thread** (`threading.Thread`)
- Shared data (`all_jobs`, `active_workers`) is protected by `threading.Lock`
- The pending job queue uses `queue.Queue` (intrinsically thread-safe)

### Failure Handling
- Workers send periodic **heartbeats** (ping every 10s)
- Server **watchdog thread** checks all workers every 10s — any worker silent for 30s is declared dead
- If a worker dies mid-job, the job is **re-queued** automatically
- Jobs are retried up to **3 times** before being marked permanently FAILED

---

## Evaluation Checklist (Deliverable 1)

| Requirement | Implementation |
|-------------|----------------|
| TCP sockets directly | `socket.socket(AF_INET, SOCK_STREAM)` in `server.py` |
| SSL/TLS for all control & data | `ssl.SSLContext.wrap_socket()` on every connection |
| Multiple concurrent clients | Thread-per-connection in `_handle_connection()` |
| Communication over network sockets | All state shared only through TCP; no shared memory |
| Centralized job queue with synchronization | `queue.Queue` + `threading.Lock` in `server.py` |
| Job assignment & acknowledgment | `assign_job` / `ack` message exchange |
| Completion handling | `job_done` / `job_failed` messages, status tracking |
| Worker failure & re-queuing | Watchdog thread + `_requeue_job()` with retry limit |

---

## GitHub Submission Notes

- All code is in a single `job_queue_service/` directory
- `test_integration.py` is the Deliverable 1 demo — run it for a complete walkthrough
- Logs are written to `logs/server.log`
- TLS certs are committed for ease of testing (in production, generate fresh ones)
