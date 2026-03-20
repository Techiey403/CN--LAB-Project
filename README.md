# Distributed Job Queue Service

A secure, socket-based distributed job queue system built with Python.  
Multiple clients submit jobs; multiple workers fetch and execute them reliably over **TCP + SSL/TLS**.

---

## Architecture

```
 [Client 1] ──┐
 [Client 2] ──┤   TCP+TLS    ┌─────────────────────┐   TCP+TLS   ┌──────────┐
 [Client N] ──┼─────────────▶│   Job Queue Server  │────────────▶│ Worker 1 │
              │              │  - Job Queue (FIFO)  │             └──────────┘
              │              │  - Job State Store   │   TCP+TLS   ┌──────────┐
              └──────────────│  - Metrics           │────────────▶│ Worker 2 │
                             └─────────────────────┘             └──────────┘
```

**Communication Flow:**
1. Client connects → sends `{"role": "client"}` → submits jobs
2. Worker connects → sends `{"role": "worker"}` → receives jobs → executes → reports result
3. Server re-queues jobs if a worker disconnects mid-execution
4. All messages are length-prefixed JSON over TLS

**Protocol Design:**
- 4-byte big-endian length prefix + JSON body
- Roles: `client` | `worker`
- Client actions: `submit`, `status`, `metrics`
- Server→Worker actions: `execute`, `ping`
- Worker→Server actions: `result`, `pong`

---

## Setup

### Prerequisites
- Python 3.9+
- `cryptography` package (for cert generation if OpenSSL CLI is not installed)

```bash
pip install cryptography
```

### Generate SSL Certificates (server machine only)
```bash
python gen_certs.py
```
This creates `server.crt` and `server.key`. Copy `server.crt` to the client machine — the private key (`server.key`) stays on the server only.

---

## Running

### SERVER Machine (your computer)

**Step 1 — Generate SSL certificates (once only)**
```bash
python gen_certs.py
```

**Step 2 — Start the server (keep this terminal open)**
```bash
python server.py
```

**Step 3 — Start one or more workers (open a new terminal for each)**
```bash
python worker.py --host localhost --port 9000
```

**Optional — Find your local IP to share with the client machine**
```bash
# Windows
ipconfig
# Look for "IPv4 Address" under your active WiFi or Ethernet adapter
# e.g. 192.168.1.10
```

**Optional — Open firewall port (run as admin)**
```bash
netsh advfirewall firewall add rule name="JobQueue" dir=in action=allow protocol=TCP localport=9000
```

---

### CLIENT Machine (other computer)

**Files needed — copy these from the server machine:**
```
client.py
performance_test.py
server.crt        ← must match the server's certificate
```

**Step 1 — Submit jobs**
```bash
# Replace 192.168.1.10 with the server machine's actual IP
python client.py --host 192.168.1.10 --port 9000 --jobs 3 --payload "echo hello"

# Submit jobs and fetch live server metrics
python client.py --host 192.168.1.10 --port 9000 --jobs 3 --metrics
```

**Step 2 — Run performance test**
```bash
# 5 concurrent clients, 4 jobs each
python performance_test.py --host 192.168.1.10 --port 9000 --clients 5 --jobs 4
```

> Both machines must be on the same network (same WiFi or LAN).

---

## File Structure

```
.
├── server.py            # Central job queue server
├── client.py            # Job submission client
├── worker.py            # Job execution worker
├── performance_test.py  # Concurrent load & latency benchmarking
├── gen_certs.py         # SSL certificate generator
├── server.crt           # Generated TLS certificate (after gen_certs.py)
├── server.key           # Generated TLS private key (after gen_certs.py)
└── README.md
```

---

## Key Features

| Feature | Details |
|---|---|
| Transport | TCP sockets (raw `socket` module) |
| Security | SSL/TLS via Python `ssl` module, TLS 1.2+ |
| Concurrency | `threading.Thread` per connection |
| Job Queue | `queue.Queue` (thread-safe FIFO) |
| Exactly-once | Job state machine: PENDING → ASSIGNED → DONE/FAILED |
| Fault tolerance | Worker disconnect triggers job re-queue |
| Metrics | Live counters: submitted, completed, failed, requeued |

---

## Performance Metrics (sample output)

```
=======================================================
  Performance Test: 5 clients x 4 jobs each
=======================================================

Results:
  Total jobs completed : 20
  Total errors         : 0
  Total elapsed time   : 8.42s
  Throughput           : 2.38 jobs/sec
  Avg latency          : 2.104s
  Min latency          : 0.812s
  Max latency          : 4.231s
  Median latency       : 1.987s

Server Metrics:
  jobs_submitted           : 20
  jobs_completed           : 20
  jobs_failed              : 0
  jobs_requeued            : 0
=======================================================
```

---

## Edge Cases Handled

- Abrupt client disconnection (try/except on recv)
- Worker failure mid-job → job re-queued automatically
- SSL handshake failures → logged and connection closed
- Invalid role/action → error response sent
- Worker timeout (30s) → job re-queued
- Partial reads → `_recv_exact` ensures full message delivery
