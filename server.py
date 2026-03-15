"""
Distributed Job Queue Server - Deliverable 1
=============================================
Socket Programming – Jackfruit Mini Project (#12: Distributed Job Queue Service)

Architecture: Centralized Server (multi-client / multi-worker)
Protocol: TCP with SSL/TLS encryption
Language: Python

This server is the brain of the system. It does three things:
  1. Accepts job submissions from CLIENT connections
  2. Assigns those jobs to WORKER connections
  3. Tracks job state and handles worker failures (re-queuing)

All communication happens over raw TCP sockets wrapped with SSL/TLS.
"""

import socket
import ssl
import threading
import json
import uuid
import time
import logging
import queue
import signal
import sys
from datetime import datetime
from enum import Enum


# ─────────────────────────── Logging Setup ────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/server.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("JobQueueServer")


# ─────────────────────────── Job States ───────────────────────────────────────
class JobStatus(Enum):
    PENDING   = "pending"    # Waiting in the queue
    RUNNING   = "running"    # Assigned to a worker
    DONE      = "done"       # Completed successfully
    FAILED    = "failed"     # Worker died mid-job → will be re-queued


# ─────────────────────────── Message Protocol ─────────────────────────────────
# Every message sent over the socket is a JSON-encoded dict followed by "\n".
# The "type" field tells the receiver what kind of message this is.
#
#  CLIENT → SERVER:
#    { "type": "submit_job", "payload": "shell command or task description" }
#    { "type": "query_job",  "job_id": "uuid" }
#    { "type": "list_jobs" }
#
#  WORKER → SERVER:
#    { "type": "register_worker" }
#    { "type": "job_done",   "job_id": "uuid", "result": "output string" }
#    { "type": "job_failed", "job_id": "uuid", "reason": "error message" }
#
#  SERVER → CLIENT / WORKER:
#    { "type": "ack",           "job_id": "...", "message": "..." }
#    { "type": "assign_job",    "job_id": "...", "payload": "..." }
#    { "type": "job_status",    "job_id": "...", "status": "...", "result": "..." }
#    { "type": "error",         "message": "..." }
#    { "type": "jobs_list",     "jobs": [...] }


# ─────────────────────────── Constants ────────────────────────────────────────
SERVER_HOST              = "0.0.0.0"   # Listen on all interfaces
SERVER_PORT              = 9000        # Single port for clients AND workers
TLS_CERT_FILE            = "certs/server.crt"
TLS_KEY_FILE             = "certs/server.key"
WORKER_HEARTBEAT_TIMEOUT = 30          # Seconds before we declare a worker dead
MAX_JOB_RETRIES          = 3           # How many times a failed job gets re-queued


class JobQueueServer:
    """
    The central server. Holds the job queue and manages all connections.

    Key data structures:
      self.pending_jobs  – a thread-safe FIFO queue of jobs waiting for a worker
      self.all_jobs      – dict of job_id → job_record (for status queries)
      self.active_workers– dict of worker_id → worker metadata (for failure detection)
      self.running_jobs  – dict of job_id → worker_id (to know who's running what)
    """

    def __init__(self):
        # The main pending queue. Workers pop from here.
        self.pending_jobs: queue.Queue = queue.Queue()

        # Master record of every job ever submitted, keyed by job UUID.
        self.all_jobs: dict = {}

        # Workers currently connected, keyed by a generated worker_id.
        self.active_workers: dict = {}

        # Maps job_id → worker_id while a job is being executed.
        self.running_jobs: dict = {}

        # Locks to prevent race conditions when multiple threads touch shared data.
        self.jobs_lock   = threading.Lock()
        self.workers_lock = threading.Lock()

        # Event used to cleanly shut down the server.
        self.shutdown_event = threading.Event()

        # Set up the raw TCP socket.
        self.server_socket = self._create_server_socket()

        # Set up the TLS context (wraps all connections end-to-end).
        self.tls_context = self._create_tls_context()

        log.info("Job Queue Server initialised on %s:%d", SERVER_HOST, SERVER_PORT)


    # ── Socket & TLS Setup ────────────────────────────────────────────────────

    def _create_server_socket(self) -> socket.socket:
        """
        Creates a TCP socket, enables address reuse (so we can restart quickly),
        binds it to the configured host/port, and sets it to listening mode.
        Returns the raw socket (TLS wrapping happens separately).
        """
        raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # SO_REUSEADDR lets us restart the server without waiting for TIME_WAIT.
        raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        raw_socket.bind((SERVER_HOST, SERVER_PORT))
        raw_socket.listen(50)   # Up to 50 connections can queue before being accepted

        return raw_socket


    def _create_tls_context(self) -> ssl.SSLContext:
        """
        Builds a TLS context that wraps every accepted socket with encryption.
        We use TLS 1.2+ and load our self-signed certificate + private key.
        """
        # PROTOCOL_TLS_SERVER means we're the server side of the handshake.
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

        # Enforce at least TLS 1.2 (reject older, weaker versions).
        tls_context.minimum_version = ssl.TLSVersion.TLSv1_2

        # Load the certificate chain and private key.
        tls_context.load_cert_chain(certfile=TLS_CERT_FILE, keyfile=TLS_KEY_FILE)

        return tls_context


    # ── Main Accept Loop ──────────────────────────────────────────────────────

    def run(self):
        """
        The main loop: accept incoming TCP connections, wrap them in TLS,
        and hand each one off to a dedicated handler thread.
        """
        # Start the background watchdog that detects dead workers.
        watchdog_thread = threading.Thread(
            target=self._worker_watchdog_loop, daemon=True, name="Watchdog"
        )
        watchdog_thread.start()

        log.info("Server is ready. Waiting for connections...")

        while not self.shutdown_event.is_set():
            try:
                self.server_socket.settimeout(1.0)  # Lets us check shutdown_event
                raw_client_socket, client_address = self.server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break   # Socket was closed during shutdown

            # Wrap the raw TCP connection with TLS.
            try:
                secure_socket = self.tls_context.wrap_socket(
                    raw_client_socket, server_side=True
                )
            except ssl.SSLError as ssl_error:
                log.warning("TLS handshake failed from %s: %s", client_address, ssl_error)
                raw_client_socket.close()
                continue

            log.info("New TLS connection from %s", client_address)

            # Spawn a thread for this connection so the accept loop stays free.
            connection_thread = threading.Thread(
                target=self._handle_connection,
                args=(secure_socket, client_address),
                daemon=True,
            )
            connection_thread.start()

        log.info("Server shut down cleanly.")


    # ── Per-Connection Handler ─────────────────────────────────────────────────

    def _handle_connection(self, secure_socket: ssl.SSLSocket, client_address: tuple):
        """
        Called in its own thread for every accepted connection.
        Reads the first message to determine whether the caller is a CLIENT or WORKER,
        then delegates to the appropriate handler.
        """
        try:
            first_message = self._receive_message(secure_socket)
            if first_message is None:
                return

            message_type = first_message.get("type", "")

            if message_type == "register_worker":
                # This connection is a WORKER — handle it in worker mode.
                self._handle_worker_connection(secure_socket, client_address, first_message)

            elif message_type in ("submit_job", "query_job", "list_jobs"):
                # This connection is a CLIENT — handle the request and close.
                self._handle_client_request(secure_socket, client_address, first_message)

            else:
                self._send_message(secure_socket, {
                    "type": "error",
                    "message": f"Unknown message type: '{message_type}'"
                })

        except Exception as unexpected_error:
            log.error("Error handling connection from %s: %s", client_address, unexpected_error)
        finally:
            secure_socket.close()


    # ── Client Request Handler ────────────────────────────────────────────────

    def _handle_client_request(
        self,
        secure_socket: ssl.SSLSocket,
        client_address: tuple,
        message: dict,
    ):
        """
        Handles a single request from a job-submitting CLIENT.
        Clients send one request and receive one response (request-response pattern).
        """
        message_type = message.get("type")

        if message_type == "submit_job":
            job_payload = message.get("payload", "").strip()

            if not job_payload:
                self._send_message(secure_socket, {
                    "type": "error",
                    "message": "Job payload cannot be empty."
                })
                return

            # Create a unique ID for this job and record it.
            new_job_id = str(uuid.uuid4())
            job_record = {
                "job_id":      new_job_id,
                "payload":     job_payload,
                "status":      JobStatus.PENDING.value,
                "result":      None,
                "submitted_at": datetime.utcnow().isoformat(),
                "completed_at": None,
                "retries":     0,
            }

            with self.jobs_lock:
                self.all_jobs[new_job_id] = job_record

            # Push the job into the pending queue for a worker to pick up.
            self.pending_jobs.put(job_record)

            log.info("Job submitted [%s]: %s", new_job_id[:8], job_payload[:60])

            self._send_message(secure_socket, {
                "type":    "ack",
                "job_id":  new_job_id,
                "message": "Job accepted and queued."
            })

        elif message_type == "query_job":
            job_id = message.get("job_id", "")

            with self.jobs_lock:
                job_record = self.all_jobs.get(job_id)

            if job_record is None:
                self._send_message(secure_socket, {
                    "type":    "error",
                    "message": f"Job '{job_id}' not found."
                })
            else:
                self._send_message(secure_socket, {
                    "type":   "job_status",
                    "job_id": job_id,
                    "status": job_record["status"],
                    "result": job_record["result"],
                })

        elif message_type == "list_jobs":
            with self.jobs_lock:
                jobs_summary = [
                    {
                        "job_id":       j["job_id"],
                        "payload":      j["payload"][:50],
                        "status":       j["status"],
                        "submitted_at": j["submitted_at"],
                    }
                    for j in self.all_jobs.values()
                ]

            self._send_message(secure_socket, {
                "type": "jobs_list",
                "jobs": jobs_summary,
            })

        log.info("Client %s request handled: %s", client_address, message_type)


    # ── Worker Connection Handler ─────────────────────────────────────────────

    def _handle_worker_connection(
        self,
        secure_socket: ssl.SSLSocket,
        worker_address: tuple,
        registration_message: dict,
    ):
        """
        Manages a long-lived connection with a WORKER node.
        Workers stay connected and receive a stream of job assignments.
        When a worker completes or fails a job, it sends back a result message.

        This runs in a dedicated thread per worker.
        """
        worker_id = str(uuid.uuid4())

        worker_info = {
            "worker_id":      worker_id,
            "address":        worker_address,
            "socket":         secure_socket,
            "connected_at":   time.time(),
            "last_seen_at":   time.time(),
            "current_job_id": None,
        }

        with self.workers_lock:
            self.active_workers[worker_id] = worker_info

        log.info("Worker registered [%s] from %s", worker_id[:8], worker_address)

        # Tell the worker it was registered successfully.
        self._send_message(secure_socket, {
            "type":      "ack",
            "worker_id": worker_id,
            "message":   "Worker registered. Waiting for jobs."
        })

        # Start a separate thread that pushes jobs to this worker.
        job_pusher_thread = threading.Thread(
            target=self._push_jobs_to_worker,
            args=(worker_id,),
            daemon=True,
            name=f"JobPusher-{worker_id[:8]}",
        )
        job_pusher_thread.start()

        # Main loop: listen for results from this worker.
        try:
            while not self.shutdown_event.is_set():
                result_message = self._receive_message(secure_socket)

                if result_message is None:
                    # Worker disconnected.
                    break

                # Update heartbeat timestamp so the watchdog knows this worker is alive.
                with self.workers_lock:
                    if worker_id in self.active_workers:
                        self.active_workers[worker_id]["last_seen_at"] = time.time()

                self._process_worker_result(worker_id, result_message)

        except Exception as connection_error:
            log.warning("Worker [%s] connection error: %s", worker_id[:8], connection_error)
        finally:
            self._deregister_worker(worker_id)


    def _push_jobs_to_worker(self, worker_id: str):
        """
        Runs in its own thread. Waits for jobs to appear in the pending queue,
        then assigns them to the specific worker identified by worker_id.

        If the worker has disconnected, the job is put back in the queue.
        """
        while not self.shutdown_event.is_set():
            try:
                # Block for up to 1 second, then loop (checks shutdown_event).
                pending_job = self.pending_jobs.get(timeout=1.0)
            except queue.Empty:
                continue

            # Make sure this worker is still alive before assigning.
            with self.workers_lock:
                worker_info = self.active_workers.get(worker_id)

            if worker_info is None:
                # Worker is gone — put the job back so another worker gets it.
                log.warning(
                    "Worker [%s] gone before job [%s] could be assigned. Re-queuing.",
                    worker_id[:8], pending_job["job_id"][:8]
                )
                self.pending_jobs.put(pending_job)
                return

            job_id = pending_job["job_id"]

            # Update state: job is now RUNNING, assigned to this worker.
            with self.jobs_lock:
                if job_id in self.all_jobs:
                    self.all_jobs[job_id]["status"] = JobStatus.RUNNING.value

            with self.workers_lock:
                if worker_id in self.active_workers:
                    self.active_workers[worker_id]["current_job_id"] = job_id

            self.running_jobs[job_id] = worker_id

            log.info(
                "Assigning job [%s] to worker [%s]",
                job_id[:8], worker_id[:8]
            )

            success = self._send_message(worker_info["socket"], {
                "type":    "assign_job",
                "job_id":  job_id,
                "payload": pending_job["payload"],
            })

            if not success:
                # Failed to send — worker likely died. Re-queue the job.
                log.warning("Failed to send job [%s] to worker [%s]. Re-queuing.", job_id[:8], worker_id[:8])
                self._requeue_job(job_id)
                return


    def _process_worker_result(self, worker_id: str, result_message: dict):
        """
        Processes the result (done or failed) sent back by a worker.
        Updates the job record with final status and output.
        """
        message_type = result_message.get("type")
        job_id       = result_message.get("job_id", "")

        with self.jobs_lock:
            job_record = self.all_jobs.get(job_id)

        if job_record is None:
            log.warning("Received result for unknown job [%s]. Ignoring.", job_id[:8])
            return

        if message_type == "job_done":
            job_result = result_message.get("result", "")

            with self.jobs_lock:
                self.all_jobs[job_id]["status"]       = JobStatus.DONE.value
                self.all_jobs[job_id]["result"]       = job_result
                self.all_jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()

            self.running_jobs.pop(job_id, None)

            with self.workers_lock:
                if worker_id in self.active_workers:
                    self.active_workers[worker_id]["current_job_id"] = None

            log.info("Job [%s] COMPLETED by worker [%s].", job_id[:8], worker_id[:8])

        elif message_type == "job_failed":
            failure_reason = result_message.get("reason", "Unknown error")
            log.warning(
                "Job [%s] FAILED on worker [%s]: %s",
                job_id[:8], worker_id[:8], failure_reason
            )
            self._requeue_job(job_id, reason=failure_reason)


    def _requeue_job(self, job_id: str, reason: str = "Worker disconnected"):
        """
        Puts a failed/interrupted job back into the pending queue if retries remain.
        After MAX_JOB_RETRIES attempts, the job is marked permanently FAILED.
        """
        with self.jobs_lock:
            job_record = self.all_jobs.get(job_id)
            if job_record is None:
                return

            retry_count = job_record.get("retries", 0)

            if retry_count < MAX_JOB_RETRIES:
                job_record["retries"] += 1
                job_record["status"]   = JobStatus.PENDING.value
                self.pending_jobs.put(job_record)
                log.info(
                    "Job [%s] re-queued (attempt %d/%d). Reason: %s",
                    job_id[:8], retry_count + 1, MAX_JOB_RETRIES, reason
                )
            else:
                job_record["status"] = JobStatus.FAILED.value
                job_record["result"] = f"Failed after {MAX_JOB_RETRIES} retries. Last reason: {reason}"
                log.error("Job [%s] permanently FAILED after %d retries.", job_id[:8], MAX_JOB_RETRIES)

        self.running_jobs.pop(job_id, None)


    def _deregister_worker(self, worker_id: str):
        """
        Cleans up after a worker disconnects.
        If the worker was in the middle of a job, that job is re-queued.
        """
        with self.workers_lock:
            worker_info = self.active_workers.pop(worker_id, None)

        if worker_info is None:
            return

        current_job_id = worker_info.get("current_job_id")

        if current_job_id:
            log.warning(
                "Worker [%s] disconnected while running job [%s]. Re-queuing.",
                worker_id[:8], current_job_id[:8]
            )
            self._requeue_job(current_job_id, reason="Worker disconnected unexpectedly")

        log.info("Worker [%s] deregistered.", worker_id[:8])


    # ── Watchdog ──────────────────────────────────────────────────────────────

    def _worker_watchdog_loop(self):
        """
        Runs in a background thread. Periodically checks all connected workers.
        If a worker hasn't been heard from in WORKER_HEARTBEAT_TIMEOUT seconds,
        it is considered dead and deregistered (which re-queues any in-flight job).
        """
        while not self.shutdown_event.is_set():
            time.sleep(10)   # Check every 10 seconds

            now = time.time()
            dead_worker_ids = []

            with self.workers_lock:
                for worker_id, worker_info in self.active_workers.items():
                    seconds_since_last_seen = now - worker_info["last_seen_at"]
                    if seconds_since_last_seen > WORKER_HEARTBEAT_TIMEOUT:
                        log.warning(
                            "Worker [%s] timed out (%.0f s silent). Marking dead.",
                            worker_id[:8], seconds_since_last_seen
                        )
                        dead_worker_ids.append(worker_id)

            for dead_worker_id in dead_worker_ids:
                self._deregister_worker(dead_worker_id)


    # ── Low-Level Message I/O ─────────────────────────────────────────────────

    def _send_message(self, target_socket: ssl.SSLSocket, message: dict) -> bool:
        """
        Serialises a Python dict to JSON and sends it over the socket.
        Messages are newline-delimited so the receiver knows where each ends.
        Returns True on success, False if the socket is broken.
        """
        try:
            encoded_message = (json.dumps(message) + "\n").encode("utf-8")
            target_socket.sendall(encoded_message)
            return True
        except (OSError, ssl.SSLError) as send_error:
            log.debug("Send failed: %s", send_error)
            return False


    def _receive_message(self, source_socket: ssl.SSLSocket) -> dict | None:
        """
        Reads bytes from the socket until a newline is found, then decodes
        the JSON. Returns None if the connection was closed or broken.
        """
        raw_bytes = b""
        try:
            while b"\n" not in raw_bytes:
                chunk = source_socket.recv(4096)
                if not chunk:
                    return None   # Connection closed gracefully
                raw_bytes += chunk

            return json.loads(raw_bytes.split(b"\n")[0].decode("utf-8"))

        except (json.JSONDecodeError, OSError, ssl.SSLError) as receive_error:
            log.debug("Receive failed: %s", receive_error)
            return None


    # ── Shutdown ──────────────────────────────────────────────────────────────

    def shutdown(self, *_signal_args):
        """Triggered by SIGINT / SIGTERM to gracefully stop the server."""
        log.info("Shutdown signal received. Stopping server...")
        self.shutdown_event.set()
        self.server_socket.close()


# ─────────────────────────── Entry Point ──────────────────────────────────────

if __name__ == "__main__":
    server = JobQueueServer()

    # Register OS signals so Ctrl-C triggers a clean shutdown.
    signal.signal(signal.SIGINT,  server.shutdown)
    signal.signal(signal.SIGTERM, server.shutdown)

    server.run()
