"""
Distributed Job Queue – Worker Node
=====================================
This process connects to the Job Queue Server over TLS and waits to be
assigned jobs. When a job arrives, it executes it (simulated here as a
time-consuming computation) and sends the result back to the server.

Workers are stateless — they don't know about other workers or clients.
All coordination happens through the server.
"""

import socket
import ssl
import json
import time
import logging
import threading
import random
import sys
import argparse

import sys

if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# ─────────────────────────── Logging Setup ────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WORKER %(worker_name)s] %(message)s",
)


class WorkerNode:
    """
    A single worker node that:
      1. Connects to the server over TLS
      2. Registers itself
      3. Waits in a loop to receive job assignments
      4. Executes each job
      5. Reports the result back to the server

    The heartbeat thread periodically sends a ping so the server knows
    this worker is still alive (prevents the watchdog from killing us).
    """

    def __init__(self, worker_name: str, server_host: str, server_port: int):
        self.worker_name   = worker_name
        self.server_host   = server_host
        self.server_port   = server_port
        self.worker_id     = None      # Assigned by the server after registration
        self.secure_socket = None
        self.is_running    = False

        # Logger with the worker name baked in
        self.log = logging.LoggerAdapter(
            logging.getLogger("Worker"),
            {"worker_name": worker_name}
        )


    # ── Connection ────────────────────────────────────────────────────────────

    def _create_tls_context(self) -> ssl.SSLContext:
        """
        Creates a TLS context for the CLIENT side of the connection.
        Since we use a self-signed certificate, we load the server's cert
        directly as a trusted CA (no third-party CA needed).
        """
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        # Trust only our specific self-signed server certificate.
        tls_context.load_verify_locations("certs/server.crt")

        # Enforce encrypted connections — do not accept unverified servers.
        tls_context.check_hostname = False  # hostname = "localhost", cert CN = "localhost"
        tls_context.verify_mode    = ssl.CERT_REQUIRED

        return tls_context


    def connect_to_server(self) -> bool:
        """
        Opens a TLS TCP connection to the job queue server.
        Returns True if successful, False otherwise.
        """
        raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_socket.settimeout(10)

        tls_context = self._create_tls_context()

        try:
            raw_socket.connect((self.server_host, self.server_port))
            self.secure_socket = tls_context.wrap_socket(
                raw_socket, server_hostname=self.server_host
            )
            self.log.info("TLS connection established to %s:%d", self.server_host, self.server_port)
            return True

        except (ConnectionRefusedError, ssl.SSLError, OSError) as connection_error:
            self.log.error("Could not connect to server: %s", connection_error)
            raw_socket.close()
            return False


    # ── Registration & Main Loop ──────────────────────────────────────────────

    def run(self):
        """
        Main entry point for the worker. Connects to the server, registers,
        then enters the job-receive loop.
        """
        if not self.connect_to_server():
            return

        # Send the registration message — this is how the server knows this
        # connection is a WORKER (not a client submitting jobs).
        self._send_message({"type": "register_worker", "name": self.worker_name})

        # Wait for the server's acknowledgement containing our worker_id.
        ack_message = self._receive_message()
        if ack_message is None or ack_message.get("type") != "ack":
            self.log.error("Did not receive registration ACK. Exiting.")
            return

        self.worker_id = ack_message.get("worker_id")
        self.log.info("Registered with server. Worker ID: %s", self.worker_id[:8])

        self.is_running = True

        # Start the heartbeat thread so the server's watchdog doesn't kill us.
        heartbeat_thread = threading.Thread(
            target=self._send_heartbeats, daemon=True, name="Heartbeat"
        )
        heartbeat_thread.start()

        # Main job processing loop.
        self._job_receive_loop()


    def _job_receive_loop(self):
        """
        Continuously waits for the server to push a job assignment.
        When a job arrives, it is executed synchronously and the result
        is sent back before waiting for the next job.
        """
        self.log.info("Ready. Waiting for job assignments from server...")

        while self.is_running:
            incoming_message = self._receive_message()

            if incoming_message is None:
                self.log.warning("Connection to server lost. Stopping.")
                self.is_running = False
                break

            message_type = incoming_message.get("type")

            if message_type == "assign_job":
                job_id      = incoming_message.get("job_id")
                job_payload = incoming_message.get("payload")

                self.log.info("Received job [%s]: %s", job_id[:8], job_payload[:60])

                # Execute the job and get back either a result or an error.
                execution_result, execution_error = self._execute_job(job_id, job_payload)

                if execution_error:
                    # Report failure back to the server — it will re-queue.
                    self._send_message({
                        "type":   "job_failed",
                        "job_id": job_id,
                        "reason": execution_error,
                    })
                    self.log.warning("Job [%s] reported as FAILED: %s", job_id[:8], execution_error)
                else:
                    # Report success.
                    self._send_message({
                        "type":   "job_done",
                        "job_id": job_id,
                        "result": execution_result,
                    })
                    self.log.info("Job [%s] COMPLETED.", job_id[:8])

            elif message_type == "ping":
                # Respond to server-initiated pings (used by watchdog to check liveness).
                self._send_message({"type": "pong", "worker_id": self.worker_id})

            else:
                self.log.debug("Unknown message from server: %s", message_type)


    # ── Job Execution ─────────────────────────────────────────────────────────

    def _execute_job(self, job_id: str, job_payload: str) -> tuple[str | None, str | None]:
        """
        Simulates executing a job. In a real system this would run a shell
        command, a computation, a data pipeline task, etc.

        Here we simulate work by sleeping for a random duration.
        We also simulate random failures (10% chance) to demonstrate
        the server's re-queue / retry behaviour.

        Returns: (result_string, error_string)
          - On success: (result_string, None)
          - On failure: (None, error_string)
        """
        # Simulate a 10% chance of failure to test the server's retry logic.
        if random.random() < 0.10:
            simulated_error = "Simulated random execution failure (10% chance)"
            return None, simulated_error

        # Simulate work taking between 1 and 5 seconds.
        simulated_work_duration = random.uniform(1.0, 5.0)
        self.log.info(
            "Executing job [%s] (simulated %.1f s)...", job_id[:8], simulated_work_duration
        )
        time.sleep(simulated_work_duration)

        # Produce a fake result string.
        execution_result = (
            f"Job '{job_payload[:50]}' completed by worker '{self.worker_name}' "
            f"in {simulated_work_duration:.2f}s"
        )
        return execution_result, None


    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _send_heartbeats(self):
        """
        Runs in a background thread. Sends a lightweight ping to the server
        every 10 seconds. This resets the watchdog timer on the server side
        so the server doesn't think we've died.
        """
        while self.is_running:
            time.sleep(10)
            if self.is_running:
                success = self._send_message({"type": "ping", "worker_id": self.worker_id})
                if not success:
                    self.is_running = False
                    break


    # ── Low-Level Message I/O ─────────────────────────────────────────────────

    def _send_message(self, message: dict) -> bool:
        """Serialises dict → JSON and sends it over the TLS socket."""
        try:
            encoded_message = (json.dumps(message) + "\n").encode("utf-8")
            self.secure_socket.sendall(encoded_message)
            return True
        except (OSError, ssl.SSLError) as send_error:
            self.log.debug("Send error: %s", send_error)
            return False


    def _receive_message(self) -> dict | None:
        """Reads a newline-terminated JSON message from the TLS socket."""
        raw_bytes = b""
        try:
            while b"\n" not in raw_bytes:
                chunk = self.secure_socket.recv(4096)
                if not chunk:
                    return None
                raw_bytes += chunk
            return json.loads(raw_bytes.split(b"\n")[0].decode("utf-8"))
        except (json.JSONDecodeError, OSError, ssl.SSLError):
            return None


# ─────────────────────────── Entry Point ──────────────────────────────────────

if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description="Job Queue Worker Node")
    argument_parser.add_argument(
        "--name", default="Worker-1",
        help="Human-readable name for this worker (e.g. Worker-1)"
    )
    argument_parser.add_argument(
        "--host", default="localhost",
        help="Server hostname or IP"
    )
    argument_parser.add_argument(
        "--port", type=int, default=9000,
        help="Server port"
    )
    args = argument_parser.parse_args()

    worker = WorkerNode(
        worker_name=args.name,
        server_host=args.host,
        server_port=args.port,
    )

    try:
        worker.run()
    except KeyboardInterrupt:
        print(f"\n[{args.name}] Shutting down.")
        worker.is_running = False
