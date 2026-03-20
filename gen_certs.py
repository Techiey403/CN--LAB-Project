"""
Generate self-signed SSL certificates for the job queue server.
Run this once before starting the server.
"""
import subprocess
import sys
import os

def generate_certs():
    if os.path.exists("server.crt") and os.path.exists("server.key"):
        print("[INFO] Certificates already exist. Delete server.crt and server.key to regenerate.")
        return

    print("[INFO] Generating self-signed SSL certificate...")
    cmd = [
        "openssl", "req", "-x509", "-newkey", "rsa:2048",
        "-keyout", "server.key",
        "-out", "server.crt",
        "-days", "365",
        "-nodes",
        "-subj", "/CN=localhost"
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        print("[OK] server.crt and server.key generated.")
    except FileNotFoundError:
        print("[ERROR] openssl not found. Install OpenSSL and retry.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Certificate generation failed: {e.stderr.decode()}")
        sys.exit(1)

if __name__ == "__main__":
    generate_certs()
