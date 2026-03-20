"""
Generate self-signed SSL certificates for the job queue server.
Run this once before starting the server.

Tries openssl CLI first; falls back to the 'cryptography' Python package.
Install fallback with: pip install cryptography
"""
import os
import sys
import subprocess

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CERT_PATH = os.path.join(BASE_DIR, "server.crt")
KEY_PATH  = os.path.join(BASE_DIR, "server.key")


def generate_with_openssl():
    cmd = [
        "openssl", "req", "-x509", "-newkey", "rsa:2048",
        "-keyout", KEY_PATH,
        "-out", CERT_PATH,
        "-days", "365",
        "-nodes",
        "-subj", "/CN=localhost"
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    print("[OK] Certificates generated via openssl CLI.")


def generate_with_cryptography():
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        import datetime
    except ImportError:
        print("[ERROR] Neither openssl CLI nor the 'cryptography' package is available.")
        print("        Install it with: pip install cryptography")
        sys.exit(1)

    print("[INFO] Generating certificate via Python 'cryptography' package...")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
    ])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.IPAddress(__import__("ipaddress").IPv4Address("127.0.0.1")),
            ]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    with open(KEY_PATH, "wb") as f:
        f.write(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ))

    with open(CERT_PATH, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    print("[OK] Certificates generated via cryptography package.")


def generate_certs():
    if os.path.exists(CERT_PATH) and os.path.exists(KEY_PATH):
        print("[INFO] Certificates already exist. Delete server.crt and server.key to regenerate.")
        return

    # Try openssl CLI first
    try:
        generate_with_openssl()
    except (FileNotFoundError, subprocess.CalledProcessError):
        print("[INFO] openssl CLI not found or failed, trying Python fallback...")
        generate_with_cryptography()


if __name__ == "__main__":
    generate_certs()
