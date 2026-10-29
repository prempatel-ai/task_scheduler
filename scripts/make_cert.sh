#!/usr/bin/env bash
# Generate self-signed certificate and private key with SAN for HTTPS testing
set -e

KEY_FILE="${1:-key.pem}"
CERT_FILE="${2:-cert.pem}"

openssl req -x509 -newkey rsa:2048 -nodes -sha256 \
  -keyout "$KEY_FILE" \
  -out "$CERT_FILE" \
  -days 365 \
  -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"

echo "Certificate generated: $CERT_FILE"
echo "Key generated: $KEY_FILE"
