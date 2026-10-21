"""Verify synthetic service-account JWTs captured by the local storage fixture."""

import base64
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def decode(part):
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


def main():
    tokens = json.loads(Path(sys.argv[1]).read_text())
    assert tokens, "storage received no JWTs"
    key = Path(__file__).parents[1] / "test-key.pem"
    with tempfile.TemporaryDirectory(prefix="dar-jwt-") as temporary:
        public_key = Path(temporary) / "public.pem"
        signature = Path(temporary) / "signature.bin"
        subprocess.run(
            ["openssl", "pkey", "-in", str(key), "-pubout", "-out", str(public_key)],
            check=True, capture_output=True, timeout=10,
        )
        for token in set(tokens):
            header, payload, encoded_signature = token.split(".")
            assert json.loads(decode(header)) == {
                "alg": "RS256", "typ": "JWT", "kid": "secret-key-id",
            }
            claims = json.loads(decode(payload))
            assert claims["iss"] == claims["sub"] == "secret-selected@example.test"
            assert claims["iat"] <= time.time() < claims["exp"]
            assert claims["exp"] - claims["iat"] == 3600
            signature.write_bytes(decode(encoded_signature))
            subprocess.run(
                ["openssl", "dgst", "-sha256", "-verify", str(public_key),
                 "-signature", str(signature)],
                input=f"{header}.{payload}".encode(),
                check=True, capture_output=True, timeout=10,
            )


if __name__ == "__main__":
    main()
