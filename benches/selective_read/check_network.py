"""Calibrate the 200 +/- 20 ms, 150 Mbit/s profile against signed MinIO reads."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess
import time
import uuid

import network
import observe
import storage
from storage import digest, save


def check(directory, output):
    output.mkdir()
    with storage.exclusive(directory):
        storage.verify_server(directory)
        config = network.config(directory)
        assert config and config["profile"] == dict(latency_ms=200, jitter_ms=20, mbps=150, seed=config["profile"]["seed"])
        body = bytes(range(256)) * (8 * 1024**2 // 256)
        source = output / "payload.bin"
        source.write_bytes(body)
        key = "_checks/network-" + uuid.uuid4().hex + "/payload.parquet"
        storage.put_verified(directory, key, source, digest(source))
        secret = storage.credentials(directory)
        signer = f'user = "{secret["access_key"]}:{secret["secret_key"]}"\naws-sigv4 = "aws:amz:us-east-1:s3"\n'
        def fetch(*options, object_key=key):
            proc = subprocess.Popen(["curl", "--disable", "--silent", "--show-error", "--fail-with-body", "--noproxy", "*",
                "--config", "-", "--max-time", "15", "--no-buffer", config["endpoint"] + "/" + storage.BUCKET + "/" + object_key,
                *options], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            proc.stdin.write(signer.encode())
            proc.stdin.close()
            proc.stdin = None
            return proc
        def drain():
            deadline = time.monotonic() + 10
            while network.control(directory)["active"]:
                assert time.monotonic() < deadline, "proxy failed to drain"
                time.sleep(.01)
        network.control(directory, reset=True, traced=True)
        started = time.perf_counter()
        proc = fetch()
        first = proc.stdout.read(32768)
        first_seconds = time.perf_counter() - started
        rest = proc.stdout.read()
        error = proc.stderr.read()
        assert proc.wait() == 0, error.decode()
        seconds = time.perf_counter() - started
        assert first + rest == body
        assert .17 <= first_seconds < .5 and seconds - first_seconds > .35, (first_seconds, seconds)
        for options, expected in ((["--range", "11-23"], body[11:24]), (["--range", "-8"], body[-8:])):
            proc = fetch(*options)
            received, error = proc.communicate()
            assert proc.returncode == 0 and received == expected, error.decode()
        proc = fetch("--head")
        received, error = proc.communicate()
        assert proc.returncode == 0 and f"Content-Length: {len(body)}".lower().encode() in received.lower(), error.decode()
        drain()
        serial = network.finish(directory, output)
        assert serial["requests"] == 4 and serial["response_bytes"] == len(body) + 21, serial
        records = [json.loads(line) for line in (output / "network-requests.jsonl").read_text().splitlines()]
        assert all(r["request_id"] and 180000 <= r["delay_us"] <= 220000 and not r["incomplete_body"] for r in records)
        def part(_):
            proc = fetch("--range", "0-4194303")
            received, error = proc.communicate()
            assert proc.returncode == 0 and received == body[:4194304], error.decode()
        network.control(directory, reset=True)
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(part, range(4)))
        parallel_seconds = time.perf_counter() - started
        drain()
        value = network.control(directory)
        assert value["requests"] == 4 and value["response_bytes"] == 16 * 1024**2 and not value.get("records")
        assert 1.03 <= parallel_seconds < 1.6, parallel_seconds
        network.control(directory, reset=True, traced=True)
        for attempt in (1, 2):
            proc = fetch("--header", "amz-sdk-invocation-id: network-check", "--header", f"amz-sdk-request: attempt={attempt}",
                         object_key=key + ".absent")
            received, error = proc.communicate()
            assert proc.returncode == 22 and b"NoSuchKey" in received, (proc.returncode, error.decode())
        drain()
        retries = network.control(directory)
        assert retries["requests"] == 2 and all(r["status"] == 404 for r in retries["records"])
        save(output / "retry-check.json", retries)
        network.control(directory, reset=True, traced=True)
        upstream_before = observe.metrics(directory)
        proc = fetch("--max-time", "0.3")
        partial, error = proc.communicate()
        assert proc.returncode == 28 and 0 < len(partial) < len(body), (proc.returncode, error.decode())
        drain()
        cancelled = network.control(directory)
        deadline = time.monotonic() + 10
        while (upstream_after := observe.metrics(directory))["active"]:
            assert time.monotonic() < deadline
            time.sleep(.01)
        upstream_bytes = upstream_after["response_bytes"] - upstream_before["response_bytes"]
        assert cancelled["requests"] == 1 and cancelled["records"][0]["incomplete_body"]
        assert len(partial) <= cancelled["response_bytes"] < len(body) and upstream_bytes >= cancelled["response_bytes"]
        save(output / "cancellation-check.json", dict(proxy=cancelled, upstream_bytes=upstream_bytes, consumed_bytes=len(partial)))
        save(output / "calibration.json", dict(status="passed", profile=config["profile"],
            first_payload_seconds=first_seconds, serial_seconds=seconds, parallel_seconds=parallel_seconds,
            serial_bytes=len(body), parallel_bytes=16 * 1024**2, source_sha256=digest(Path(__file__)),
            sha256=hashlib.sha256(body).hexdigest(), boundary=network.BOUNDARY,
            explicit_retry_attempts=2, cancelled_transfer="passed"))
        network.control(directory, reset=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check(args.state, args.output)
