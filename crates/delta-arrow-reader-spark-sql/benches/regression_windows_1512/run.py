"""Pair two reviewed Linux release builds with the #1391 execution harness."""

from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import resource
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
MANIFEST = HERE.parent / "functions/manifest.json"


def checksum(file):
    with file.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def cap_memory():
    cap = 12 * 1024**3
    _, hard = resource.getrlimit(resource.RLIMIT_AS)
    if hard != resource.RLIM_INFINITY:
        cap = min(cap, hard)
    resource.setrlimit(resource.RLIMIT_AS, (cap, cap))


def read_lines(data):
    return [json.loads(line) for line in data.splitlines(keepends=True) if line.endswith(b"\n")]


def verify(records, ids, config):
    if not records or records[-1] != {"status": "complete"}:
        return False
    if len(records) != 4 * len(ids) + 1:
        return False
    for i, case in enumerate(ids):
        *events, result = records[i * 4:i * 4 + 4]
        types = ["Int64", "UInt64" if case == "window.window.regr_count" else "Float64"]
        if result.get("status") != "ok" or result["id"] != case or result["configuration"] != config:
            return False
        if result["types"] != types or result["samples"] != [event["sample"] for event in events]:
            return False
        for iteration, event in enumerate(events, 1):
            sample = event["sample"]
            if (event["kind"], event["id"], event["configuration"], event["types"]) != (
                "retained_sample", case, config, types
            ):
                return False
            if (sample["iteration"], sample["input_rows"], sample["output_rows"]) != (
                iteration, 1_000_000, 1_000_000
            ):
                return False
            if sample["increment_ns"] != sample["execution_ns"] - sample["projection_ns"]:
                return False
    return True


def main(base_receipt, candidate_receipt, cases, archive):
    ids = json.loads(cases.read_text())
    all_ids = [case["id"] for case in json.loads(MANIFEST.read_text())["functions"]
               if case["id"].startswith("window.window.regr_")]
    assert len(all_ids) == 9 and ids and ids == [case for case in all_ids if case in ids]
    receipts = {}
    binaries = {}
    for arm, path in [("staging", base_receipt), ("candidate", candidate_receipt)]:
        receipts[arm] = receipt = json.loads(path.read_text())
        binaries[arm] = binary = path.parent / "functions"
        assert receipt["binary_sha256"] == checksum(binary)
        assert receipt["profile"] == "release" and receipt["target"] == "functions"
    assert receipts["staging"]["toolchain"] == receipts["candidate"]["toolchain"]
    assert receipts["staging"]["debug_info"] == receipts["candidate"]["debug_info"] == 1
    protocol_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=HERE, text=True
    ).strip()
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=HERE)
    assert receipts["candidate"]["source_commit"] == protocol_commit
    assert receipts["staging"]["source_commit"] == "b78d82bfb3c707c00671c4345c1ac3992149a6ad"
    stop_at = time.monotonic() + 45 * 60
    failures = 0
    with archive.open("xb") as output, gzip.GzipFile(
        fileobj=output, filename="", mode="wb", mtime=0
    ) as raw:
        def record(value):
            raw.write((json.dumps(value, separators=(",", ":")) + "\n").encode())

        record({"protocol_commit": protocol_commit, "manifest_sha256": checksum(MANIFEST),
                "functions": ids, "binaries": receipts,
                "started_utc": datetime.now(timezone.utc).isoformat()})
        for block in range(1, 5):
            modes = ("spark-off", "spark-on") if block % 2 else ("spark-on", "spark-off")
            arms = ("staging", "candidate") if block in (1, 4) else ("candidate", "staging")
            for mode in modes:
                for arm in arms:
                    context = {"block": block, "configuration": mode, "arm": arm}
                    remaining = stop_at - time.monotonic()
                    if remaining <= 0:
                        status, records = "session_timeout", []
                    else:
                        command = ["taskset", "-c", "2,3,4,5", str(binaries[arm]), "measure-set",
                                   str(MANIFEST), str(cases), mode]
                        try:
                            result = subprocess.run(
                                command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                env={"PATH": "/usr/bin:/bin", "TZ": "UTC"},
                                timeout=min(240, remaining), preexec_fn=cap_memory,
                            )
                            records = read_lines(result.stdout)
                            status = "ok" if result.returncode == 0 and verify(records, ids, mode) else "invalid"
                        except subprocess.TimeoutExpired as error:
                            status, records = "process_timeout", read_lines(error.stdout or b"")
                    for sample in records:
                        record(context | {"observation": sample})
                    record(context | {"process_status": status})
                    raw.flush()
                    print(block, mode, arm, status, flush=True)
                    failures += status != "ok"
        record({"failed_processes": failures, "finished_utc": datetime.now(timezone.utc).isoformat()})
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main(*(Path(arg).resolve() for arg in sys.argv[1:])))
