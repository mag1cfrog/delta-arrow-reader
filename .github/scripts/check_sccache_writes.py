import json
import os
from pathlib import Path
import subprocess


def check_writes(stats, log):
    count = stats["cache_write_errors"]
    if not count:
        return
    errors = [line for line in log.splitlines() if "Error executing cache write:" in line][-count:]
    # Concurrent builds can upload the same immutable entry before either sees it.
    if len(errors) != count or any("AlreadyExists" not in line for line in errors):
        raise RuntimeError(f"sccache reported {count} cache write errors\n" + "\n".join(errors))
    print(f"Ignoring {count} cache entries that already exist")


if __name__ == "__main__":
    stats = json.loads(subprocess.check_output(["sccache", "--show-stats", "--stats-format", "json"]))
    log = Path(os.environ["SCCACHE_ERROR_LOG"])
    check_writes(stats["stats"], log.read_text(errors="replace") if log.exists() else "")
