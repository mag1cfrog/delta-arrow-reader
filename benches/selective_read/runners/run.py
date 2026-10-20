"""Shared Python adapter identities, protocol checks and immutable artifacts."""

import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import sys


HERE = Path(__file__).resolve().parent
PROTOCOL = HERE.parents[2] / "docs/content/benchmarks/selective-read-protocol.md"
AMENDMENT = PROTOCOL.with_name("selective-read-large-workloads.md")
SAMPLING = PROTOCOL.with_name("selective-read-sampling.md")
PRODUCTION = PROTOCOL.with_name("selective-read-production-workloads.md")
SPARK_MATRIX = PROTOCOL.with_name("selective-read-spark-matrix.md")
if (HERE / "protocol.md").exists():
    PROTOCOL, AMENDMENT = HERE / "protocol.md", HERE / "large-workloads.md"
    SAMPLING = HERE / "sampling.md"
    PRODUCTION = HERE / "production-workloads.md"
    SPARK_MATRIX = HERE / "spark-matrix.md"
COMPARISON_FIELDS = ("comparison_revision", "protocol_sha256")
LARGE_IDENTITY_FIELDS = ("base_protocol_sha256", "workload_manifest_sha256")
SAMPLING_IDENTITY_FIELDS = ("sampling_sha256",)
PRODUCTION_IDENTITY_FIELDS = ("sampling_stage",)
BUDGET = {"worker_threads": 8, "max_blocking_threads": 64, "target_partitions": 8,
          "batch_rows": 8192, "datafusion_pool_bytes": 4 * 1024**3,
          "process_memory_bytes": 8 * 1024**3, "logical_cpus": 8}
_DIGESTS = {}


def file_identity(path):
    stat = os.stat(path)
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def digest(path):
    """Hash an unchanged local inode once per process, including hard links."""
    with Path(path).open("rb") as source:
        identity = file_identity(source.fileno())
        previous = _DIGESTS.get(identity[:2])
        value = previous[1] if previous and previous[0] == identity else hashlib.file_digest(source, "sha256").hexdigest()
        if file_identity(source.fileno()) != identity or file_identity(path) != identity:
            raise ValueError(f"file changed during checksum: {path}")
        _DIGESTS[identity[:2]] = identity, value
        return value


def save(path, value):
    with path.open("x") as out:
        json.dump(value, out, sort_keys=True, indent=2)
        out.write("\n")


def load_adapter(binary):
    """Import the built adapter with its bundled helpers in a probe interpreter."""
    previous = {name: sys.modules.pop(name, None) for name in ("run", "python_common")}
    search_path = sys.path.copy()
    try:
        return runpy.run_path(str(binary.resolve()), run_name="native_probe")
    finally:
        sys.path[:] = search_path
        for name, module in previous.items():
            sys.modules.pop(name, None)
            if module is not None:
                sys.modules[name] = module


def comparison_identity(value):
    revision = value["comparison_revision"]
    if type(revision) is not int or revision not in (2, 3, 4, 5, 6):
        raise ValueError("unknown comparison revision")
    fields = COMPARISON_FIELDS + (LARGE_IDENTITY_FIELDS if revision >= 3 else ())
    expected = digest(SPARK_MATRIX if revision == 6 else PRODUCTION if revision == 5 else AMENDMENT if revision >= 3 else PROTOCOL)
    if value["protocol_sha256"] != expected:
        raise ValueError("stale comparison protocol")
    if revision >= 3:
        if value.get("base_protocol_sha256") != digest(PROTOCOL) or not isinstance(value.get("workload_manifest_sha256"), str) or not re.fullmatch(
                "[0-9a-f]{64}", value["workload_manifest_sha256"]):
            raise ValueError("missing or stale large-workload identity")
    elif any(value.get(key) is not None for key in LARGE_IDENTITY_FIELDS):
        raise ValueError("revision 2 cannot carry large-workload identities")
    if revision >= 4:
        if value.get("sampling_sha256") != digest(SAMPLING):
            raise ValueError("missing or stale sampling identity")
        fields += SAMPLING_IDENTITY_FIELDS
    elif value.get("sampling_sha256") is not None:
        raise ValueError("historical revisions cannot carry a sampling amendment")
    if revision >= 5:
        if value.get("sampling_stage") not in ("pilot", "formal"):
            raise ValueError("missing or invalid production sampling stage")
        fields += PRODUCTION_IDENTITY_FIELDS
    elif value.get("sampling_stage") is not None:
        raise ValueError("historical revisions cannot carry a production sampling stage")
    return {key: value[key] for key in fields}


HISTORICAL_READERS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft")


def reader_roster(value):
    revision = value.get("comparison_revision", 2)
    if type(revision) is not int or revision not in (2, 3, 4, 5, 6):
        raise ValueError("unknown comparison reader roster")
    return (*HISTORICAL_READERS[:-1], "spark") if revision == 6 else HISTORICAL_READERS


def query_count(value):
    count = value.get("concurrent_queries")
    if count is not None:
        if (type(count) is not int or count not in (2, 4) or value["execution_mode"] != "reuse"
                or value.get("comparison_revision") != 6):
            raise ValueError("concurrent queries require revision 6, reuse mode and a count of 2 or 4")
        return count
    if value["execution_mode"] == "open":
        return 1
    return 2 if value.get("comparison_revision", 2) >= 4 else 10


def fixture_tables(manifest):
    if manifest.get("status") != "complete":
        raise ValueError("incomplete fixture manifest")
    if manifest["protocol"] == "selective-read-v1":
        return manifest["tables"]
    if manifest["protocol"] in ("selective-read-production-fixtures-v1", "selective-read-production-pairs-v1"):
        if manifest["writer"]["status"] != "complete":
            raise ValueError("incomplete production writer")
        return manifest["writer"]["tables"]
    raise ValueError("unknown fixture manifest")
