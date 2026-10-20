"""Independent Arrow reads for the native file-organization controller."""

import os
from pathlib import Path
import sys

import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "runners")]
import native
from oracle import BATCH_ROWS, inside, same_rows, verify_object


def batches(root, table):
    for item in table["files"]:
        path = verify_object(inside(root, table["path"]), item)
        yield from pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS)


def boundaries(table):
    return {"rows": table["rows"], "file_count": table["file_count"], "files": [
        {**{k: item[k] for k in ("rows", "source_ordinal_range")},
         "delta_stats": {"numRecords": item["delta_stats"]["numRecords"]},
         "row_groups": [{"rows": group["rows"]} for group in item["row_groups"]]}
        for item in table["files"]]}


def geometry(table, files):
    native.call("workload", {"action": "file-geometry", "table": boundaries(table), "files": files})


if __name__ == "__main__":
    if sys.argv[1:2] not in (["prepare"], ["report"]):
        raise SystemExit("usage: file_organizations.py {prepare,report} --help")
    binary = str(native.executable())
    command = [binary, "selective-read", sys.argv[1] + "-files", *sys.argv[2:]]
    if sys.argv[1] == "prepare":
        command += ["--python", sys.executable, "--harness", str(HERE)]
    os.execv(binary, command)
