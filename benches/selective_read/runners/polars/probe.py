"""Exercise snapshot reuse through the actual Polars SDK."""

from pathlib import Path
import shutil
import sys
from unittest import TestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run
from capabilities import CORPUS


def native_probes(binary, output):
    adapter = run.load_adapter(binary)
    table = output / "reuse-table"
    shutil.copytree(CORPUS / "deletion_vectors/table", table)
    source = adapter["scan"]({"table_uri": table.as_uri(), "snapshot_version": 1}, {})
    source.collect_schema()
    (table / "_delta_log").rename(table / "hidden-log")
    counts = []
    try:
        for predicate in ("", " WHERE id IN (1, 2, 5, 6, 12)"):
            plan = adapter["query"](source, "SELECT id, value, label FROM bench" + predicate)
            counts.append(sum(frame.height for frame in plan.collect_batches(**adapter["COLLECT_OPTIONS"])))
        assert counts == [9, 2], counts
        with TestCase().assertRaises(Exception, msg="fresh scan succeeded without the log"):
            adapter["scan"]({"table_uri": table.as_uri(), "snapshot_version": 1}, {}).collect_schema()
    finally:
        (table / "hidden-log").rename(table / "_delta_log")
    run.save(output / "native-reuse.json", {"status": "passed", "rows": counts,
        "evidence": "two newly planned queries after hiding the copied snapshot log; fresh scan fails"})


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: probe.py BINARY OUTPUT")
    native_probes(Path(sys.argv[1]), Path(sys.argv[2]))
