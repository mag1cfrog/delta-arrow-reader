"""Check Daft's public cases, native streaming/reuse, and Delta failure modes."""

from contextlib import closing
import json
import os
from pathlib import Path
import runpy
import shutil
import sys
from unittest import TestCase

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import check as shared_check
import run
from capabilities import (CORPUS, capability_report, check_cli, delta_capabilities,
                          exported, native_reuse, probe, stale_expression, type_semantics)


def native_probes(binary, output):
    adapter = runpy.run_path(str(binary), run_name="capability_probe")
    adapter["engine"]()
    table = output / "versions"
    config, _ = adapter["storage"](table.as_uri())
    source = adapter["scan"]({"table_uri": table.as_uri(), "snapshot_version": 1}, config)
    (table / "_delta_log").rename(table / "hidden-log")
    counts = []
    try:
        for predicate in ("", " WHERE id IN (1, 2, 5, 6, 12)"):
            plan = adapter["query"](source, "SELECT id, value, label FROM bench" + predicate)
            with closing(plan.to_arrow_iter(results_buffer_size=8)) as stream:
                counts.append(sum(batch.num_rows for batch in stream))
            assert source._result is None and plan._result is None, "native result was cached"
        assert counts == [9, 2], counts
        with TestCase().assertRaises(Exception, msg="fresh scan succeeded without the log"):
            adapter["scan"]({"table_uri": table.as_uri(), "snapshot_version": 1}, config)
    finally:
        (table / "hidden-log").rename(table / "_delta_log")
    run.save(output / "native-reuse.json", {"status": "passed", "rows": counts,
        "evidence": "two newly planned queries after hiding the copied snapshot log; no result cache; fresh scan fails"})


def check(binary, fixtures, output):
    output.mkdir()
    shared_check.check([binary], fixtures, output / "public-cases")
    observations = delta_capabilities(binary, fixtures, output, dv_status="unsupported")
    base = run.request(fixtures, "li.clustered.eq2-in20", "open", "validation", "probe")
    table = output / "no-dv"
    original_file = next(table.glob("*.parquet"))
    source = pq.ParquetFile(original_file).read()
    with pa.ipc.open_file(CORPUS / "deletion_vectors/expected.arrow") as saved:
        deleted = saved.read_all()
    base.update(table_uri=table.resolve().as_uri(), fixture_manifest_sha256=run.digest(CORPUS / "manifest.json"),
                case_id="probe.daft")
    type_semantics(binary, base, source, output, observations)

    # A second no-DV snapshot checks time travel independently of DV support.
    versions = output / "versions"
    shutil.copytree(table, versions)
    replacement = versions / "replacement.parquet"
    pq.write_table(deleted, replacement)
    actions = [{"remove": {"path": original_file.name, "deletionTimestamp": 0, "dataChange": True}},
               {"add": {"path": replacement.name, "size": replacement.stat().st_size, "partitionValues": {},
                        "modificationTime": 0, "dataChange": True}}]
    (versions / "_delta_log/00000000000000000001.json").write_text("".join(json.dumps(a) + "\n" for a in actions))
    for mode in ("open", "reuse"):
        for version, expected in ((0, source), (1, deleted)):
            destination = output / f"version-{version}-{mode}"
            result = probe(binary, dict(base, table_uri=versions.resolve().as_uri(), snapshot_version=version,
                           canonical_sql="SELECT id, value, label FROM bench", execution_mode=mode), destination)
            exported(result, destination, expected)
            observations.append({"path": destination.name, "status": "passed", "feature": "explicit snapshot",
                                 "version": version, "output_rows": expected.num_rows})

    # Disabling future DV writes does not remove existing active descriptors.
    # Preserve the native wrong result and the independent validation failure.
    disabled = output / "dv-property-disabled"
    shutil.copytree(CORPUS / "deletion_vectors/table", disabled)
    actions = [json.loads(line) for line in (disabled / "_delta_log/00000000000000000000.json").read_text().splitlines()]
    metadata = next(a for a in actions if "metaData" in a)
    metadata["metaData"]["configuration"]["delta.enableDeletionVectors"] = "false"
    (disabled / "_delta_log/00000000000000000002.json").write_text(json.dumps(metadata) + "\n")
    for mode in ("open", "reuse"):
        for suffix, ids in (("all", set(range(1, 13))), ("deleted-only", {2, 5, 12}), ("mixed", {1, 2, 5, 6, 12})):
            predicate = "" if suffix == "all" else " WHERE id IN (" + ", ".join(map(str, sorted(ids))) + ")"
            expected = pa.Table.from_pylist([r for r in deleted.to_pylist() if r["id"] in ids], schema=deleted.schema)
            destination = output / f"disabled-{mode}-{suffix}"
            payload = dict(base, table_uri=disabled.resolve().as_uri(), snapshot_version=2, execution_mode=mode,
                           canonical_sql="SELECT id, value, label FROM bench" + predicate)
            record = probe(binary, payload, destination)
            assert record["status"] == "success", record
            assert all(q["output_rows"] == len(ids) for q in record["queries"]), record
            try:
                exported(record, destination, expected)
            except AssertionError:
                reason = "active deletion vectors were ignored after disabling future DV writes"
            else:
                raise AssertionError("pinned Daft behavior changed: update the capability evidence")
            proof = {"status": "validation_failed", "checks": [], "failure_reason": reason}
            run.save(destination / "correctness.json", proof)
            record.update(status="validation_failed", failure_reason=reason, correctness=proof)
            run.save(destination / "observation.json", record)
            observations.append({"path": destination.name, "status": "validation_failed", "feature": "active DV with property disabled",
                                 "expected_rows": expected.num_rows, "actual_rows": record["queries"][0]["output_rows"], "reason": reason})
    result = probe(binary, dict(payload, purpose="timing", correctness_file=str((destination / "correctness.json").resolve())),
                   output / "failed-dv-timing")
    assert result["status"] == "validation_failed" and not result["queries"] and result["phase"] == "correctness_gate", result
    observations.append({"path": "failed-dv-timing", "status": "passed", "feature": "incorrect DV output cannot authorize timing"})

    # One file with many row groups places a native parse error beyond initial
    # buffering. Exercise the actual adapter stream, without a test-only query path.
    stream_table = output / "stream-table"
    (stream_table / "_delta_log").mkdir(parents=True)
    actions = [json.loads(line) for line in (table / "_delta_log/00000000000000000000.json").read_text().splitlines()]
    actions = [a for a in actions if "protocol" in a or "metaData" in a]
    path = stream_table / "stream.parquet"
    with pq.ParquetWriter(path, source.schema) as writer:
        for index in range(128):
            writer.write_table(pa.Table.from_pydict({"id": range(8192), "value": [1] * 8192,
                "label": ["2000-01-01" if index < 127 else "bad"] * 8192}, schema=source.schema), row_group_size=8192)
    actions.append({"add": {"path": path.name, "size": path.stat().st_size, "partitionValues": {},
                            "modificationTime": 0, "dataChange": True}})
    (stream_table / "_delta_log/00000000000000000000.json").write_text("".join(json.dumps(a) + "\n" for a in actions))
    result = probe(binary, dict(base, table_uri=stream_table.resolve().as_uri(), purpose="diagnostic",
                   canonical_sql="SELECT id FROM bench WHERE to_date(label, '%Y-%m-%d') IS NOT NULL"), output / "late-stream-error")
    assert result["status"] == "operational_failure" and "failed to parse date" in result["failure_reason"], result
    assert 0 < result["partial_query"]["output_rows"] < 128 * 8192, result
    observations.append({"path": "late-stream-error", "status": "passed", "feature": "streaming",
                         "rows_before_error": result["partial_query"]["output_rows"]})

    native_reuse(__file__, binary, output, observations)

    missing = output / "missing-parquet-table"
    shutil.copytree(table, missing)
    next(missing.glob("*.parquet")).unlink()
    result = probe(binary, dict(base, table_uri=missing.resolve().as_uri(), canonical_sql="SELECT * FROM bench"), output / "missing-parquet")
    assert result["status"] == "operational_failure" and result["failure_reason"], result
    observations.append({"path": "missing-parquet", "status": result["status"], "feature": "missing data object"})

    payload = run.request(fixtures, "li.clustered.eq2-in20", "open", "diagnostic", "failure-probe")
    secrets = {"AWS_ACCESS_KEY_ID": "probe-access", "AWS_SECRET_ACCESS_KEY": "probe-secret",
               "AWS_SESSION_TOKEN": "probe-token", "AWS_ENDPOINT_URL": "http://127.0.0.1:9"}
    result = probe(binary, dict(payload, table_uri="s3://probe-bucket/table", purpose="timing"), output / "s3-configuration",
                   env=dict(os.environ, **secrets, DAFT_RUNNER="ray", DAFT_ANALYTICS_ENABLED="1", DAFT_DASHBOARD_URL="invalid"))
    assert result["status"] == "validation_failed" and result["phase"] == "correctness_gate", result
    assert result["settings"]["daft"]["compute_workers"] == 8 and result["settings"]["storage"]["url_style"] == "path", result
    assert not any(secrets[k] in json.dumps(result) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"))
    observations.append({"path": "s3-configuration", "status": "passed", "feature": "storage setup and frozen environment"})

    result = stale_expression("daft", binary, fixtures, output)
    assert result["status"] == "validation_failed" and not result["queries"], result
    observations.append({"path": "expression-rejected", "status": "passed", "feature": "native expression certificate binding"})

    broken = output / "missing-dependency"
    broken.write_text(binary.read_text().splitlines()[0] + "\nimport runpy, sys\nsys.modules['daft'] = None\n"
                      + f"runpy.run_path({str(binary.resolve())!r}, run_name='__main__')\n")
    broken.chmod(0o755)
    result = run.invoke(broken, payload, output / "dependency-failure")
    assert result["status"] == "operational_failure" and "ModuleNotFoundError" in result["failure_reason"], result
    observations.append({"path": "dependency-failure", "status": result["status"], "feature": "missing dependency"})

    capability_report("daft", binary, output, observations,
                      scope="bounded fixtures; includes expected unsupported and incorrect results; rerun exact campaign fixtures")


if __name__ == "__main__":
    check_cli(check, __doc__, native_probes)
