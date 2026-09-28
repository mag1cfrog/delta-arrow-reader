"""Check DuckDB's public-case contract and bounded Delta/streaming capabilities."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(HERE.parent))
import check as shared_check
import run


def probe(binary, payload, output, env=None):
    """Export a bounded capability fixture; it is not a public-case certificate."""
    output.mkdir()
    run.save(output / "request.json", payload)
    with (output / "stdout.jsonl").open("x") as stdout, (output / "stderr.log").open("x") as stderr:
        process = subprocess.run([str(binary.resolve()), str((output / "request.json").resolve()),
                                  str((output / "reader").resolve())], stdout=stdout, stderr=stderr, env=env)
    record_path = output / "reader/record.json"
    record = json.loads(record_path.read_text()) if record_path.exists() else {
        "status": "invalid_input", "failure_reason": (output / "stderr.log").read_text()}
    assert (process.returncode == 0) == (record["status"] == "success"), record
    return record


def exported(record, output, expected):
    assert record["status"] == "success", record
    for query in record["queries"]:
        with pa.ipc.open_stream(output / "reader" / query["result"]) as stream:
            batches = list(stream)  # These fixtures have at most twelve rows.
            assert all(b.num_rows <= 8192 for b in batches)
            actual = pa.Table.from_batches(batches, schema=stream.schema)
        assert [(f.name, f.type) for f in actual.schema] == [(f.name, f.type) for f in expected.schema]
        assert actual.sort_by("id").to_pylist() == expected.sort_by("id").to_pylist()
        assert query["output_rows"] == expected.num_rows and query["completion_ns"] is None


def check(binary, fixtures, output):
    output.mkdir()
    shared_check.check([binary], fixtures, output / "public-cases")
    corpus = ROOT / "tests/reader/fixtures/external_writer/corpus"
    manifest = json.loads((corpus / "manifest.json").read_text())
    fixture = next(f for f in manifest["fixtures"] if f["name"] == "deletion_vectors")
    original = corpus / "deletion_vectors"
    for name, expected in fixture["files"].items():
        path = original / name
        assert path.stat().st_size == expected["bytes"] and run.digest(path) == expected["sha256"]
    source = pq.ParquetFile(next((original / "table").glob("*.parquet"))).read()
    with pa.ipc.open_file(original / "expected.arrow") as saved:
        deleted = saved.read_all()
    assert source.num_rows == 12 and deleted.num_rows == 9
    assert set(source["id"].to_pylist()) - set(deleted["id"].to_pylist()) == {2, 5, 12}

    no_dv = output / "no-dv"
    shutil.copytree(original / "table", no_dv)
    (no_dv / "_delta_log/00000000000000000001.json").unlink()
    log = no_dv / "_delta_log/00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "protocol" in action:
            action["protocol"] = {"minReaderVersion": 1, "minWriterVersion": 2}
        if "metaData" in action:
            action["metaData"]["configuration"].pop("delta.enableDeletionVectors", None)
    log.write_text("".join(json.dumps(a) + "\n" for a in actions))

    observations = []
    base = run.request(fixtures, "li.clustered.eq2-in20", "open", "validation", "probe")
    # The corpus hash identifies these bounded probes. They cannot authorize public timing.
    base.update(fixture_manifest_sha256=run.digest(corpus / "manifest.json"), case_id="probe.spark-dv")
    for mode in ("open", "reuse"):
        for name, table, version, expected in (("no-dv", no_dv, 0, source),
                ("feature-only", original / "table", 0, source), ("real-dv", original / "table", 1, deleted)):
            for predicate in ("", " WHERE id IN (2, 5, 12)", " WHERE id IN (1, 2, 5, 6, 12)"):
                if name == "no-dv" and predicate:
                    continue
                selected = expected
                if predicate:
                    ids = {2, 5, 12} if predicate == " WHERE id IN (2, 5, 12)" else {1, 2, 5, 6, 12}
                    selected = pa.Table.from_pylist([r for r in expected.to_pylist() if r["id"] in ids], schema=expected.schema)
                destination = output / f"{mode}-{name}-{len(observations)}"
                payload = dict(base, table_uri=table.resolve().as_uri(), snapshot_version=version,
                               canonical_sql="SELECT id, value, label FROM bench" + predicate,
                               execution_mode=mode, run_id=destination.name)
                record = probe(binary, payload, destination)
                exported(record, destination, selected)
                assert len(record["queries"]) == (10 if mode == "reuse" else 1)
                assert record["settings"]["provider"]["pin_snapshot"] == (mode == "reuse")
                observations.append({"path": destination.name, "status": "passed", "mode": mode, "feature": name,
                                     "version": version, "predicate": predicate, "output_rows": selected.num_rows})

    unknown = output / "unsupported-feature"
    shutil.copytree(original / "table", unknown)
    unknown_log = unknown / "_delta_log/00000000000000000000.json"
    actions = [json.loads(line) for line in unknown_log.read_text().splitlines()]
    for action in actions:
        if "protocol" in action:
            for key in ("readerFeatures", "writerFeatures"):
                action["protocol"][key].append("unknownBenchmarkProbe")
    unknown_log.write_text("".join(json.dumps(a) + "\n" for a in actions))
    missing = output / "missing-dv"
    shutil.copytree(original / "table", missing)
    next(missing.glob("deletion_vector_*.bin")).unlink()
    for mode in ("open", "reuse"):
        for name, table, version, status in (("wrong-version", original / "table", 999, "operational_failure"),
                ("unsupported", unknown, 0, "unsupported"), ("missing-dv", missing, 1, "operational_failure")):
            destination = output / f"{mode}-{name}"
            payload = dict(base, table_uri=table.resolve().as_uri(), snapshot_version=version,
                           canonical_sql="SELECT * FROM bench", execution_mode=mode, run_id=destination.name)
            record = probe(binary, payload, destination)
            assert record["status"] == status and record["failure_reason"], record
            if mode == "reuse" and name == "wrong-version":
                assert record["phase"] == "snapshot_open" and not record["queries"]
            observations.append({"path": destination.name, "status": status, "reason": record["failure_reason"]})

    # An error after delivered rows distinguishes streaming from a collected result
    # sliced into batches. Use more rows than DuckDB's native stream buffer holds.
    payload = run.request(fixtures, "li.clustered.eq2-in20", "open", "diagnostic", "streaming")
    payload["canonical_sql"] = "SELECT CASE WHEN i=999999 THEN error('late-stream-probe') ELSE i END AS id FROM range(1000000) t(i)"
    result = probe(binary, payload, output / "late-stream-error")
    assert result["status"] == "operational_failure" and "late-stream-probe" in result["failure_reason"], result
    assert 0 < result["partial_query"]["output_rows"] < 1000000, result
    observations.append({"path": "late-stream-error", "status": "passed", "feature": "streaming",
                         "rows_before_error": result["partial_query"]["output_rows"]})

    for name, changes in (("unknown-field", {"surprise": True}), ("negative-version", {"snapshot_version": -1}),
                          ("bad-budget", {"resource_budget": {}}), ("credential-url", {"table_uri": "s3://user:secret@bucket/table"})):
        invalid = dict(payload, **changes)
        record = probe(binary, invalid, output / name)
        assert record["status"] == "invalid_input", record
        observations.append({"path": name, "status": "passed", "feature": "invalid request rejected"})
    for sql in ("SELECT 1; SELECT 2", "CREATE TABLE bad AS SELECT 1", "INSTALL delta"):
        destination = output / f"sql-rejected-{len(observations)}"
        record = probe(binary, dict(payload, canonical_sql=sql), destination)
        assert record["status"] == "operational_failure" and record["phase"] == "setup", record
        observations.append({"path": destination.name, "status": "passed", "feature": "read-only SQL", "sql": sql})

    # Secret setup must succeed without table I/O or recording credential values.
    # The missing certificate stops execution before the intentionally unreachable S3 URL.
    secret_values = {"AWS_ACCESS_KEY_ID": "probe-access", "AWS_SECRET_ACCESS_KEY": "probe-secret",
                     "AWS_SESSION_TOKEN": "probe-token", "AWS_REGION": "us-east-1",
                     "AWS_ENDPOINT_URL": "http://127.0.0.1:9"}
    remote = dict(payload, table_uri="s3://probe-bucket/table", purpose="timing", correctness_file=None)
    result = probe(binary, remote, output / "s3-secret-setup", env=dict(os.environ, **secret_values))
    assert result["status"] == "validation_failed" and result["phase"] == "correctness_gate", result
    assert result["queries"] == [] and result["settings"]["storage"]["url_style"] == "path", result
    assert not any(secret_values[k] in json.dumps(result) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"))
    observations.append({"path": "s3-secret-setup", "status": "passed", "feature": "S3 configuration before table I/O"})

    changed = output / "changed-extension"
    changed.mkdir()
    for name in (binary.name, "run.py", "oracle.py", "protocol.md", "lock.json", "build.json"):
        shutil.copy2(binary.with_name(name), changed / name)
    (changed / "httpfs.duckdb_extension").write_bytes(b"not the locked extension")
    result = probe(changed / binary.name, payload, output / "extension-rejected")
    assert result["status"] == "operational_failure" and result["phase"] == "setup", result
    assert "extension checksum mismatch" in result["failure_reason"] and not result["queries"], result
    observations.append({"path": "extension-rejected", "status": "passed", "feature": "extension hash verification"})

    broken = output / "missing-dependency"
    broken.write_text(f"#!{sys.executable}\nimport __selective_read_missing_dependency__\n")
    broken.chmod(0o755)
    result = run.invoke(broken, payload, output / "import-failure")
    assert result["status"] == "operational_failure" and "ModuleNotFoundError" in result["failure_reason"], result
    observations.append({"path": "import-failure", "status": "passed", "feature": "startup failure classification"})
    result = run.invoke(binary, dict(payload, surprise=True), output / "launcher-invalid-input")
    assert result["status"] == "invalid_input", result
    observations.append({"path": "launcher-invalid-input", "status": "passed", "feature": "input rejection classification"})

    # The independent oracle must reject plausible counts with wrong projected values.
    reference = output / "public-cases/reference-li.clustered.eq2-in20"
    invalid = run.request(fixtures, "li.clustered.eq2-in20", "open", "validation", "wrong-values")
    invalid["canonical_sql"] = invalid["canonical_sql"].replace("SELECT l_orderkey,", "SELECT l_orderkey + 1 AS l_orderkey,", 1)
    assert "l_orderkey + 1" in invalid["canonical_sql"]
    # Supply the reference SQL identity only in this deliberately corrupted export.
    result_dir = output / "wrong-values"
    record = probe(binary, invalid, result_dir)
    assert record["status"] == "success", record
    identity_path = result_dir / "reader/query-0.identity.json"
    identity = json.loads(identity_path.read_text())
    identity["canonical_sql_sha256"] = json.loads((reference / "reference.json").read_text())["canonical_sql_sha256"]
    identity_path.write_text(json.dumps(identity))
    try:
        shared_check.oracle.check(reference, fixtures, result_dir / "reader/query-0.arrow", identity_path)
    except ValueError as error:
        assert "reference" in str(error) or "row" in str(error), error
        observations.append({"path": "wrong-values", "status": "passed", "feature": "wrong values rejected", "reason": str(error)})
    else:
        raise AssertionError("oracle accepted incorrect values")

    public = json.loads((output / "public-cases/checks.json").read_text())["invocations"]
    summary = {"status": "passed", "public_contract_invocations": public,
               "capability_invocations": len(observations), "total_invocations": public + len(observations)}
    run.save(output / "capabilities.json", {**summary, "reader": "duckdb",
        "build_sha256": run.digest(binary.with_name("build.json")), "corpus_manifest_sha256": run.digest(corpus / "manifest.json"),
        "scope": "bounded fixtures; rerun against exact campaign fixtures", "observations": observations})
    print(json.dumps(summary))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check(args.binary, args.fixtures, args.output)
