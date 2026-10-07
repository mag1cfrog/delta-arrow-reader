"""Shared bounded Delta snapshot/DV checks, independent of the reader engine."""

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq

import check as shared_check
import run

CORPUS = Path(__file__).resolve().parents[3] / "tests/reader/fixtures/external_writer/corpus"


def check_cli(check, description, native_probes=None):
    if native_probes is not None and len(sys.argv) == 4 and sys.argv[1] == "--native-probes":
        native_probes(Path(sys.argv[2]), Path(sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=description)
    for name in ("binary", "fixtures", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    check(args.binary, args.fixtures, args.output)


def capability_report(reader, binary, output, observations, scope="bounded fixtures; rerun against exact campaign fixtures"):
    public = json.loads((output / "public-cases/checks.json").read_text())["invocations"]
    summary = {"status": "passed", "public_contract_invocations": public}
    summary.update({"capability_invocations": len(observations), "total_invocations": public + len(observations)}
                   if reader == "duckdb" else {"capability_checks": len(observations)})
    run.save(output / "capabilities.json", {**summary, "reader": reader,
        "build_sha256": run.digest(binary.with_name("build.json")), "corpus_manifest_sha256": run.digest(CORPUS / "manifest.json"),
        "scope": scope, "observations": observations})
    print(json.dumps(summary))


def type_semantics(binary, payload, source, output, observations):
    for sql, expected in (
        ("SELECT label, id, value FROM bench WHERE value IS NULL OR label IS NULL",
         pa.Table.from_pylist([r for r in source.to_pylist() if r["value"] is None or r["label"] is None],
                             schema=source.schema).select(["label", "id", "value"])),
        ("SELECT id, value, label FROM bench WHERE id IN (2, 2, NULL)", source.slice(1, 1)),
    ):
        destination = output / f"types-{len(observations)}"
        result = probe(binary, dict(payload, canonical_sql=sql), destination)
        exported(result, destination, expected)
        assert result["identity"]["native_expression_sha256"], result
        observations.append({"path": destination.name, "status": "passed", "feature": "null/IN/projection semantics"})


def stale_expression(reader, binary, fixtures, output):
    proof = output / f"public-cases/{reader}-li.clustered.eq2-in20-open-validation/correctness.json"
    changed = json.loads(proof.read_text())
    changed["checks"][0]["native_expression_sha256"] = "0" * 64
    bad_proof = output / "stale-expression.json"
    run.save(bad_proof, changed)
    return run.invoke(binary, run.request(fixtures, "li.clustered.eq2-in20", "open", "timing", "stale-expression", correctness=bad_proof),
                      output / "expression-rejected")


def native_reuse(script, binary, output, observations):
    command = [str(binary.with_name("venv") / "bin/python"), "-I", "-B", str(Path(script).resolve()),
               "--native-probes", str(binary.resolve()), str(output.resolve())]
    with (output / "native-probes.log").open("x") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    observations.append({"path": "native-reuse.json", "status": "passed", "feature": "native snapshot reuse"})


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
        assert [(f.name, shared_check.oracle.logical_type(f.type)) for f in actual.schema] == [
            (f.name, shared_check.oracle.logical_type(f.type)) for f in expected.schema]
        assert actual.sort_by("id").to_pylist() == expected.sort_by("id").to_pylist()
        assert query["output_rows"] == expected.num_rows and query["completion_ns"] is None


def delta_capabilities(binary, fixtures, output, *, dv_status="success"):
    corpus = CORPUS
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
                expected_status = "success" if name == "no-dv" else dv_status
                assert record["status"] == expected_status, record
                if expected_status == "success":
                    exported(record, destination, selected)
                    assert len(record["queries"]) == (10 if mode == "reuse" else 1)
                else:
                    assert record["failure_reason"] and not record["queries"], record
                observations.append({"path": destination.name, "status": "passed" if expected_status == "success" else expected_status,
                                     "mode": mode, "feature": name, "version": version, "predicate": predicate,
                                     "output_rows": selected.num_rows if expected_status == "success" else None,
                                     "reason": record["failure_reason"]})

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
                ("unsupported", unknown, 0, "unsupported"),
                ("missing-dv", missing, 1, "unsupported" if dv_status == "unsupported" else "operational_failure")):
            destination = output / f"{mode}-{name}"
            payload = dict(base, table_uri=table.resolve().as_uri(), snapshot_version=version,
                           canonical_sql="SELECT * FROM bench", execution_mode=mode, run_id=destination.name)
            record = probe(binary, payload, destination)
            assert record["status"] == status and record["failure_reason"], record
            if mode == "reuse" and name == "wrong-version":
                assert record["phase"] == "snapshot_open" and not record["queries"]
            observations.append({"path": destination.name, "status": status, "reason": record["failure_reason"]})

    return observations
