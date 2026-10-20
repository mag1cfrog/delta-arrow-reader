"""Shared bounded Delta snapshot/DV checks, independent of the reader engine."""

import argparse
import json
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq

import check as shared_check
import run

CORPUS = Path(__file__).resolve().parents[3] / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus"


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
    summary = {"status": "passed", "public_contract_invocations": public, "capability_checks": len(observations)}
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


def export_corpus(record, output, deleted, ids):
    original = CORPUS / "deletion_vectors"
    source = pq.ParquetFile(next((original / "table").glob("*.parquet"))).read()
    with pa.ipc.open_file(original / "expected.arrow") as saved:
        live = saved.read_all()
    assert source.num_rows == 12 and live.num_rows == 9
    assert set(source["id"].to_pylist()) - set(live["id"].to_pylist()) == {2, 5, 12}
    expected = live if deleted else source
    if ids is not None:
        expected = pa.Table.from_pylist([row for row in expected.to_pylist() if row["id"] in ids], schema=expected.schema)
    exported(record, output, expected)
    return expected.num_rows


def delta_capabilities(binary, fixtures, output):
    import native
    return native.call("delta-capabilities", {"binary": str(binary), "fixtures": str(fixtures),
        "output": str(output), "python": sys.executable, "harness": str(Path(__file__).resolve().parents[1])})
