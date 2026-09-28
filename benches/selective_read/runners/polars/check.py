"""Check Polars' public-case contract, Delta capabilities, expressions and reuse."""

import argparse
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import check as shared_check
import run
from capabilities import CORPUS, delta_capabilities, exported, probe


def native_probes(binary, output):
    adapter = runpy.run_path(str(binary), run_name="capability_probe")
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
        try:
            adapter["scan"]({"table_uri": table.as_uri(), "snapshot_version": 1}, {}).collect_schema()
        except Exception:
            pass
        else:
            raise AssertionError("fresh scan succeeded without the log")
    finally:
        (table / "hidden-log").rename(table / "_delta_log")
    run.save(output / "native-reuse.json", {"status": "passed", "rows": counts,
        "evidence": "two newly planned queries after hiding the copied snapshot log; fresh scan fails"})


def check(binary, fixtures, output):
    output.mkdir()
    shared_check.check([binary], fixtures, output / "public-cases")
    observations = delta_capabilities(binary, fixtures, output)
    base = run.request(fixtures, "li.clustered.eq2-in20", "open", "validation", "probe")
    table = output / "no-dv"
    source = pq.ParquetFile(next(table.glob("*.parquet"))).read()
    base.update(table_uri=table.resolve().as_uri(), fixture_manifest_sha256=run.digest(CORPUS / "manifest.json"),
                case_id="probe.polars-types")
    for sql, expected in (
        ("SELECT label, id, value FROM bench WHERE value IS NULL OR label IS NULL",
         pa.Table.from_pylist([r for r in source.to_pylist() if r["value"] is None or r["label"] is None],
                             schema=source.schema).select(["label", "id", "value"])),
        ("SELECT id, value, label FROM bench WHERE id IN (2, 2, NULL)", source.slice(1, 1)),
    ):
        destination = output / f"types-{len(observations)}"
        result = probe(binary, dict(base, canonical_sql=sql), destination)
        exported(result, destination, expected)
        assert result["identity"]["native_expression_sha256"], result
        observations.append({"path": destination.name, "status": "passed", "feature": "null/IN/projection semantics"})

    # Use the adapter's actual Delta -> native predicate -> collect_batches path.
    # The final file's invalid cast must fail after earlier batches were delivered.
    stream_table = output / "stream-table"
    (stream_table / "_delta_log").mkdir(parents=True)
    actions = [json.loads(line) for line in (table / "_delta_log/00000000000000000000.json").read_text().splitlines()]
    actions = [a for a in actions if "protocol" in a or "metaData" in a]
    for index in range(32):
        path = stream_table / f"{index:03d}.parquet"
        data = pa.Table.from_pydict({"id": range(8192), "value": [1] * 8192,
                                    "label": ["1" if index < 31 else "bad"] * 8192}, schema=source.schema)
        pq.write_table(data, path)
        actions.append({"add": {"path": path.name, "size": path.stat().st_size, "partitionValues": {},
                                "modificationTime": 0, "dataChange": True}})
    (stream_table / "_delta_log/00000000000000000000.json").write_text("".join(json.dumps(a) + "\n" for a in actions))
    result = probe(binary, dict(base, table_uri=stream_table.resolve().as_uri(), purpose="diagnostic",
                   canonical_sql="SELECT id FROM bench WHERE CAST(label AS BIGINT) > 0"), output / "late-stream-error")
    assert result["status"] == "operational_failure" and "conversion" in result["failure_reason"], result
    assert 0 < result["partial_query"]["output_rows"] < 32 * 8192, result
    observations.append({"path": "late-stream-error", "status": "passed", "feature": "streaming",
                         "rows_before_error": result["partial_query"]["output_rows"]})

    command = [str(binary.with_name("venv") / "bin/python"), "-I", "-B", str(Path(__file__).resolve()),
               "--native-probes", str(binary.resolve()), str(output.resolve())]
    with (output / "native-probes.log").open("x") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    observations.append({"path": "native-reuse.json", "status": "passed", "feature": "native snapshot reuse"})

    payload = run.request(fixtures, "li.clustered.eq2-in20", "open", "diagnostic", "failure-probe")
    for name, changes in (("unknown-field", {"surprise": True}), ("negative-version", {"snapshot_version": -1}),
                          ("bad-budget", {"resource_budget": {}}), ("credential-url", {"table_uri": "s3://user:secret@bucket/table"})):
        result = run.invoke(binary, dict(payload, **changes), output / name)
        assert result["status"] == "invalid_input", result
        observations.append({"path": name, "status": "passed", "feature": "invalid request rejected"})
    for sql in ("SELECT id FROM bench; SELECT id FROM bench", "DELETE FROM bench", "SELECT id FROM other"):
        destination = output / f"sql-rejected-{len(observations)}"
        result = probe(binary, dict(payload, canonical_sql=sql), destination)
        assert result["status"] == "operational_failure" and not result["queries"], result
        observations.append({"path": destination.name, "status": "passed", "feature": "unsupported query rejected"})

    secrets = {"AWS_ACCESS_KEY_ID": "probe-access", "AWS_SECRET_ACCESS_KEY": "probe-secret",
               "AWS_SESSION_TOKEN": "probe-token", "AWS_ENDPOINT_URL": "http://127.0.0.1:9"}
    result = probe(binary, dict(payload, table_uri="s3://probe-bucket/table", purpose="timing"),
                   output / "s3-configuration", env=dict(os.environ, **secrets, POLARS_MAX_THREADS="1", POLARS_VERBOSE="1"))
    assert result["status"] == "validation_failed" and result["phase"] == "correctness_gate", result
    assert result["settings"]["thread_pool_size"] == 8 and result["settings"]["storage"]["url_style"] == "path", result
    assert not any(secrets[k] in json.dumps(result) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"))
    observations.append({"path": "s3-configuration", "status": "passed", "feature": "storage setup and frozen environment"})

    proof = output / "public-cases/polars-li.clustered.eq2-in20-open-validation/correctness.json"
    changed = json.loads(proof.read_text())
    changed["checks"][0]["native_expression_sha256"] = "0" * 64
    bad_proof = output / "stale-expression.json"
    run.save(bad_proof, changed)
    result = run.invoke(binary, run.request(fixtures, "li.clustered.eq2-in20", "open", "timing", "stale-expression", correctness=bad_proof),
                        output / "expression-rejected")
    assert result["status"] == "validation_failed" and result["queries"] == [], result
    observations.append({"path": "expression-rejected", "status": "passed", "feature": "native expression certificate binding"})

    # Corrupt a saved export while preserving its plausible row count and identity.
    saved = output / "public-cases/polars-li.clustered.eq2-in20-open-validation/reader"
    with pa.ipc.open_stream(saved / "query-0.arrow") as stream:
        values = stream.read_all()
    rows = values.to_pylist()
    rows[0]["l_orderkey"] += 1
    corrupt = output / "wrong-values.arrow"
    with corrupt.open("xb") as sink, pa.ipc.new_stream(sink, values.schema) as writer:
        writer.write_table(pa.Table.from_pylist(rows, schema=values.schema))
    identity = json.loads((saved / "query-0.identity.json").read_text())
    identity["result_sha256"] = run.digest(corrupt)
    run.save(output / "wrong-values.identity.json", identity)
    try:
        shared_check.oracle.check(output / "public-cases/reference-li.clustered.eq2-in20", fixtures,
                                  corrupt, output / "wrong-values.identity.json")
    except ValueError as error:
        assert "reference" in str(error) or "row" in str(error), error
    else:
        raise AssertionError("oracle accepted incorrect values")
    observations.append({"path": "wrong-values.arrow", "status": "passed", "feature": "wrong values rejected"})

    public = json.loads((output / "public-cases/checks.json").read_text())["invocations"]
    summary = {"status": "passed", "public_contract_invocations": public, "capability_checks": len(observations)}
    run.save(output / "capabilities.json", {**summary, "reader": "polars",
        "build_sha256": run.digest(binary.with_name("build.json")), "corpus_manifest_sha256": run.digest(CORPUS / "manifest.json"),
        "scope": "bounded fixtures; rerun against exact campaign fixtures", "observations": observations})
    print(json.dumps(summary))


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--native-probes":
        native_probes(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--binary", type=Path, required=True)
        parser.add_argument("--fixtures", type=Path, required=True)
        parser.add_argument("--output", type=Path, required=True)
        args = parser.parse_args()
        check(args.binary, args.fixtures, args.output)
