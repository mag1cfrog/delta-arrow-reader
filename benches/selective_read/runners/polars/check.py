"""Check Polars' public-case contract, Delta capabilities, expressions and reuse."""

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
                          native_reuse, probe, stale_expression, type_semantics)


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
        with TestCase().assertRaises(Exception, msg="fresh scan succeeded without the log"):
            adapter["scan"]({"table_uri": table.as_uri(), "snapshot_version": 1}, {}).collect_schema()
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
    type_semantics(binary, base, source, output, observations)

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

    native_reuse(__file__, binary, output, observations)

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

    result = stale_expression("polars", binary, fixtures, output)
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

    capability_report("polars", binary, output, observations)


if __name__ == "__main__":
    check_cli(check, __doc__, native_probes)
