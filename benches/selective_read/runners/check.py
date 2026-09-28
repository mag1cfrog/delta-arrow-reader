"""Exercise both real readers, independent correctness, clocks and failure propagation."""

import argparse
import copy
import json
from pathlib import Path
import shutil
import sys

import run

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import oracle


def check(binaries, fixtures, output):
    output.mkdir()
    cases = ("li.clustered.eq2-in20", "li.shuffled.eq2-in20", "li.clustered.empty",
             "li.clustered.date7-limit", "li.clustered.q6-scan", "wide.clustered.eq2-in20")
    references = {}
    for case in cases:
        directory = output / f"reference-{case}"
        references[case] = (directory, oracle.prepare(fixtures, case, directory))

    invocations = 0
    for binary in binaries:
        reader = json.loads(binary.with_name("build.json").read_text())["reader_id"]
        for case in cases:
            reference, expected = references[case]
            executions = ("open", "reuse") if case.endswith("eq2-in20") else ("open",)
            for execution in executions:
                name = f"{reader}-{case}-{execution}"
                validation_dir = output / (name + "-validation")
                payload = run.request(fixtures, case, execution, "validation", name)
                result = run.invoke(binary, payload, validation_dir, fixtures, reference)
                assert result["status"] == "success", result
                assert result["correctness"]["status"] == "passed", result
                assert all(q["completion_ns"] is None for q in result["queries"])
                if reader == "delta-rs":
                    settings = result["provider_evidence"]["scan_config"]
                    assert settings["enable_parquet_pushdown"] and settings["schema_force_view_types"]
                proof = validation_dir / "correctness.json"
                payload = run.request(fixtures, case, execution, "timing", name, correctness=proof)
                measured_dir = output / (name + "-timing")
                measured = run.invoke(binary, payload, measured_dir)
                assert measured["status"] == "success", measured
                assert measured["correctness"]["status"] == "passed", measured
                assert len(measured["queries"]) == (10 if execution == "reuse" else 1)
                assert measured["cleanup_ns"] >= 0
                assert not list((measured_dir / "reader").glob("*.arrow"))
                for query in measured["queries"]:
                    assert query["output_rows"] == expected["output_rows"]
                    assert query["completion_ns"] > 0
                    if expected["output_rows"]:
                        assert 0 < query["first_batch_ns"] <= query["completion_ns"]
                    else:
                        assert query["first_batch_ns"] is None
                        assert query["first_batch_unavailable_reason"] == "empty result"
                if execution == "reuse":
                    total = measured["initialization_ns"] + sum(q["completion_ns"] for q in measured["queries"])
                    assert measured["initialization_plus_all_queries_ns"] == total
                    assert measured["session_elapsed_ns"] >= total
                    assert measured["initialization_plus_query1_ns"] == measured["initialization_ns"] + measured["queries"][0]["completion_ns"]
                else:
                    assert measured["open_query_ns"] == measured["queries"][0]["completion_ns"]
                invocations += 2

        case = cases[0]
        reference = references[case][0]
        payload = run.request(fixtures, case, "open", "diagnostic", f"{reader}-diagnostic")
        diagnostic_dir = output / f"{reader}-diagnostic"
        diagnostic = run.invoke(binary, payload, diagnostic_dir)
        assert diagnostic["status"] == "success", diagnostic
        assert diagnostic["open_query_ns"] is None
        assert (diagnostic_dir / "reader" / diagnostic["queries"][0]["physical_plan"]).stat().st_size > 0
        invocations += 1

        for mode in ("open", "reuse"):
            observed = run.invoke(binary, run.request(fixtures, case, mode, "io", f"{reader}-io-{mode}"),
                                  output / f"{reader}-io-{mode}")
            assert observed["status"] == "success" and observed["diagnostic_session_ns"] > 0, observed
            assert observed["provider_evidence"] is None and observed["open_query_ns"] is None, observed
            assert all(q["physical_plan"] is None and q["completion_ns"] is None for q in observed["queries"])
            events = observed["diagnostic_events"]
            assert [e["event"] for e in events] == ["snapshot_open"] + ["query_start", "stream_complete"] * (10 if mode == "reuse" else 1) + ["cleanup_complete"]
            assert [e["time_ns"] for e in events] == sorted(e["time_ns"] for e in events)
            assert observed["diagnostic_process"]["exited_ns"] >= events[-1]["time_ns"]
            invocations += 1

        for name, changes in (
            ("wrong-snapshot", {"snapshot_version": 999}),
            ("bad-query", {"canonical_sql": "SELECT missing_column FROM bench"}),
        ):
            broken = dict(payload, **changes)
            failed = run.invoke(binary, broken, output / f"{reader}-{name}")
            assert failed["status"] == "operational_failure" and failed["failure_reason"], failed
            invocations += 1

        table = output / f"{reader}-wrong-schema"
        shutil.copytree(fixtures / "li.clustered", table)
        log = table / "_delta_log/00000000000000000000.json"
        actions = [json.loads(line) for line in log.read_text().splitlines()]
        for action in actions:
            if "metaData" in action:
                schema = json.loads(action["metaData"]["schemaString"])
                next(f for f in schema["fields"] if f["name"] == "l_linenumber")["type"] = "long"
                action["metaData"]["schemaString"] = json.dumps(schema)
        log.write_text("".join(json.dumps(action) + "\n" for action in actions))
        broken = run.request(fixtures, case, "open", "validation", f"{reader}-wrong-schema", table_uri=table.resolve().as_uri())
        failed = run.invoke(binary, broken, output / f"{reader}-schema-result", fixtures, reference)
        assert failed["status"] in ("validation_failed", "operational_failure") and failed["failure_reason"], failed
        invocations += 1

        proof = output / f"{reader}-{case}-open-validation/correctness.json"
        base = run.request(fixtures, case, "open", "timing", f"{reader}-stale", correctness=proof)
        for field, value in (("snapshot_version", 1), ("canonical_sql", "SELECT 1"),
                             ("table_uri", (fixtures / "li.shuffled").resolve().as_uri())):
            broken = copy.deepcopy(base)
            broken[field] = value
            failed = run.invoke(binary, broken, output / f"{reader}-stale-{field}")
            assert failed["status"] == "validation_failed", failed
            assert failed["queries"] == [] and failed["open_query_ns"] is None
            invocations += 1
        wrong_count = json.loads(proof.read_text())
        wrong_count["checks"][0]["output_rows"] += 1
        count_proof = output / f"{reader}-wrong-count-proof.json"
        run.save(count_proof, wrong_count)
        broken = dict(base, correctness_file=str(count_proof.resolve()))
        failed = run.invoke(binary, broken, output / f"{reader}-wrong-timing-count")
        assert failed["status"] == "validation_failed" and failed["queries"], failed
        invocations += 1
    summary = {"status": "passed", "readers": len(binaries), "public_cases": len(cases), "invocations": invocations}
    run.save(output / "checks.json", summary)
    print(json.dumps(summary))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, action="append", required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check(args.binary, args.fixtures, args.output)
