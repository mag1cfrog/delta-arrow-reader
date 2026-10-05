"""Check Spark capabilities and revision 6 clocks against the saved Delta corpus."""

import argparse
import json
from pathlib import Path
import resource
import sys

import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import run
from capabilities import CORPUS, delta_capabilities, exported, probe


def revision6(binary, fixtures, output):
    observations = []
    original = CORPUS / "deletion_vectors"
    with pa.ipc.open_file(original / "expected.arrow") as saved:
        expected = saved.read_all()
    base = run.request(fixtures, "li.clustered.eq2-in20", "open", "validation", "probe")
    base.update(comparison_revision=6, protocol_sha256=run.digest(run.SPARK_MATRIX),
                base_protocol_sha256=run.digest(run.PROTOCOL), sampling_sha256=run.digest(run.SAMPLING),
                sampling_stage="pilot", workload_manifest_sha256=run.digest(CORPUS / "manifest.json"),
                fixture_manifest_sha256=run.digest(CORPUS / "manifest.json"), case_id="probe.spark-revision6",
                table_uri=(original / "table").as_uri(), snapshot_version=1,
                canonical_sql="SELECT id, value, label FROM bench")
    for mode in ("open", "reuse"):
        payload = base | {"execution_mode": mode, "run_id": mode + "-validation"}
        destination = output / ("revision6-" + mode)
        record = probe(binary, payload, destination)
        exported(record, destination, expected)
        assert len(record["queries"]) == (1 if mode == "open" else 2)
        # Exact corpus exports authorize only this bounded probe, not public cases.
        certificate = output / (mode + "-bounded-correctness.json")
        run.save(certificate, {"status": "passed", "checks": [record["identity"] | {
            "status": "passed", "oracle_sha256": run.digest(HERE.parents[1] / "oracle.py"),
            "output_rows": expected.num_rows} for query in record["queries"]]})
        timed = payload | {"purpose": "timing", "correctness_file": str(certificate.resolve())}
        result = probe(binary, timed, output / (mode + "-timing"))
        assert result["status"] == "success" and result["correctness"]["status"] == "passed", result
        assert all(q["completion_ns"] > 0 and q["first_batch_ns"] is None for q in result["queries"])
        if mode == "reuse":
            assert result["initialization_plus_all_queries_ns"] == result["initialization_ns"] + sum(q["completion_ns"] for q in result["queries"])
        else:
            assert result["open_query_ns"] == result["queries"][0]["completion_ns"]
        assert result["cleanup_ns"] >= 0 and result["startup_ns"] > 0 and result["session_elapsed_ns"] > 0
        for name, change in (("missing-proof", {"correctness_file": None}),
                             ("changed-snapshot", {"snapshot_version": 0}),
                             ("changed-sql", {"canonical_sql": "SELECT id FROM bench"})):
            bad = probe(binary, timed | change, output / (mode + "-" + name))
            assert bad["status"] == "validation_failed" and not bad["queries"], bad
        observations.extend({"path": name, "status": "passed", "feature": "revision 6 exact values, clocks and proof identity"}
                            for name in (destination.name, mode + "-timing", mode + "-missing-proof",
                                         mode + "-changed-snapshot", mode + "-changed-sql"))
    return observations


def check(binary, fixtures, output):
    output.mkdir()
    # Reproduce the production validation cap: the AWS JAR exceeds this size.
    soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    resource.setrlimit(resource.RLIMIT_FSIZE, (min(256 * 1024**2, soft) if soft != resource.RLIM_INFINITY else 256 * 1024**2, hard))
    checks = delta_capabilities(binary, fixtures, output)
    base = run.request(fixtures, "li.clustered.eq2-in20", "open", "diagnostic", "invalid")
    for name, changes in (("unknown-field", {"surprise": True}),
                          ("negative-version", {"snapshot_version": -1}),
                          ("bad-budget", {"resource_budget": {}}),
                          ("credential-uri", {"table_uri": "s3://user:secret@bucket/table"}),
                          ("timing-rejected", {"purpose": "timing"}),
                          ("campaign-rejected", {"campaign_id": "formal"}),
                          ("sql-rejected", {"canonical_sql": "SELECT * FROM bench; DROP TABLE bench"})):
        record = probe(binary, dict(base, **changes), output / name)
        assert record["status"] == "invalid_input" and record["failure_reason"], record
        checks.append({"path": name, "status": "passed", "feature": "invalid/formal request rejected"})
    checks += revision6(binary, fixtures, output)
    summary = {"status": "passed", "reader": "spark", "pilot_only": True, "publication_ready": False,
               "build_sha256": run.digest(binary.with_name("build.json")),
               "corpus_manifest_sha256": run.digest(CORPUS / "manifest.json"),
               "scope": "bounded exact-value capability checks; not a performance campaign",
               "max_file_size_bytes": resource.getrlimit(resource.RLIMIT_FSIZE)[0],
               "observations": checks}
    run.save(output / "capabilities.json", summary)
    print(json.dumps({"status": "passed", "invocations": len(checks), "pilot_only": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check(args.binary, args.fixtures, args.output)
