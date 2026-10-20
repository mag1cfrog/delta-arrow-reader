"""Check the real MinIO boundary, prepared remote readers, and tracing overhead.

Run explicitly on a dedicated server. This is not a performance campaign or CI job.
"""

import argparse
import json
from pathlib import Path
import statistics

import observe
import storage
from storage import digest, save
import run
import oracle
from capabilities import CORPUS


def known_requests(directory, output):
    return storage.native.call("observe", {"action": "check-requests", "directory": str(directory), "output": str(output)})


def check(directory, binaries, fixtures, output):
    output.mkdir()
    storage.verify_server(directory)
    known = known_requests(directory, output / "known")
    storage.upload(directory, fixtures, output / "upload.json")
    receipt = json.loads((output / "upload.json").read_text())
    failed = run.request(fixtures, "li.clustered.eq2-in20", "open", "io", "failed-reader",
                         table_uri=receipt["table_root"] + "/li.clustered")
    failed = observe.invoke(directory, Path("/bin/false"), failed, output / "failed-reader")
    assert failed["status"] == "operational_failure" and failed["storage_capture"]["status"] == "passed"
    assert failed["storage_io"]["requests"] == 0
    cases = ("li.clustered.eq2-in20", "li.shuffled.eq2-in20", "li.clustered.empty",
             "li.clustered.date7-limit", "wide.clustered.eq2-in20")
    references = {}
    for case in cases:
        reference = output / ("reference-" + case)
        references[case] = (reference, oracle.prepare(fixtures, case, reference))

    corpus = json.loads((CORPUS / "manifest.json").read_text())
    dv = next(f for f in corpus["fixtures"] if f["name"] == "deletion_vectors")
    dv_root = "_checks/" + digest(CORPUS / "manifest.json")
    with storage.exclusive(directory):
        for name, expected in dv["files"].items():
            source = CORPUS / "deletion_vectors" / name
            assert source.stat().st_size == expected["bytes"] and digest(source) == expected["sha256"]
            if name.startswith("table/"):
                storage.put_verified(directory, dv_root + "/" + name, source, expected["sha256"])

    records, overhead = [], []
    for binary in binaries:
        reader = json.loads(binary.with_name("build.json").read_text())["reader_id"]
        for case in cases:
            uri = receipt["table_root"] + "/" + case.rsplit(".", 1)[0]
            for mode in (("open", "reuse") if case == cases[0] else ("open",)):
                name = f"{reader}-{case}-{mode}"
                payload = run.request(fixtures, case, mode, "validation", name, table_uri=uri)
                validation_dir = output / (name + "-validation")
                validated = observe.invoke(directory, binary, payload, validation_dir, fixtures, references[case][0])
                assert validated["status"] == "success" and validated["correctness"]["status"] == "passed", validated
                assert validated["storage_capture"]["status"] == "disabled"
                result_dir = output / (name + "-io")
                result = observe.invoke(directory, binary, dict(payload, purpose="io"), result_dir)
                assert result["status"] == "success" and result["storage_capture"]["status"] == "passed", result
                assert result["external_resource_limits"]["process_memory_bytes"] == 8 * 1024**3
                assert result["external_resource_limits"]["swap_bytes"] == 0
                assert all(q["output_rows"] == references[case][1]["output_rows"] and q["physical_plan"] is None for q in result["queries"])
                assert result["provider_evidence"] is None and result["diagnostic_session_ns"] > 0
                assert result["storage_io"]["by_class"]["delta_log"]["requests"] > 0
                if not case.endswith("empty"):
                    assert result["storage_io"]["parquet_get_objects"], result
                assert result["storage_capture"]["started_ns"] <= result["diagnostic_process"]["started_ns"]
                assert result["storage_capture"]["drained_ns"] >= result["diagnostic_process"]["exited_ns"]
                records.append({"reader": reader, "case": case, "mode": mode, "status": "passed", "io": result["storage_io"]})
                # Exercise a real, correctness-gated remote timing invocation.
                if case == cases[0]:
                    timed = run.request(fixtures, case, mode, "timing", name + "-timing", table_uri=uri,
                                        correctness=validation_dir / "correctness.json")
                    measured = observe.invoke(directory, binary, timed, output / (name + "-timing"))
                    assert measured["status"] == "success" and measured["storage_capture"]["status"] == "disabled", measured
                    assert measured["external_metrics"]["response_bytes"] is None

        payload = run.request(fixtures, cases[0], "open", "io", reader + "-dv",
                              table_uri=f"s3://{storage.BUCKET}/{dv_root}/table")
        payload.update(snapshot_version=1, case_id="probe.spark-dv", canonical_sql="SELECT id, value, label FROM bench",
                       fixture_manifest_sha256=digest(CORPUS / "manifest.json"))
        dv_result = observe.invoke(directory, binary, payload, output / (reader + "-dv"))
        assert dv_result["storage_capture"]["status"] == "passed", dv_result
        assert dv_result["status"] == "success" and dv_result["queries"][0]["output_rows"] == 9, dv_result
        assert dv_result["storage_io"]["by_class"]["deletion_vector"]["response_bytes"] > 0, dv_result
        records.append({"reader": reader, "case": "probe.spark-dv", "status": dv_result["status"], "io": dv_result["storage_io"]})

        # ABBA after one untimed warm-up in each mode. These fresh-client,
        # reused-cache diagnostics measure observer cost, never reader speedups.
        samples = {False: [], True: []}
        order = (False, True, False, True, True, False)
        for index, traced in enumerate(order):
            name = f"{reader}-overhead-{index}"
            payload = run.request(fixtures, cases[0], "open", "io", name, table_uri=receipt["table_root"] + "/li.clustered")
            result = observe.invoke(directory, binary, payload, output / name, traced=traced)
            assert result["status"] == "success", result
            if index >= 2: samples[traced].append(result["diagnostic_session_ns"])
        off, on = statistics.median(samples[False]), statistics.median(samples[True])
        overhead.append({"reader": reader, "order": list(order), "warmups": 2, "untraced_session_ns": samples[False],
                         "traced_session_ns": samples[True], "median_difference_ns": on - off,
                         "median_ratio": on / off, "interpretation": "bounded diagnostic overhead check, not a performance estimate"})
        print(json.dumps({"reader": reader, "status": "passed", "median_observer_ratio": on / off}), flush=True)

    secret = storage.credentials(directory)["secret_key"].encode()
    for path in output.rglob("*"):
        if path.is_file() and path.suffix in (".json", ".jsonl", ".log"):
            value = path.read_bytes()
            assert secret not in value and b"AWS4-HMAC-SHA256 Credential=" not in value, path
    result = {"status": "passed", "source_sha256": digest(Path(__file__)), "known": known,
              "readers": records, "observer_overhead": overhead, "server_sha256": digest(directory / "server.json"),
              "fixture_manifest_sha256": digest(fixtures / "manifest.json"), "builds": {str(b): digest(b.with_name("build.json")) for b in binaries}}
    save(output / "checks.json", result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--binary", type=Path, action="append", required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check(args.state, args.binary, args.fixtures, args.output)
