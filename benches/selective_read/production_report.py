"""Combine audited formal batches without pooling or replacing their samples."""

import argparse
import json
from pathlib import Path
import tempfile

import campaign as runner
import large_workloads
import metadata_cache
from run import BUDGET, comparison_identity, digest, reader_roster, save
from supervise import require

READERS = large_workloads.READERS
MODES = ("open", "reuse")


def conditions(config):
    server = config["server"]
    network = server.get("network")
    model_fields = {"vendor_id", "cpu family", "model", "model name", "stepping", "microcode"}
    cpu_model = sorted({(key.strip(), value.strip()) for line in server["cpu_info"].splitlines()
                        for key, _, value in [line.partition(":")] if key.strip() in model_fields})
    return {"reader_builds": config["reader_builds"], "resource_budget": BUDGET,
            "server_build_sha256": server["build_sha256"], "cpus": server["cpus"],
            "topology": server["topology"], "cpu_model": cpu_model, "kernel": server["kernel"],
            "cache_policy": config["cache_policy"],
            "transport": ({**{k: network[k] for k in ("profile", "cpu", "binary_sha256", "source_sha256",
                                                     "byte_boundary", "chunk_bytes", "jitter", "go_version")},
                           "limits": {k: network["limits"][k] for k in ("MemoryMax", "MemorySwapMax")}}
                          if network else None)}


def report(definition, campaigns, output):
    with metadata_cache.bindings():
        return report_cached(definition, campaigns, output)


def report_cached(definition, campaigns, output):
    frozen = large_workloads.load(definition)
    identity = large_workloads.identity(definition)
    readers = reader_roster(identity)
    require(identity["comparison_revision"] in (5, 6) and identity["sampling_stage"] == "formal"
            and frozen["scope"] == "formal"
            and set(frozen["inventory"]) == {r["case_id"] for r in frozen["cases"]},
            "use the complete eight-case formal definition")
    require(not output.exists(), "report output already exists")
    cases = {r["case_id"]: r for r in frozen["cases"]}
    entries, sources, shared, builds = {}, [], None, {}
    with tempfile.TemporaryDirectory(dir=output.parent, prefix="production-report-") as scratch:
        for index, campaign in enumerate(campaigns):
            config = json.loads((campaign / "campaign.json").read_text())
            combined = runner.combined_diagnostics(config)
            gate_warmup = runner.validation_warmup(config)
            comparison = comparison_identity(config)
            require(all(comparison.get(k) == v for k, v in identity.items() if k != "workload_manifest_sha256"),
                    "campaign is not from the same formal protocol")
            observed = conditions(config)
            require(shared is None or shared == observed, "incompatible reader builds, resources, cache or transport")
            shared = observed
            audited = large_workloads.report(campaign, Path(scratch) / str(index))
            observations = [json.loads(line) for line in (campaign / "observations.jsonl").read_text().splitlines()]
            if combined or gate_warmup:
                runner.validate_diagnostics(config, json.loads((campaign / "inventory.json").read_text()),
                                            json.loads((campaign / "schedule.json").read_text()), observations)
            for row in observations:
                record = row["observation"]
                if record["status"] == "success":
                    limits, watch = record["external_resource_limits"], record["supervision"]
                    require(row["request"]["resource_budget"] == BUDGET
                            and watch["query_deadline_seconds"] == 1800 and watch["cleanup_deadline_seconds"] == 60
                            and limits["cpu_affinity"] == shared["cpus"]["reader"]
                            and limits["process_memory_bytes"] == BUDGET["process_memory_bytes"] and limits["swap_bytes"] == 0,
                            "formal resource or watchdog limits changed")
            for row in audited["rows"]:
                job, reader = row["job"], row["reader_id"]
                case, mode = job["case_id"], job["execution_mode"]
                key = case, mode, reader
                require(case in cases and row["case"] == cases[case], "campaign case differs from the frozen definition")
                require(key not in entries, "duplicate case/profile coverage; do not pool retries")
                gate = row["gate"]
                native = gate.get("identity")
                if native:
                    build = native["reader_build_sha256"]
                    require(builds.setdefault(reader, build) == build, "native reader build changed")
                measurement = row["measurements"]
                if measurement["eligible"]:
                    require(measurement["scheduled_samples"] == 5
                            and all(metric["samples"] == 5 for metric in measurement["metrics"].values()),
                            "formal distributions require five independent samples")
                status = "incomplete" if gate["status"] == "success" and not measurement["eligible"] else gate["status"]
                entries[key] = {**{k: v for k, v in row.items() if k != "case"}, "status": status,
                                "campaign_id": audited["campaign_id"], "campaign_status": audited["status"],
                                "warmup_source": "exact-validation-gate" if gate_warmup else "standalone",
                                "plans": [{"run_id": observed["run_id"], "artifacts": observed["observation"]["plan_artifacts"]}
                                          for observed in observations if observed.get("job_id") == job["id"]
                                          and observed.get("reader_id") == reader and "plan_artifacts" in observed["observation"]]}
            sources.append({"path": str(campaign.resolve()), "campaign_id": audited["campaign_id"],
                            "upload_verification": config.get("upload_verification"),
                            "in_process_upload": config.get("in_process_upload", False),
                            "combined_diagnostics": combined,
                            "gate_warmup": gate_warmup,
                            "warmup_amendment_sha256": config.get("warmup_amendment_sha256"),
                            "diagnostics_amendment_sha256": config.get("diagnostics_amendment_sha256"),
                            "status": audited["status"], "workload_manifest_sha256": comparison["workload_manifest_sha256"],
                            "observations_sha256": audited["observations_sha256"], "summary_sha256": audited["summary_sha256"],
                            "config_sha256": digest(campaign / "campaign.json"), "frozen_sha256": digest(campaign / "frozen.json")})
    rows = [entries.get((case, mode, reader), {"job": {"id": case if mode == "open" else "reuse." + case,
                                                     "case_id": case, "execution_mode": mode},
                                             "reader_id": reader, "status": "not_run", "gate": None,
                                             "measurements": None, "oracle": None, "campaign_id": None,
                                             "campaign_status": None})
            for case in cases for mode in MODES for reader in readers]
    complete = [case for case in cases if all(row["status"] in ("success", "unsupported")
                                             and row["campaign_status"] == "complete"
                                             for row in rows if row["job"]["case_id"] == case)]
    result = {"format": "selective-read-production-report-v1", **identity,
              "status": "complete" if len(complete) == len(cases) else "incomplete",
              "formal_coverage_complete": len(complete) == len(cases), "complete_cases": complete,
              "publication_ready": False, "readers": list(readers), "conditions": shared, "reader_build_sha256": builds,
              "definition": str(definition.resolve()), "cases": cases, "campaigns": sources, "rows": rows}
    output.mkdir()
    save(output / "production-report.json", result)
    (output / "README.md").write_text(markdown(result))
    return result


def markdown(result):
    def seconds(row, key):
        metric = ((row["measurements"] or {}).get("metrics", {}).get(key))
        return (f"{metric['median'] / 1e9:.3f} [{metric['q1'] / 1e9:.3f}, {metric['q3'] / 1e9:.3f}]"
                if metric else "-")
    lines = ["# Staged Q2/Q4 benchmark report", "",
             f"Completed formal cases: {len(result['complete_cases'])}/8. Publication ready: false.", "",
             "Times show median [Q1, Q3] in seconds from five independent invocations.",
             "The bracketed range covers the middle 50% of timings. Reuse retains one native source for two queries;",
             "initialization plus query 1 and query 2 are reported separately. Missing results have no timing value.", "",
             "| Case | Files | Stored columns | Output columns | Parquet GiB |",
             "| --- | ---: | ---: | ---: | ---: |"]
    for case, row in result["cases"].items():
        geometry = row["geometry"]
        lines.append(f"| {case} | {geometry['files']} | {geometry['stored_columns']} | {row['projection_columns']} | {geometry['physical_bytes'] / 1024**3:.2f} |")
    lines += ["", "DV snapshots share their base snapshot's Parquet data. Table bytes describe the physical layout, not traffic.", "",
              "| Case | Reader | Open status | Open seconds | Reuse status | Init + query 1 seconds | Query 2 seconds |",
              "| --- | --- | --- | ---: | --- | ---: | ---: |"]
    entries = {(r["job"]["case_id"], r["job"]["execution_mode"], r["reader_id"]): r for r in result["rows"]}
    for case in result["cases"]:
        for reader in result["readers"]:
            opened, reused = (entries[case, mode, reader] for mode in MODES)
            lines.append(f"| {case} | {reader} | {opened['status']} | {seconds(opened, 'open_query_ns')} | {reused['status']} | {seconds(reused, 'initialization_plus_query1_ns')} | {seconds(reused, 'query_1.completion_ns')} |")
    lines += ["", "The JSON report retains IQRs, sample statuses, eligible ratios, exact-gate evidence, file/group/page geometry",
              "and separate I/O diagnostics. An incomplete campaign remains visible and cannot complete a case.", "",
              "Each campaign declares whether plans share its untimed I/O invocation. DuckDB EXPLAIN remains separate;",
              "the JSON retains diagnostic modes and plan hashes. Diagnostic sessions are excluded from timing distributions.", "",
              "Warmup may come from the exact validation gate or a separate invocation, as declared per campaign.",
              "Compare readers within the same case and declared warming method; samples are not pooled across methods.", "",
              "Matching cases and query profiles are taken from one audited campaign each. Samples are never pooled across retries.", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--definition", type=Path, required=True)
    parser.add_argument("--campaign", type=Path, action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = report(args.definition, args.campaign, args.output)
    print(json.dumps({k: result[k] for k in ("status", "complete_cases", "formal_coverage_complete", "publication_ready")}))
