"""Bounded aggregation check; simulated records are not performance evidence."""

import copy
import json
from pathlib import Path
import tempfile
from unittest.mock import patch

import production_report as reporting


def main():
    identity = {"comparison_revision": 5, "sampling_stage": "formal", "workload_manifest_sha256": "definition"}
    cases = [{"case_id": f"production.{shape}.{layout}" + suffix,
              "geometry": {"files": 130 if shape == "q2" else 60, "stored_columns": 416 if shape == "q2" else 90,
                           "physical_bytes": 1024**3}, "projection_columns": 69 if shape == "q2" else 71}
             for shape in ("q2", "q4") for layout in ("localized", "scattered") for suffix in ("", ".dv")]
    definition = {"scope": "formal", "cases": cases, "inventory": {row["case_id"]: {} for row in cases}}
    config = {**identity, "reader_builds": {reader: {"version": "pinned"} for reader in reporting.READERS},
              "server": {"build_sha256": "minio", "cpus": {"reader": list(range(8))}, "topology": [],
                         "kernel": "fixed", "cpu_info": "model name: check-cpu\ncpu MHz: 1000\n"},
              "cache_policy": "fresh clients; reused caches"}
    reports = {}
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        for case in cases:
            path = root / case["case_id"]
            path.mkdir()
            (path / "campaign.json").write_text(json.dumps(config))
            (path / "frozen.json").write_text("{}")
            (path / "observations.jsonl").write_text("")
            rows = []
            for mode in reporting.MODES:
                for reader in reporting.READERS:
                    unsupported = case["case_id"].endswith(".dv") and reader == "daft"
                    rows.append({"case": case, "job": {"case_id": case["case_id"], "execution_mode": mode},
                                 "reader_id": reader, "gate": {"status": "unsupported" if unsupported else "success",
                                     "identity": {"reader_build_sha256": reader}},
                                 "measurements": {"eligible": not unsupported, "scheduled_samples": 0 if unsupported else 5,
                                     "metrics": {} if unsupported else {"open_query_ns": {"samples": 5, "median": 2e9}}},
                                 "oracle": {"output_rows": 895}, "native_phases": {"planning_ns": None}})
            reports[path] = {"campaign_id": path.name, "status": "complete", "rows": rows,
                             "observations_sha256": "journal", "summary_sha256": "summary"}

        def aggregate(paths):
            destination = root / ("report-" + str(len(list(root.glob("report-*")))))
            return reporting.report(root / "definition.json", paths, destination)

        with patch.object(reporting.large_workloads, "load", return_value=definition), \
             patch.object(reporting.large_workloads, "identity", return_value=identity), \
             patch.object(reporting, "comparison_identity", side_effect=lambda value: {key: value[key] for key in identity}), \
             patch.object(reporting.large_workloads, "report", side_effect=lambda path, output: reports[path]):
            paths = list(reports)
            result = aggregate(paths[:1])
            assert len(result["rows"]) == 80 and len(result["complete_cases"]) == 1
            assert sum(row["status"] == "not_run" for row in result["rows"]) == 70
            result = aggregate(paths)
            assert result["formal_coverage_complete"] and not result["publication_ready"]
            assert sum(row["status"] == "unsupported" for row in result["rows"]) == 8
            reports[paths[0]]["status"] = "incomplete"
            assert not aggregate(paths)["formal_coverage_complete"]
            reports[paths[0]]["status"] = "complete"

            def rejected(paths):
                try:
                    aggregate(paths)
                except ValueError:
                    return
                raise AssertionError("invalid formal aggregation accepted")

            rejected([paths[0], paths[0]])
            altered = copy.deepcopy(config)
            altered["sampling_stage"] = "pilot"
            (paths[0] / "campaign.json").write_text(json.dumps(altered))
            rejected(paths[:1])
            altered = copy.deepcopy(config)
            altered["reader_builds"]["daft"]["version"] = "different"
            (paths[0] / "campaign.json").write_text(json.dumps(altered))
            rejected(paths[:2])
            (paths[0] / "campaign.json").write_text(json.dumps(config))
            network = {"profile": {"latency_ms": 200, "jitter_ms": 20, "mbps": 150, "seed": "fixed"},
                       "cpu": 9, "binary_sha256": "proxy", "source_sha256": {"proxy": "source"},
                       "byte_boundary": "body", "chunk_bytes": 65536, "jitter": "deterministic", "go_version": "pinned",
                       "limits": {"MemoryMax": "536870912", "MemorySwapMax": "0"}}
            altered = copy.deepcopy(config)
            altered["server"]["network"] = network
            for path in paths[:2]:
                (path / "campaign.json").write_text(json.dumps(altered))
            assert len(aggregate(paths[:2])["complete_cases"]) == 2
            altered["server"]["network"]["profile"]["mbps"] = 75
            (paths[1] / "campaign.json").write_text(json.dumps(altered))
            rejected(paths[:2])
            for path in paths[:2]:
                (path / "campaign.json").write_text(json.dumps(config))
            success = {"request": {"resource_budget": reporting.BUDGET}, "observation": {"status": "success",
                       "supervision": {"query_deadline_seconds": 1800, "cleanup_deadline_seconds": 60},
                       "external_resource_limits": {"cpu_affinity": list(range(8)), "swap_bytes": 0,
                                                     "process_memory_bytes": reporting.BUDGET["process_memory_bytes"]}}}
            journal = paths[0] / "observations.jsonl"
            journal.write_text(json.dumps(success) + "\n")
            assert len(aggregate(paths[:1])["complete_cases"]) == 1
            success["observation"]["supervision"]["query_deadline_seconds"] = 600
            journal.write_text(json.dumps(success) + "\n")
            rejected(paths[:1])
            journal.write_text("")
            metric = reports[paths[0]]["rows"][0]["measurements"]["metrics"]["open_query_ns"]
            metric["samples"] = 4
            rejected(paths[:1])
            metric["samples"] = 5
            reports[paths[0]]["rows"][0]["case"] = dict(cases[0], projection_columns=1)
            rejected(paths[:1])
    print("Staged coverage, unsupported readers, incomplete batches and incompatible input rejection passed")


if __name__ == "__main__":
    main()
