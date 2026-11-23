"""Check the full fixture, then time equivalent restricted cases in both builds."""

import argparse
from pathlib import Path
import sys
import time
from unittest.mock import patch

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "time_conversions_1409_round3"))
import run_pair
import compare
from run import runner, schedule, write

FULL_MANIFEST = HERE.parent / "functions" / "manifest.json"


def verify_full(builds, cases, output):
    tuning = schedule()
    deadline = time.monotonic() + 1800
    checks = {}
    for case_id in cases:
        checks[case_id] = {}
        for arm, build in builds.items():
            work = output / case_id / arm
            plans = work / "plans"
            result = runner.invoke(build / "functions", ["preflight", FULL_MANIFEST, case_id, plans],
                                   work, tuning, deadline, preflight=True)
            if result["status"] != "ok":
                raise ValueError(f"{case_id} {arm}: full-input preflight {result['status']}")
            payload = result["records"][-1]
            assert payload["input_rows"] == tuning["input_rows"]
            assert len(payload["baselines"]) == 3
            assert all(o["rows"] == tuning["input_rows"] for o in payload["baselines"])
            checks[case_id][arm] = {}
            for outcome in payload["results"]:
                mode = outcome["configuration"]
                assert outcome["status"] == "ok"
                assert outcome["source_guard_rows"] == tuning["input_rows"]
                checks[case_id][arm][mode] = {
                    "rows": outcome["rows"], "types": outcome["types"],
                    "result_sha256": runner.digest(plans / f"{mode}.rows.json"),
                }
        assert set(checks[case_id]["staging"]) == {"native", "spark-off", "spark-on"}
        assert checks[case_id]["staging"] == checks[case_id]["candidate"], case_id
    write(output / "verified.json", checks)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    for name in ("pins", "staging", "candidate", "output"):
        cli.add_argument(name, type=lambda v: Path(v).resolve())
    args = cli.parse_args()
    pins = runner.read(args.pins)
    args.output.mkdir()
    builds = {"staging": args.staging, "candidate": args.candidate}
    for arm, directory in builds.items():
        receipt = runner.read(directory / "receipt.json")
        assert receipt["source"] == pins[f"{arm}_source"]
        assert runner.digest(directory / "functions") == receipt["sha256"]
    verify_full(builds, pins["case_ids"], args.output / "full-input")
    active_schedule = dict(schedule(), input_rows=pins["restricted_input_rows"])
    with patch.object(compare, "schedule", lambda: active_schedule):
        run_pair.run(args.pins, args.staging, args.candidate, args.output / "restricted")


if __name__ == "__main__":
    main()
