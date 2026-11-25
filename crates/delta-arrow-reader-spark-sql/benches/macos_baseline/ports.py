"""Connect the historical focused harnesses to the macOS session controller."""

import time

from native import BENCHES, legacy_runner


def process(binary_dir, case, count, output, resources, timeout, block):
    suite, name, mode = (case[key] for key in ("suite", "id", "configuration"))
    deadline = time.monotonic() + timeout
    output.mkdir(parents=True)
    if suite == "functions_1406":
        assert count == 3
        with legacy_runner("functions_1406/run.py") as runner:
            rows = runner["probe"](binary_dir / "functions_1406_driver", case["input_profile"],
                                   ["measure", BENCHES / "functions/manifest.json", name, mode], output / "samples.jsonl")
        return "ok", rows
    if suite == "regression_windows_1512":
        from cells import function_set, read
        schedule = read(BENCHES / "functions/schedule.json")
        with legacy_runner("functions/run.py") as runner:
            result = function_set(binary_dir / "functions", [name], mode, count, output / "process", deadline, schedule, runner)
        if result["status"] == "ok" and count == 3:
            with legacy_runner("regression_windows_1512/run.py") as runner:
                assert runner["verify"](result["records"], [name], mode)
        return result["status"], result["records"]
    if suite == "nullif_620_recheck":
        with legacy_runner("nullif_620_recheck/run.py") as runner:
            args = ["--macos-samples", count, "measure", name.split("-")[0], mode, block,
                    resources["nullif_eligible"], name]
            rows = runner["call"](binary_dir / "nullif_620", args, output / "measure", deadline)
        return "ok", rows
    if suite == "footprint":
        assert count == 1 and mode == "native"
        with legacy_runner("footprint/native.py") as runner:
            query, = [row for row in runner["MANIFEST"]["cases"] if row["id"] == name]
            runner["MANIFEST"]["snapshot"] = str(resources["snapshot"])
            inputs = output / "inputs"
            runner["write_query_inputs"](inputs)
            command = runner["command"](resources["stock_cli"], query, inputs)
            row = runner["invoke"](command, output / "process", timeout, timed=True)
        return "ok", [{"id": name, "configuration": mode, **row}]
    if suite == "pending_range_1099":
        binary = binary_dir / "pending_range_isolated"
        arguments = ["measure", BENCHES / "pending_range_1099/manifest.json", resources["range_eligible"], str(block)]
        with legacy_runner("pending_range_1099/run.py") as runner:
            rows = runner["invoke"]({"binary": binary}, arguments, output, "measure")
        return "ok", rows
    assert suite == "pending_range_1099_isolated"
    with legacy_runner("pending_range_1099_isolated/run.py") as runner:
        args = ["measure", runner["MANIFEST"], resources["range_eligible"], str(block), runner["NATIVE_CONTROLS"]]
        status, rows = runner["invoke"](binary_dir / "pending_range_isolated", args, output, "measure", deadline)
    return status, rows
