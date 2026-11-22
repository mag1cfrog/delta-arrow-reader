"""Count remaining buffer copies and argument work in the merged RANGE path."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "pending_range_1099_isolated"))
from build import CRATE, LEGACY, RELEASE, digest, git
from run import FROZEN, MANIFEST, SPARK_REFERENCE, append, invoke, reference_rows

COUNTERS = "datafusion_common::pending_range_1099::"
SPARK_AGGREGATE = "vendor/dependencies/datafusion-physical-expr/src/window/spark_integer_range_aggregate.rs"


def instrument(workspace):
    def insert(path, anchor, replacement):
        file = workspace / path
        source = file.read_text()
        assert source.count(anchor) == 1, path
        file.write_text(source.replace(anchor, replacement, 1))

    common = FROZEN["COMMON"]
    insert(common + "lib.rs", "pub mod utils;",
           "pub mod utils;\n/// Disposable bounded-window observations.\npub mod pending_range_1099;")
    shutil.copyfile(LEGACY / "events.rs", workspace / common / "pending_range_1099.rs")
    start = "        self.record_batch =\n            concat_batches"
    insert(FROZEN["STATE"], start,
           f"        {COUNTERS}add(4, 1);\n"
           f"        {COUNTERS}add(5, self.record_batch.num_rows() + batch.num_rows());\n" + start)
    start = "        let partition_batches =\n            self.evaluate_partition_batches(&record_batch, window_expr)?;"
    insert(FROZEN["EXEC"], start, f"        {COUNTERS}input_batch(record_batch.num_rows());\n" + start)
    start = "            concat_batches(self.input_schema(), [input_buffer, &record_batch])?"
    insert(FROZEN["EXEC"], start,
           f"            {COUNTERS}add(6, 1);\n"
           f"            {COUNTERS}add(7, input_buffer.num_rows() + record_batch.num_rows());\n" + start)
    comparison = "        let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {"
    insert(FROZEN["STATE"], comparison,
           comparison + f"\n            {COUNTERS}add(if SIDE {{ 18 }} else {{ 17 }}, 1);")
    for file, indent, receiver in [(FROZEN["EXPR"], "        ", "self"),
                                    (SPARK_AGGREGATE, "    ", "aggregate")]:
        evaluation = f"{indent}let values = {receiver}.evaluate_args(record_batch)?;"
        insert(file, evaluation, evaluation + f"\n{indent}{COUNTERS}add(10, 1);\n"
               f"{indent}{COUNTERS}add(11, values.iter().map(|value| value.len()).sum());")
    insert("benches/pending_range_1099/driver.rs", "// @observations@", """fn reset_events(_: bool) {
    datafusion::common::pending_range_1099::reset(false);
}
fn snapshot_events() -> Value {
    let (counters, pending) = datafusion::common::pending_range_1099::snapshot();
    json!({"counters":counters.into_iter().collect::<BTreeMap<_,_>>(),"pending":pending})
}""")
    entry = workspace / "benches/pending_range_observed.rs"
    entry.write_text('''//! Disposable pending RANGE work counter.
#[path = "pending_range_1099/driver.rs"]
mod pending;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    pending::run().await
}
''')
    with (workspace / "Cargo.toml").open("a") as manifest:
        manifest.write('\n[[bench]]\nname = "pending_range_observed"\n'
                       'path = "benches/pending_range_observed.rs"\nharness = false\n')


def build(source, target, output):
    assert git("rev-parse", source) == source and not git("status", "--porcelain")
    repo = Path(git("rev-parse", "--show-toplevel"))
    assert not target.is_relative_to(repo) and not output.is_relative_to(repo)
    output.mkdir()
    with tempfile.TemporaryDirectory(prefix="range-counters-", dir=output.parent) as scratch:
        with tempfile.TemporaryFile() as archive:
            subprocess.run(["git", "archive", source], cwd=repo, stdout=archive, check=True)
            archive.seek(0)
            with tarfile.open(fileobj=archive) as contents:
                contents.extractall(scratch, filter="data")
        workspace = Path(scratch) / CRATE
        instrument(workspace)
        cargo, rustc = shutil.which("cargo"), shutil.which("rustc")
        assert cargo and rustc
        tools = os.pathsep.join((str(Path(cargo).parent), str(Path(rustc).parent), os.defpath))
        settings = {"HOME": str(Path.home()), "PATH": tools, "TZ": "UTC",
                    "CARGO_TARGET_DIR": str(target), **RELEASE}
        command = ["cargo", "+1.96.0", "build", "--offline", "--locked", "--release",
                   "--bench", "pending_range_observed", "--message-format=json"]
        with subprocess.Popen(command, cwd=workspace, env=settings, stdout=subprocess.PIPE,
                              text=True) as process:
            artifacts = [msg["executable"] for msg in map(json.loads, process.stdout)
                         if msg["reason"] == "compiler-artifact"
                         and msg["target"]["name"] == "pending_range_observed" and msg.get("executable")]
            assert process.wait() == 0 and len(artifacts) == 1
        output_binary = output / "pending_range_observed"
        shutil.copy2(artifacts[0], output_binary)
    (output / "receipt.json").write_text(json.dumps({
        "source": source, "sha256": digest(output_binary), "observer_sha256": digest(Path(__file__)),
    }) + "\n")


def check_counts(records):
    constant = [row for row in records if row["shape"] == "constant"]
    assert len(constant) == 27
    for row in constant:
        rows, batch = row["rows"], row["batch"]
        batches, extra = divmod(rows, batch)
        assert extra == 0 and batches > 0
        prefix_rows = batch * batches * (batches + 1) // 2
        counters = row["before_eof"]["counters"]
        assert counters["input_batches"] == counters["argument_calls"] == batches
        assert counters["input_rows"] == rows and row["before_eof_rows"] == 0
        assert counters["input_concat_calls"] == counters["partition_concat_calls"] == batches - 1
        assert counters["input_copied_rows"] == counters["partition_copied_rows"] == prefix_rows - batch
        assert counters["argument_array_rows"] == prefix_rows
        expected_comparisons = prefix_rows if row["configuration"] == "native" else rows
        assert counters["upper_comparisons"] == expected_comparisons
    return constant


def observe(canonical, observed, reference, output):
    assert not git("status", "--porcelain")
    receipts = {arm: json.loads((directory / "receipt.json").read_text())
                for arm, directory in [("canonical", canonical), ("observed", observed)]}
    original, instrumented = receipts["canonical"], receipts["observed"]
    assert instrumented["observer_sha256"] == digest(Path(__file__))
    assert not git("diff", "--name-only", original["source"], instrumented["source"])
    spark = reference_rows(reference)
    output.mkdir()
    archive = output / "samples.jsonl.gz"
    builds = {arm: {key: receipt[key] for key in ("source", "sha256")}
              for arm, receipt in receipts.items()}
    append(archive, {"protocol": git("rev-parse", "HEAD"), "builds": builds,
                     "reference_sha256": SPARK_REFERENCE, "manifest_sha256": digest(MANIFEST)})
    traces = {}
    deadline = time.monotonic() + 300
    for name, directory, filename in [
        ("canonical", canonical, "pending_range_isolated"),
        ("observed", observed, "pending_range_observed"),
    ]:
        binary = directory / filename
        assert digest(binary) == receipts[name]["sha256"]
        status, rows = invoke(binary, ["trace", MANIFEST], output, name, deadline)
        append(archive, {"arm": name, "status": status, "records": rows})
        assert status == "ok" and len(rows) == 63 and len({row["id"] for row in rows}) == 63
        traces[name] = rows

    for original_row, observed_row in zip(traces["canonical"], traces["observed"], strict=True):
        assert original_row == observed_row | {"before_eof": None, "after_eof": None}, original_row["id"]
        if observed_row["configuration"] != "native":
            expected, = (case for case in spark if case["coverage"]["shape"] == observed_row["shape"]
                         and case["coverage"]["rows"] == observed_row["rows"]
                         and case["ansi"] == (observed_row["configuration"] == "spark-on"))
            FROZEN["compare_reference"](observed_row, expected)
    check_counts(traces["observed"])
    append(archive, {"validated_trace_cells": 63, "scaling_cells": 27})
    print("Validated 63 paired traces and 27 scaling cells")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    build_command = commands.add_parser("build")
    build_command.add_argument("source")
    build_command.add_argument("target", type=Path)
    build_command.add_argument("output", type=Path)
    observe_command = commands.add_parser("observe")
    for name in ("canonical", "observed", "reference", "output"):
        observe_command.add_argument(name, type=Path)
    args = vars(parser.parse_args())
    action = args.pop("action")
    {"build": build, "observe": observe}[action](**{
        key: value.resolve() if isinstance(value, Path) else value for key, value in args.items()
    })
