"""Account for nested spans and report canonical walls and observed stage noise."""

import collections
import csv
import gzip
import hashlib
import json
import pathlib
import re
import statistics

PHASES = ("logical", "physical", "total")


def parts(row, index, mode):
    values = {phase: row[phase + "_ns"][index] for phase in PHASES}
    counts = {phase: 1 for phase in PHASES}
    raw = collections.Counter()
    for name, elapsed in row["events"][index]:
        assert isinstance(elapsed, int) and elapsed >= 0
        if "/transformed=" not in name:
            raw[name] += elapsed
            collapsed = re.sub(r"(physical\.optimizer\.rule)/\d+/", r"\1/", name)
            values[collapsed] = values.get(collapsed, 0) + elapsed
            counts[collapsed] = counts.get(collapsed, 0) + 1
    if mode == "canonical":
        assert not row["events"][index], "canonical binary contains observations"
        return values, counts
    assert "physical.analyzer" in raw and "physical.optimizer" in raw
    if row["configuration"].startswith("spark-"):
        assert "logical.parsing" in raw and "physical.planner" in raw
        assert values["logical.parsing.parser_construction"] > 0
    children = {
        "logical": [name for name in raw if name.startswith("logical.") and name.count(".") == 1],
        "logical.parsing": [name for name in raw if name.startswith("logical.parsing.")],
        "physical": (["physical.preparation", "physical.frame", "physical.rename"] if "physical.frame" in raw
                     else [name for name in ("physical.analyzer", "physical.optimizer", "physical.planner") if name in raw]),
        "physical.frame": ["physical.analyzer", "physical.optimizer", "physical.planner"],
        "physical.analyzer": [name for name in raw if name.startswith("physical.analyzer.")],
        "physical.optimizer": [name for name in raw if name.startswith("physical.optimizer.")],
        "physical.planner": [name for name in raw if name.startswith("physical.planner.")],
    }
    for parent, descendants in children.items():
        if parent not in values:
            continue
        remainder = values[parent] - sum(raw[name] for name in descendants)
        assert remainder >= 0, (row["id"], row["configuration"], parent, remainder)
        values[parent + ".residual"] = remainder
        counts[parent + ".residual"] = 1
    return values, counts


def self_check():
    row = {"id": "self-check", "configuration": "native-plain", "logical_ns": [100],
           "physical_ns": [1000], "total_ns": [1100], "events": [[
               ("physical.analyzer", 200), ("physical.analyzer.rule/example", 100),
               ("physical.optimizer", 300), ("physical.optimizer.rule/0/example/body", 100),
               ("physical.optimizer.rule/1/example/body", 100)]]}
    values, counts = parts(row, 0, "observed")
    assert values["physical.residual"] == 500 and values["physical.analyzer.residual"] == 100
    assert values["physical.optimizer.rule/example/body"] == 200 and counts["physical.optimizer.rule/example/body"] == 2
    row["physical_ns"] = [100]
    try:
        parts(row, 0, "observed")
    except AssertionError:
        pass
    else:
        raise AssertionError("negative residual was accepted")


def write_csv(path, rows):
    rows = list(rows)
    assert rows
    with path.open("x", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def source_for(stage, sources):
    if stage.startswith("physical.analyzer.rule/"):
        return sources.get(stage.rsplit("/", 1)[1], "vendor/dependencies/datafusion-optimizer/src/analyzer/mod.rs (call site; rule source unresolved)")
    if stage.startswith("physical.optimizer.rule/"):
        if stage.endswith("/validation"):
            return "vendor/dependencies/datafusion-optimizer/src/utils.rs::assert_valid_optimization"
        if stage.endswith("/preparation"):
            return "vendor/dependencies/datafusion-optimizer/src/optimizer.rs::optimize plan/schema preparation"
        return sources.get(stage.split("/")[1], "vendor/dependencies/datafusion-optimizer/src/optimizer.rs (call site; rule source unresolved)")
    known = {
        "logical": "src/lib.rs::plan_query",
        "physical": "datafusion-54.1.0/src/execution/session_state.rs::create_physical_plan and default planner/framework",
        "logical.substitution": "sail-sql-analyzer/src/substitution.rs::substitute_sql_variables",
        "logical.parsing.lexer_construction": "sail-sql-parser/src/lexer.rs::create_lexer",
        "logical.parsing.lexing": "sail-sql-analyzer/src/parser.rs::parse! lexer.parse",
        "logical.parsing.parser_construction": "sail-sql-parser/src/parser.rs::create_parser",
        "logical.parsing.parse": "sail-sql-analyzer/src/parser.rs::parse! parser.parse",
        "logical.parsing.token_input": "sail-sql-analyzer/src/parser.rs::parse! token slice/map_parser_input",
        "logical.parsing": "sail-sql-analyzer/src/parser.rs::parse_one_statement",
        "logical.ast_conversion": "sail-sql-analyzer/src/statement.rs::from_ast_read_only_statement",
        "logical.timezone": "sail-plan/src/config.rs::PlanConfig::with_normalized_timezone",
        "logical.resolution": "sail-plan/src/resolver/plan.rs::resolve_named_plan",
        "logical.resolver_preparation": "src/statement_planning.rs::plan_statement resolver setup",
        "physical.preparation": "src/lib.rs::SparkSqlQuery::create_physical_plan state/extensions/frame",
        "physical.rename": "sail-common-datafusion/src/rename/physical_plan.rs::rename_physical_plan",
        "physical.analyzer": "vendor/dependencies/datafusion-optimizer/src/analyzer/mod.rs::execute_and_check",
        "physical.analyzer.input_validation": "datafusion-expr::LogicalPlan::check_invariants(Always), analyzer input",
        "physical.analyzer.final_validation": "datafusion-expr::LogicalPlan::check_invariants(Executable), analyzer output",
        "physical.analyzer.function_rewrite_preparation": "datafusion-optimizer/src/analyzer/function_rewrite.rs::ApplyFunctionRewrites::new",
        "physical.optimizer": "vendor/dependencies/datafusion-optimizer/src/optimizer.rs::optimize",
        "physical.optimizer.input_validation": "datafusion-expr::LogicalPlan::check_invariants(Executable), optimizer input",
        "physical.optimizer.final_validation": "datafusion-expr::LogicalPlan::check_invariants(Executable), optimizer output",
        "physical.optimizer.final_schema_validation": "vendor/dependencies/datafusion-optimizer/src/utils.rs::assert_valid_optimization",
        "physical.optimizer.initial_signature": "vendor/dependencies/datafusion-optimizer/src/plan_signature.rs::LogicalPlanSignature::new and signature set",
        "physical.optimizer.pass_signature": "vendor/dependencies/datafusion-optimizer/src/plan_signature.rs::LogicalPlanSignature::new and signature set",
        "physical.planner.default_physical_planner": "datafusion-54.1.0/src/physical_planner.rs::DefaultPhysicalPlanner (including physical optimizers)",
        "physical.planner.extension_order_scan": "sail-plan/src/physical_plan/mod.rs::SparkQueryPlanner::create_physical_plan apply_with_subqueries",
        "physical.planner.spark_lowering": "sail-plan/src/physical_plan/mod.rs::SparkQueryPlanner::create_physical_plan expression/window/prefix lowering",
        "physical.planner.drop_declaration_inputs": "sail-plan/src/declaration_inputs.rs::drop_declaration_inputs",
        "physical.planner.align_result_metadata": "sail-plan/src/physical_plan/mod.rs::align_result_metadata_for_execution",
        "physical.planner.seeded_rand_binding": "sail-plan/src/physical_plan/seeded_rand.rs::bind_seeded_rand_streams",
        "physical.planner": "sail-plan/src/physical_plan/mod.rs::SparkQueryPlanner::create_physical_plan",
        "physical.frame": "datafusion-54.1.0/src/execution/session_state.rs::create_physical_plan",
    }
    if stage.endswith(".residual"):
        return source_for(stage.removesuffix(".residual"), sources) + "; residual includes framework/teardown and observer overhead"
    return known.get(stage, stage + " (framework work at enclosing instrumented call site)")


def run(run_directory, destination):
    root, output = pathlib.Path(run_directory), pathlib.Path(destination)
    completion = json.loads((root / "complete.json").read_text())
    sample_path = root / "samples.jsonl.gz"
    assert hashlib.sha256(sample_path.read_bytes()).hexdigest() == completion["samples_sha256"]
    with gzip.open(sample_path, "rt") as source:
        rows = [json.loads(line) for line in source]
    assert completion["processes"] == 70 and len(rows) == completion["records"] == 2100
    metadata = json.loads((root / "metadata.json").read_text())
    expected = {item["process"]: item for item in metadata["schedule"]["runs"]}
    assert set(row["process"] for row in rows) == set(expected)
    process_data, shapes, passes = {}, {}, collections.defaultdict(set)
    for row in rows:
        assert all(row[key] == expected[row["process"]][key] for key in ("mode", "configuration", "series", "arm", "pair"))
        per_stage, call_counts = collections.defaultdict(list), collections.defaultdict(list)
        for index in range(9):
            values, counts = parts(row, index, row["mode"])
            if row["mode"] == "observed":
                shape = tuple(name for name, _ in row["events"][index])
                key = (row["configuration"], row["id"])
                assert shapes.setdefault(key, shape) == shape, ("rule/pass order changed", key)
                for name in shape:
                    match = re.match(r"physical\.optimizer\.rule/(\d+)/([^/]+)/body$", name)
                    if match:
                        passes[(row["configuration"], row["id"], match[2])].add(int(match[1]))
            for stage, value in values.items():
                per_stage[stage].append(value)
                call_counts[stage].append(counts[stage])
        for stage, values in per_stage.items():
            key = (row["mode"], row["series"], row["arm"], row["configuration"], row["id"], stage)
            process_data.setdefault(key, []).append((statistics.median(values), statistics.median(call_counts[stage])))
    output.mkdir(parents=True, exist_ok=False)
    medians, main = {}, []
    for key, samples in sorted(process_data.items(), key=lambda item: str(item[0])):
        mode, series, arm, configuration, query, stage = key
        assert len(samples) == 5, key
        if series != "main":
            continue
        values = [sample[0] for sample in samples]
        q1, _, q3 = statistics.quantiles(values, n=4, method="inclusive")
        median = statistics.median(values)
        medians[(mode, configuration, query, stage)] = median
        rule = stage.split("/")[1] if stage.startswith("physical.optimizer.rule/") else ""
        main.append(dict(mode=mode, configuration=configuration, id=query, stage=stage,
                         median_ms=median / 1e6, min_ms=min(values) / 1e6, max_ms=max(values) / 1e6,
                         iqr_ms=(q3-q1) / 1e6, calls=statistics.median(sample[1] for sample in samples),
                         optimizer_pass_indices=";".join(map(str, sorted(passes[(configuration, query, rule)]))) if rule else ""))
    noise = []
    noise_values = {}
    for key, samples in process_data.items():
        mode, series, arm, configuration, query, stage = key
        if mode == "canonical" and (series != "noise" or arm != "a"):
            continue
        if mode == "observed" and (series != "main" or arm != "a"):
            continue
        right = process_data[(mode, "noise", "b", configuration, query, stage)]
        left_median, right_median = (statistics.median(sample[0] for sample in side) for side in (samples, right))
        difference = abs(left_median - right_median)
        noise_values[(mode, configuration, query, stage)] = difference
        noise.append(dict(mode=mode, configuration=configuration, id=query, stage=stage,
                          a_ms=left_median / 1e6, b_ms=right_median / 1e6, absolute_difference_ms=difference / 1e6,
                          ratio=right_median / left_median if left_median else ""))
    sources = metadata["code_sources"]
    large, walls = [], []
    for item in main:
        mode, configuration, query, stage = (item[key] for key in ("mode", "configuration", "id", "stage"))
        if stage in PHASES:
            canonical = medians[("canonical", configuration, query, stage)]
            native = medians[("canonical", "native-configured", query, stage)]
            if mode == "canonical":
                observed = medians[("observed", configuration, query, stage)]
                walls.append(dict(configuration=configuration, id=query, phase=stage,
                    canonical_ms=canonical / 1e6, native_configured_ms=native / 1e6,
                    spark_or_configuration_gap_ms=(canonical-native) / 1e6,
                    observed_ms=observed / 1e6, observer_wall_difference_ms=(observed-canonical) / 1e6))
            continue
        phase = stage.split(".")[0]
        native = medians[("canonical", "native-configured", query, phase)]
        if item["median_ms"] * 1e6 > native:
            large.append(dict(configuration=configuration, id=query, stage=stage, median_ms=item["median_ms"],
                              native_phase_ms=native / 1e6, stage_noise_ms=noise_values[(mode, configuration, query, stage)] / 1e6,
                              source=source_for(stage, sources)))
    write_csv(output / "planning-1390-stages.csv", main)
    write_csv(output / "planning-1390-noise.csv", sorted(noise, key=lambda row: (row["mode"], row["configuration"], row["id"], row["stage"])))
    write_csv(output / "planning-1390-walls.csv", walls)
    write_csv(output / "planning-1390-large-parts.csv", large)
    grouped = collections.defaultdict(list)
    for row in large:
        grouped[(row["stage"], row["source"])].append(row)
    report = ["# Planning attribution (#1390, PR1)", "", "Canonical walls use untouched production source. Observed stages use the committed disposable overlay. No query was executed.", "",
              f"Source baseline: `{metadata['receipts']['canonical']['baseline']}`. Protocol: `{metadata['receipts']['canonical']['protocol_commit']}`.", "",
              "All 120 paired/stress configuration cases and 558 applicable Spark sweep plans matched their canonical public plan displays, properties and exact schemas. The frozen #1379 execution checks provide historical result evidence; this run did not repeat result validation.", "",
              "[Wall times](planning-1390-walls.csv) retain both native controls and observer inflation. [Stage medians](planning-1390-stages.csv) include every measured case, rule application count and pass indices. [A/B noise](planning-1390-noise.csv) gives each observed stage's same-binary difference. [Large parts](planning-1390-large-parts.csv) lists each component larger than its whole canonical native-configured phase.", "",
              "Parent intervals include their child intervals. Residuals contain framework work, teardown and observation overhead. DefaultPhysicalPlanner includes physical optimizer passes and their validation. Small parts near their A/B difference remain unresolved; the observer wall difference is not subtracted from their values.", "",
              "| Component | Cases above native phase | Largest median (ms) | Code |", "| --- | ---: | ---: | --- |"]
    for (stage, source), cases in sorted(grouped.items()):
        report.append(f"| `{stage}` | {len(cases)} | {max(row['median_ms'] for row in cases):.6f} | `{source}` |")
    report += ["", "Raw samples, full plan traces, assembly hashes and build receipts accompany these tables. No parser or planning optimization was made. Exact public snapshots do not compare opaque execution-node internals.", ""]
    (output / "planning-1390.md").write_text("\n".join(report))
