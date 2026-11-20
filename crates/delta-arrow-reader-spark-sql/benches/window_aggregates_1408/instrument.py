"""Insert phase clocks into an isolated staging checkout."""

from pathlib import Path

EXPR = Path("vendor/dependencies/datafusion-physical-expr/src")
PLAN = Path("vendor/dependencies/datafusion-physical-plan/src/aggregates")
PHASES = ("create", "discard", "replay", "update", "retract", "merge",
          "evaluate", "state", "materialize", "convert")
NAMES = "Create Discard Replay Update Retract Merge Evaluate State Materialize Convert"
EXPR_IMPORT = "use crate::aggregate::dar_aggregate_probe::{self as probe, Phase};\n"
PLAN_IMPORT = "use datafusion_physical_expr::aggregate::dar_aggregate_probe::{self as probe, Phase};\n"
CLOCKS = """
/// Scratch aggregate phase clocks for #1408.
pub mod dar_aggregate_probe {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::time::Instant;

    #[derive(Clone, Copy)]
    pub enum Phase { PHASES }

    const LEN: usize = PHASE_COUNT;
    static ELAPSED: [AtomicU64; LEN] = [const { AtomicU64::new(0) }; LEN];
    static CALLS: [AtomicU64; LEN] = [const { AtomicU64::new(0) }; LEN];
    static ROWS: [AtomicU64; LEN] = [const { AtomicU64::new(0) }; LEN];

    pub fn record(phase: Phase, start: Instant, rows: usize) {
        let elapsed = start.elapsed().as_nanos() as u64;
        ELAPSED[phase as usize].fetch_add(elapsed, Relaxed);
        CALLS[phase as usize].fetch_add(1, Relaxed);
        ROWS[phase as usize].fetch_add(rows as u64, Relaxed);
    }

    pub fn time<T>(phase: Phase, rows: usize, operation: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let result = operation();
        record(phase, start, rows);
        result
    }

    pub fn take() -> [(u64, u64, u64); LEN] {
        std::array::from_fn(|phase| (
            ELAPSED[phase].swap(0, Relaxed),
            CALLS[phase].swap(0, Relaxed),
            ROWS[phase].swap(0, Relaxed),
        ))
    }
}
"""


def replace(source, original, modified, count=1):
    assert source.count(original) == count, original
    return source.replace(original, modified)


def add_import(source, line):
    # Keep the imports after any inner module documentation.
    index = source.index("\nuse ") + 1
    return source[:index] + line + source[index:]


def driver(source, instrumented):
    source = replace(source, "Ok(rows) => { results.push", """Ok(rows) => {
                        fs::write(work.join("native.rows.json"), serde_json::to_vec(&rows.0)?)?;
                        results.push""")
    source = replace(source, "        let comparison = native.as_ref()",
                     '        fs::write(work.join(format!("{configuration}.rows.json")), '
                     'serde_json::to_vec(&rows.0)?)?;\n        let comparison = native.as_ref()')
    if instrumented:
        source = add_import(source, "use datafusion_physical_expr::aggregate::dar_aggregate_probe;\n")
        source = replace(source, "drain(context, physical).await?", "profile_query(context, physical).await?", 2)
        source += """

async fn profile_query(context: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Result<(u128, usize)> {
    dar_aggregate_probe::take();
    let result = drain(context, plan).await?;
    let metrics = dar_aggregate_probe::take();
    eprintln!("aggregate-phases {}", serde_json::to_string(&metrics)?);
    Ok(result)
}
"""
    return source


def aggregate_factory(source):
    for constructor in ("self.fun.accumulator(acc_args)", "self.fun.create_sliding_accumulator(args)"):
        source = replace(source, constructor,
                         f"dar_aggregate_probe::time(dar_aggregate_probe::Phase::Create, 0, || {constructor})")
    return source + CLOCKS.replace("PHASES", ", ".join(NAMES.split())).replace("PHASE_COUNT", str(len(PHASES)))


def sliding(source):
    source = add_import(source, EXPR_IMPORT)
    source = replace(source, "*accumulator = self.get_accumulator()?;",
                     """let fresh = self.get_accumulator()?;
                probe::time(Phase::Discard, 0, || *accumulator = fresh);""", 2)
    source = replace(source, "            self.aggregate.evaluate_empty_window(accumulator.as_mut())",
                     "            probe::time(Phase::Evaluate, 0, || self.aggregate.evaluate_empty_window(accumulator.as_mut()))")
    source = replace(source, "            if update_bound > 0 {", """            if update_bound > 0 {
                let update_clock = std::time::Instant::now();""")
    source = replace(source, "                accumulator.update_batch(&update)?",
                     """                accumulator.update_batch(&update)?;
                let phase = if recompute_frame { Phase::Replay } else { Phase::Update };
                probe::record(phase, update_clock, update_bound);""")
    source = replace(source, "            if retract_bound > 0 && !recompute_frame {",
                     """            if retract_bound > 0 && !recompute_frame {
                let retract_clock = std::time::Instant::now();""")
    source = replace(source, "                accumulator.retract_batch(&retract)?",
                     """                accumulator.retract_batch(&retract)?;
                probe::record(Phase::Retract, retract_clock, retract_bound);""")
    return replace(source, "            accumulator.evaluate()",
                   "            probe::time(Phase::Evaluate, 0, || accumulator.evaluate())")


def standard(source):
    source = add_import(source, EXPR_IMPORT)
    source = replace(source, "let mut evaluator = self.expr.create_evaluator()?;",
                     "let mut evaluator = probe::time(Phase::Create, 0, || self.expr.create_evaluator())?;")
    source = replace(source, "let evaluator = self.expr.create_evaluator()?;",
                     "let evaluator = probe::time(Phase::Create, 0, || self.expr.create_evaluator())?;")
    for arguments in ("&values, &range", "&values, &state.window_frame_range"):
        source = replace(source, f"evaluator.evaluate({arguments})",
                         f"probe::time(Phase::Evaluate, 0, || evaluator.evaluate({arguments}))")
    source = replace(source, "evaluator.evaluate_all_with_rank(num_rows, &sort_partition_points)",
                     "probe::time(Phase::Evaluate, num_rows, || evaluator.evaluate_all_with_rank(num_rows, &sort_partition_points))")
    source = replace(source, "evaluator.evaluate_all(&values, num_rows)",
                     "probe::time(Phase::Evaluate, num_rows, || evaluator.evaluate_all(&values, num_rows))")
    source = replace(source, "row_wise_results[0].to_array()",
                     "probe::time(Phase::Materialize, 1, || row_wise_results[0].to_array())")
    return replace(source, "ScalarValue::iter_to_array(row_wise_results)",
                   "probe::time(Phase::Materialize, row_wise_results.len(), || ScalarValue::iter_to_array(row_wise_results))", 2)


def window_output(source):
    return replace(add_import(source, EXPR_IMPORT), "ScalarValue::iter_to_array(row_wise_results)",
                   "probe::time(Phase::Materialize, row_wise_results.len(), || ScalarValue::iter_to_array(row_wise_results))")


def ungrouped_aggregate(source):
    source = add_import(source, PLAN_IMPORT)
    for mode, method, phase in [("Raw", "update_batch", "Update"), ("Partial", "merge_batch", "Merge")]:
        source = replace(source, f"AggregateInputMode::{mode} => accum.{method}(&values),",
                         f"AggregateInputMode::{mode} => probe::time(Phase::{phase}, batch.num_rows(), || accum.{method}(&values)),")
    return source


def aggregate_output(source):
    source = add_import(source, PLAN_IMPORT)
    source = replace(source, "accumulator.evaluate().and_then(|v| v.to_array())",
                     """probe::time(Phase::Evaluate, 0, || accumulator.evaluate())
                        .and_then(|v| probe::time(Phase::Materialize, 1, || v.to_array()))""")
    source = replace(source, "accumulator.state().and_then(|e| {",
                     "probe::time(Phase::State, 0, || accumulator.state()).and_then(|e| {")
    return replace(source, ".map(|v| v.to_array())",
                   ".map(|v| probe::time(Phase::Materialize, 1, || v.to_array()))")


def scalar_conversion(source):
    source = add_import(source, EXPR_IMPORT)
    source = replace(source, "        let mut output = if retain_row_errors {",
                     """        let conversion_clock = matches!(
            self.name.as_str(), "array_to_string" | "array_join" | "list_to_string" | "list_join"
        ).then(std::time::Instant::now);
        let mut output = if retain_row_errors {""")
    return replace(source, "        let rows = output\n",
                   """        if let Some(clock) = conversion_clock {
            probe::record(Phase::Convert, clock, batch.num_rows());
        }
        let rows = output
""")


def cast_conversion(source):
    source = add_import(source, EXPR_IMPORT)
    return replace(source, "        value.cast_to(self.cast_type(), Some(&self.cast_options))\n",
                   """        let cast = || value.cast_to(self.cast_type(), Some(&self.cast_options));
        if self.cast_type() == &Utf8 && value.data_type() == LargeUtf8 {
            probe::time(Phase::Convert, batch.num_rows(), cast)
        } else {
            cast()
        }
""")


def exact_float_rows(source):
    for width in (8, 16):
        old = f"""            json!(if value.is_nan() {{
                "nan".into()
            }} else {{
                format!("{{:0{width}x}}", value.to_bits())
            }})"""
        new = f'            json!(format!("{{:0{width}x}}", value.to_bits()))'
        source = replace(source, old, new)
    return source


def changes(workspace, instrumented):
    original = workspace / "benches/functions.rs"
    yield original, driver(original.read_text(), instrumented)
    encoder = workspace / "tests/support/spark_results.rs"
    yield encoder, exact_float_rows(encoder.read_text())
    if not instrumented:
        return
    edits = [
        (EXPR / "aggregate.rs", aggregate_factory),
        (EXPR / "window/sliding_aggregate.rs", sliding),
        (EXPR / "window/standard.rs", standard),
        (EXPR / "window/window_expr.rs", window_output),
        (EXPR / "scalar_function.rs", scalar_conversion),
        (EXPR / "expressions/cast.rs", cast_conversion),
        (PLAN / "no_grouping.rs", ungrouped_aggregate),
        (PLAN / "mod.rs", aggregate_output),
    ]
    for path, instrument in edits:
        original = workspace / path
        yield original, instrument(original.read_text())
