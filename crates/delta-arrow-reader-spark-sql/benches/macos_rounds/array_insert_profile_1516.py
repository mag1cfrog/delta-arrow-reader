"""Attribute the length and CASE work around literal ARRAY_INSERT."""

from statistics import median
import sys
from unittest.mock import patch

sys.dont_write_bytecode = True
import percentile_profile_1516 as profile

EXTRA = {
    **profile.EXTRA,
    "case_total": "CaseTotal",
    "case_predicates": "CasePredicates",
    "array_length": "ArrayLength",
    "insert_copy": "InsertCopy",
    "native_slice": "NativeSlice",
    "native_concat": "NativeConcat",
}
base_overlay = profile.overlay


def overlay(workspace, targets):
    instrumented, phases = base_overlay(workspace, targets)
    expr = workspace / "vendor/dependencies/datafusion-physical-expr/src"
    case = expr / "expressions/case.rs"
    scalar = expr / "scalar_function.rs"
    retained = ("benches/functions.rs", "tests/support/spark_results.rs")
    paths = {workspace / name: instrumented[workspace / name] for name in retained}
    paths[expr / "aggregate.rs"] = instrumented[expr / "aggregate.rs"]
    replace = profile.frozen.mac_build.replace

    text = case.read_text()
    import_at = text.index("\nuse ") + 1
    text = text[:import_at] + "use crate::aggregate::dar_aggregate_probe::{self as probe, Phase};\n" + text[import_at:]
    before = """            let when_value = when_predicate
                .evaluate(&remainder_batch)?
                .into_array(remainder_batch.num_rows())?;"""
    after = """            let rows = remainder_batch.num_rows();
            let when_value = probe::time(Phase::CasePredicates, rows, || {
                when_predicate.evaluate(&remainder_batch)?.into_array(rows)
            })?;"""
    text = replace(text, before, after)
    text = replace(
        text,
        """    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match &self.eval_method {""",
        """    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let started = std::time::Instant::now();
        let result = match &self.eval_method {""",
    )
    text = replace(
        text,
        """            EvalMethod::WithExprScalarLookupTable(lookup_table) => {
                self.with_lookup_table(batch, lookup_table)
            }
        }
    }

    fn children""",
        """            EvalMethod::WithExprScalarLookupTable(lookup_table) => {
                self.with_lookup_table(batch, lookup_table)
            }
        };
        probe::record(Phase::CaseTotal, started, batch.num_rows());
        result
    }

    fn children""",
    )
    paths[case] = text

    text = scalar.read_text()
    import_at = text.index("\nuse ") + 1
    text = text[:import_at] + "use crate::aggregate::dar_aggregate_probe::{self as probe, Phase};\n" + text[import_at:]
    text = replace(
        text,
        """        let mut output = if retain_row_errors {""",
        """        let phase = match self.name.as_str() {
            "array_length" => Some(Phase::ArrayLength),
            "spark_insert_bigint_at_second_position" => Some(Phase::InsertCopy),
            "array_slice" => Some(Phase::NativeSlice),
            "array_concat" => Some(Phase::NativeConcat),
            _ => None,
        };
        let started = phase.map(|phase| (phase, std::time::Instant::now()));
        let mut output = if retain_row_errors {""",
    )
    paths[scalar] = replace(
        text,
        """        let rows = output
""",
        """        if let Some((phase, started)) = started {
            probe::record(phase, started, batch.num_rows());
        }
        let rows = output
""",
    )
    return paths, phases


def phase_result(records, rows):
    result = {name: median(record[name][0] / rows for record in records) for name in records[0]}
    result["case_remainder"] = median(
        (record["case_total"][0] - record["case_predicates"][0] - record["insert_copy"][0]) / rows
        for record in records
    )
    return result


if __name__ == "__main__":
    with patch.multiple(profile, EXTRA=EXTRA, overlay=overlay, phase_result=phase_result, __file__=__file__):
        profile.main()
