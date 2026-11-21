//! Compare the logarithm validity mask with the original CASE.

use arrow::array::{ArrayRef, Float64Array};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;

use super::{
    Arc, AsArray, DataType, Float64Type, SessionConfig, SessionContext, SessionStateBuilder,
    SparkSqlOptions, collect, configure_spark_sql, plan_query,
};

#[tokio::test]
async fn logarithm_domain_matches_case_with_exact_float_bits() -> Result<()> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(1)),
        )
        .build(),
    );
    let numbers = [
        None,
        Some(-0.0),
        Some(0.0),
        Some(-1.0),
        Some(-0.5),
        Some(1.0),
        Some(2.0),
        Some(f64::MIN),
        Some(f64::MAX),
        Some(f64::from_bits(1)),
        Some(-f64::from_bits(1)),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::from_bits(0x7ff8_0000_0000_0001)),
        Some(f64::from_bits(0xfff8_0000_0000_0001)),
        Some(f64::from_bits(0x7ff0_0000_0000_0001)),
        Some(f64::from_bits(0xfff0_0000_0000_0001)),
    ];
    let bases = [
        Some(2.0),
        None,
        Some(10.0),
        Some(-1.0),
        Some(0.0),
        Some(1.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
    ];
    let values = numbers
        .into_iter()
        .flat_map(|number| std::iter::repeat_n(number, bases.len()))
        .collect::<Vec<_>>();
    let base_values = bases
        .into_iter()
        .cycle()
        .take(values.len())
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter([
        ("d", Arc::new(Float64Array::from(values)) as ArrayRef),
        (
            "base",
            Arc::new(Float64Array::from(base_values)) as ArrayRef,
        ),
    ])?;
    let inputs = [
        ("log_values", batch.clone()),
        ("log_sliced", batch.slice(1, batch.num_rows() - 2)),
        (
            "log_positive",
            batch.slice(5 * bases.len(), 2 * bases.len()),
        ),
        ("log_empty", batch.slice(0, 0)),
    ];
    for (name, batch) in inputs {
        session.register_batch(name, batch)?;
    }
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        };
        for table in ["log_values", "log_sliced", "log_positive", "log_empty"] {
            let mut results = Vec::new();
            for (arg, uses_mask) in [
                ("d", true),
                ("CASE WHEN d IS NULL THEN d ELSE d END", false),
            ] {
                let sql = format!(
                    "SELECT ln({arg}) AS ln, log(base, {arg}) AS log, log10({arg}) AS log10, \
                     log2({arg}) AS log2, log1p({arg}) AS log1p FROM {table}"
                );
                let query = plan_query(&session, &sql, &options).await?;
                assert_eq!(
                    query
                        .logical_plan
                        .display_indent()
                        .to_string()
                        .contains("spark_positive_double_or_null("),
                    uses_mask,
                    "{sql}; ANSI={ansi_mode}",
                );
                let plan = query.create_physical_plan(&session).await?;
                let schema = plan.schema();
                assert!(
                    schema
                        .fields()
                        .iter()
                        .all(|field| field.data_type() == &DataType::Float64)
                );
                let batches = collect(plan, session.task_ctx()).await?;
                let batch = concat_batches(&schema, &batches)?;
                let columns = batch
                    .columns()
                    .iter()
                    .map(|values| {
                        values
                            .as_primitive::<Float64Type>()
                            .iter()
                            .map(|value| value.map(f64::to_bits))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                results.push((schema, columns));
            }
            assert_eq!(results[0], results[1], "{table}; ANSI={ansi_mode}");
        }
    }
    Ok(())
}
