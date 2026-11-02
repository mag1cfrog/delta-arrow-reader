//! Spark setup preserves native constant-group plans, errors, schemas and float bits.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Float32Array, Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::Value;

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_result};

#[derive(Debug, PartialEq, Eq)]
struct NativeSnapshot {
    schema: SchemaRef,
    rows: Vec<Vec<Option<String>>>,
    logical_plan: String,
    physical_plan: String,
}

async fn capture_native(session: &SessionContext, sql: &str) -> TestResult<NativeSnapshot> {
    let frame = session.sql(sql).await?;
    let logical_plan = frame
        .clone()
        .into_optimized_plan()?
        .display_indent()
        .to_string();
    let physical = frame.create_physical_plan().await?;
    let schema = physical.schema();
    let physical_plan = displayable(physical.as_ref()).indent(true).to_string();
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(row) {
                            return Ok(None);
                        }
                        Ok(Some(match column.data_type() {
                            DataType::Float32 => format!(
                                "{:08x}",
                                column.as_primitive::<Float32Type>().value(row).to_bits()
                            ),
                            DataType::Float64 => format!(
                                "{:016x}",
                                column.as_primitive::<Float64Type>().value(row).to_bits()
                            ),
                            _ => array_value_to_string(column.as_ref(), row)?,
                        }))
                    })
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort();
    Ok(NativeSnapshot {
        schema,
        rows,
        logical_plan,
        physical_plan,
    })
}

#[tokio::test]
async fn spark_setup_preserves_native_constant_groups_before_and_after_spark() -> TestResult {
    let floats: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(f32::from_bits(0xffc04321)),
        Some(f32::from_bits(0x7fc01234)),
        Some(f32::NEG_INFINITY),
        Some(f32::INFINITY),
        None,
    ]));
    let doubles: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(f64::from_bits(0xfff8000000004321)),
        Some(f64::from_bits(0x7ff8000000001234)),
        Some(f64::NEG_INFINITY),
        Some(f64::INFINITY),
        None,
    ]));
    let mut differences = Vec::new();
    for values in [floats, doubles] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("k", values.data_type().clone(), true)
                .with_metadata([("source".to_owned(), "raw floating bits".to_owned())].into()),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..7)), values],
        )?;
        for batch_size in [1, 32] {
            for ansi_mode in [true, false] {
                let config = SessionConfig::new()
                    .with_batch_size(batch_size)
                    .with_target_partitions(2);
                let native = SessionContext::new_with_state(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(config.clone())
                        .build(),
                );
                let configured = SessionContext::new_with_state(
                    configure_spark_sql(
                        SessionStateBuilder::new()
                            .with_default_features()
                            .with_config(config),
                    )
                    .build(),
                );
                let batches = (0..batch.num_rows())
                    .step_by(batch_size)
                    .map(|start| batch.slice(start, batch_size.min(batch.num_rows() - start)))
                    .collect();
                let provider = Arc::new(MemTable::try_new(schema.clone(), vec![batches])?);
                native.register_table("inputs", provider.clone())?;
                configured.register_table("inputs", provider)?;
                for after_spark in [false, true] {
                    if after_spark {
                        let query = plan_query(&configured,
                            "SELECT MIN(k) AS low, MAX(k) AS high FROM inputs GROUP BY CAST(1 AS INT)",
                            &SparkSqlOptions { ansi_mode, ..Default::default() }).await?;
                        collect(
                            query.create_physical_plan(&configured).await?,
                            configured.task_ctx(),
                        )
                        .await?;
                    }
                    for sql in [
                        "SELECT k FROM inputs ORDER BY id",
                        "SELECT MIN(k) AS low, MAX(k) AS high FROM inputs",
                        "SELECT MIN(k) AS low, MAX(k) AS high FROM inputs GROUP BY CAST(1 AS INT)",
                        "SELECT MIN(k) AS low, MAX(k) AS high FROM inputs GROUP BY id % 2",
                        "SELECT MIN(k) AS low, MAX(k) AS high FROM inputs GROUP BY CAST(1 AS INT), id % 2",
                        "SELECT COUNT(*) AS n, SUM(id) AS s FROM inputs GROUP BY CAST(1 AS INT)",
                        "SELECT COUNT(*) AS n FROM inputs WHERE false GROUP BY CAST(1 AS INT)",
                        "SELECT low FROM (SELECT MIN(k) AS low FROM inputs GROUP BY CAST(1 AS INT)) g",
                        "SELECT (SELECT MIN(k) FROM inputs GROUP BY CAST(1 AS INT)) AS low",
                        "SELECT EXISTS(SELECT MIN(k) FROM inputs GROUP BY CAST(1 AS INT)) AS present",
                        "SELECT COUNT(*) AS n FROM inputs GROUP BY CAST('bad' AS INT)",
                        "SELECT CAST('bad' AS INT) AS bad, COUNT(*) AS n FROM inputs GROUP BY CAST('bad' AS INT)",
                    ] {
                        let expected = capture_native(&native, sql)
                            .await
                            .map_err(|error| error.to_string());
                        let actual = capture_native(&configured, sql)
                            .await
                            .map_err(|error| error.to_string());
                        if sql.contains("'bad'") {
                            assert!(
                                expected.is_err(),
                                "native CAST error control succeeded: {sql}"
                            );
                        } else {
                            assert!(
                                expected.is_ok(),
                                "native control failed: {sql}: {expected:?}"
                            );
                        }
                        if actual != expected {
                            differences.push(format!("{:?} ANSI={ansi_mode} batch={batch_size} after_spark={after_spark} {sql}: expected {expected:?}, got {actual:?}", schema.field(1).data_type()));
                        }
                    }
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_group_preparation_keeps_recorded_empty_groups_and_cast_errors() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let mut checked = 0;
    for line in include_str!("fixtures/array-contains-spark.jsonl")
        .lines()
        .skip(1)
    {
        let case: Value = serde_json::from_str(line)?;
        if !matches!(
            case["id"].as_str(),
            Some(
                "projection_grouping_null_lookup"
                    | "projection_grouping_empty_null_lookup"
                    | "projection_grouping_null_array"
                    | "projection_grouping_empty_null_array"
                    | "projection_cast_needed_elsewhere"
                    | "projection_subquery_cast"
            )
        ) {
            continue;
        }
        if case["expected"]["condition"] == "CAST_INVALID_INPUT" {
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    session_timezone: "UTC".into(),
                    ..Default::default()
                },
            )
            .await?;
            let actual = async {
                let physical = query.create_physical_plan(&session).await?;
                collect(physical, session.task_ctx()).await
            }
            .await;
            assert!(
                actual.as_ref().is_err_and(|error| {
                    let DataFusionError::ArrowError(error, _) = error.find_root() else {
                        return false;
                    };
                    matches!(error.as_ref(), ArrowError::CastError(message)
                        if message == "Cannot cast string 'bad' to value of Int32 type")
                }),
                "{case}: {actual:?}"
            );
        } else {
            let actual = capture_spark_query_result(&session, &case).await;
            let mut expected = case["expected"].clone();
            expected
                .as_object_mut()
                .ok_or("missing expected result")?
                .remove("status");
            for row in expected["rows"].as_array_mut().ok_or("missing rows")? {
                for cell in row.as_array_mut().ok_or("missing row")? {
                    if cell.is_number() {
                        *cell = Value::String(cell.to_string());
                    }
                }
            }
            assert_eq!(actual?, expected, "{case}");
        }
        checked += 1;
    }
    assert_eq!(checked, 12);
    Ok(())
}
