//! Sliding extrema retain the original winning value, including ties and NULLs.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

const WINDOWS: &[(&str, &str, &str)] = &[
    (
        "sliding_rows",
        "",
        "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    ),
    (
        "sliding_range",
        "",
        "RANGE BETWEEN 4 PRECEDING AND CURRENT ROW",
    ),
    (
        "growing",
        "",
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    ),
    (
        "partition",
        "",
        "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    ),
    (
        "earlier_empty",
        "",
        "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING",
    ),
    (
        "later_empty",
        "",
        "ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING",
    ),
    (
        "filtered",
        "FILTER (WHERE keep)",
        "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    ),
];

fn extrema_session() -> Result<SessionContext> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(1)
                        .with_batch_size(4),
                ),
        )
        .build(),
    );
    let id: ArrayRef = Arc::new(Int64Array::from(vec![
        0, 1, 3, 4, 8, 9, 10, 11, 16, 17, 18, 20,
    ]));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(100),
        None,
        Some(30),
        Some(20),
        Some(90),
        Some(i64::MAX),
        Some(-7),
        Some(i64::MIN),
        Some(8),
        None,
        Some(40),
        Some(50),
    ]));
    let keys: ArrayRef = Arc::new(Int64Array::from(vec![
        None,
        Some(i64::MAX),
        Some(i64::MAX),
        Some(i64::MIN),
        Some(3),
        None,
        Some(3),
        Some(3),
        Some(-4),
        Some(-4),
        Some(1),
        None,
    ]));
    let groups: ArrayRef = Arc::new(Int64Array::from(vec![0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1]));
    let keep: ArrayRef = Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(true),
        Some(true),
        Some(false),
        Some(true),
        Some(false),
        Some(true),
        Some(true),
        None,
    ]));
    let batch = RecordBatch::try_from_iter([
        ("id", id),
        ("v", values),
        ("k", keys),
        ("g", groups),
        ("keep", keep),
    ])?;
    let input = MemTable::try_new(
        batch.schema(),
        vec![vec![
            batch.slice(0, 4),
            batch.slice(4, 4),
            batch.slice(8, 4),
        ]],
    )?;
    session.register_table("samples", Arc::new(input))?;
    Ok(session)
}

async fn execute_extrema(session: &SessionContext, sql: &str, ansi_mode: bool) -> Result<Value> {
    let logical = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        },
    )
    .await?;
    let plan = logical.create_physical_plan(session).await?;
    let types: Vec<_> = plan
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{:?}", field.data_type()))
        .collect();
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        let executed_types: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| format!("{:?}", field.data_type()))
            .collect();
        assert_eq!(types, executed_types, "{sql}");
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .map(|column| {
                    let value = ScalarValue::try_from_array(column, row)?;
                    Ok(if value.is_null() {
                        Value::Null
                    } else {
                        json!(value.to_string())
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(values);
        }
    }
    Ok(json!({"types":types,"rows":rows}))
}

fn assert_staging(observations: Vec<Value>) -> Result<()> {
    let reference: Value =
        serde_json::from_str(include_str!("fixtures/sliding-max-min-by/staging.json"))
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
    assert_eq!(Value::Array(observations), reference["observations"]);
    Ok(())
}

#[tokio::test]
async fn sliding_extrema_match_staging_in_both_ansi_modes() -> Result<()> {
    let session = extrema_session()?;
    let mut observations = Vec::new();
    for ansi_mode in [false, true] {
        for (id, filter, frame) in WINDOWS {
            let window = format!("OVER (PARTITION BY g ORDER BY id {frame})");
            let sql = format!(
                "SELECT id, max_by(v,k) {filter} {window}, min_by(v,k) {filter} {window} FROM samples ORDER BY id"
            );
            let result = execute_extrema(&session, &sql, ansi_mode).await?;
            observations.push(json!({"id":id,"ansi_mode":ansi_mode,"result":result}));
        }
        for (id, sql) in [
            ("plain", "SELECT max_by(v,k), min_by(v,k) FROM samples"),
            (
                "non_bigint_value",
                "SELECT id, max_by(CAST(v % 127 AS INT),k) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), min_by(CAST(v % 127 AS INT),k) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
            ),
            (
                "non_bigint_key",
                "SELECT id, max_by(v,CAST(k % 127 AS INT)) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), min_by(v,CAST(k % 127 AS INT)) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
            ),
            (
                "empty_plain",
                "SELECT max_by(v,k), min_by(v,k) FROM samples WHERE false",
            ),
            (
                "empty_window",
                "SELECT max_by(v,k) OVER w, min_by(v,k) OVER w FROM samples WHERE false WINDOW w AS (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)",
            ),
        ] {
            let result = execute_extrema(&session, sql, ansi_mode).await?;
            observations.push(json!({"id":id,"ansi_mode":ansi_mode,"result":result}));
        }
    }
    assert_staging(observations)
}

#[tokio::test]
async fn min_by_required_key_matches_the_filtered_reducer() -> Result<()> {
    let session = extrema_session()?;
    for ansi_mode in [false, true] {
        for (key, suffix) in [
            ("id", ""),
            ("id", " WHERE false"),
            ("k", ""),
            ("k", " WHERE k IS NULL"),
            ("CAST(k AS BIGINT)", ""),
            ("id", " GROUP BY g ORDER BY g"),
            ("required_key", ""),
        ] {
            let source = if key == "required_key" {
                "(SELECT v, COALESCE(k, 0) AS required_key FROM samples) AS required_keys"
            } else {
                "samples"
            };
            let actual = format!("SELECT min_by(v,{key}) FROM {source}{suffix}");
            let filtered =
                format!("SELECT min_by(v,{key}) FILTER (WHERE true) FROM {source}{suffix}");
            assert_eq!(
                execute_extrema(&session, &actual, ansi_mode).await?,
                execute_extrema(&session, &filtered, ansi_mode).await?,
                "{actual}, ANSI={ansi_mode}",
            );
        }
    }
    Ok(())
}
