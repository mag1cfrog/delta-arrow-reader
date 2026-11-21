//! Direct string aggregation preserves staged results and keeps other forms on the list path.

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

const CASES: &[(&str, &str, bool)] = &[
    ("plain_literal", "SELECT listagg(s, '|') FROM samples", true),
    ("plain_default", "SELECT listagg(s) FROM samples", true),
    (
        "plain_empty_delimiter",
        "SELECT string_agg(s, '') FROM samples",
        true,
    ),
    (
        "plain_filtered",
        "SELECT string_agg(s, '-') FILTER (WHERE id < 4) FROM samples",
        true,
    ),
    (
        "plain_empty_input",
        "SELECT listagg(s, '#') FROM samples WHERE id < 0",
        true,
    ),
    (
        "plain_all_null",
        "SELECT string_agg(s, '|') FROM samples WHERE id = 0",
        true,
    ),
    (
        "plain_distinct",
        "SELECT listagg(DISTINCT s, ',') FROM samples",
        false,
    ),
    (
        "plain_ordered",
        "SELECT listagg(s, ',') WITHIN GROUP (ORDER BY id DESC) FROM samples",
        false,
    ),
    (
        "plain_null_delimiter",
        "SELECT listagg(s, NULL) FROM samples",
        false,
    ),
    ("plain_binary", "SELECT listagg(b, ',') FROM samples", false),
    (
        "window_listagg",
        "SELECT id, listagg(s, '|') OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
        true,
    ),
    (
        "window_string_agg",
        "SELECT id, string_agg(s, '') OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
        true,
    ),
    (
        "window_array_join",
        "SELECT id, array_join(s, '·') OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
        true,
    ),
    (
        "window_range",
        "SELECT id, listagg(s, '|') OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
        false,
    ),
    (
        "window_growing",
        "SELECT id, listagg(s, '|') OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
        false,
    ),
    (
        "window_full",
        "SELECT id, listagg(s, '|') OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM samples ORDER BY id",
        false,
    ),
    (
        "window_empty_frame",
        "SELECT id, listagg(s, '|') OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM samples ORDER BY id",
        false,
    ),
    (
        "window_dynamic_delimiter",
        "SELECT id, listagg(s, sep) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM samples ORDER BY id",
        false,
    ),
];

fn make_session() -> Result<SessionContext> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(1)),
        )
        .build(),
    );
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(0..8));
    let groups: ArrayRef = Arc::new(Int64Array::from(vec![0, 0, 0, 0, 0, 1, 1, 1]));
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        None,
        Some(""),
        Some("é"),
        Some("a,b"),
        Some("é"),
        None,
        None,
        Some("tail"),
    ]));
    let separators: ArrayRef = Arc::new(StringArray::from(vec![
        Some("/"),
        Some(""),
        None,
        Some(":"),
        Some("|"),
        Some("-"),
        None,
        Some(";"),
    ]));
    let bytes: ArrayRef = Arc::new(BinaryArray::from_vec(vec![
        b"a", b"", b"b", b"c", b"d", b"e", b"f", b"g",
    ]));
    session.register_batch(
        "samples",
        RecordBatch::try_from_iter([
            ("id", id),
            ("g", groups),
            ("s", strings),
            ("sep", separators),
            ("b", bytes),
        ])?,
    )?;
    Ok(session)
}

async fn observe(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
    direct: bool,
) -> Result<Value> {
    let options = SparkSqlOptions {
        ansi_mode,
        ..Default::default()
    };
    let logical = match plan_query(session, sql, &options).await {
        Ok(plan) => plan,
        Err(error) => return Ok(json!({ "error": error.to_string() })),
    };
    let physical = match logical.create_physical_plan(session).await {
        Ok(plan) => plan,
        Err(error) => return Ok(json!({ "error": error.to_string() })),
    };
    let plan = displayable(physical.as_ref())
        .indent(true)
        .to_string()
        .to_ascii_lowercase();
    check_string_path(&plan, direct, sql);
    let types: Vec<_> = physical
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{:?}", field.data_type()))
        .collect();
    let mut rows = Vec::new();
    match collect(physical, session.task_ctx()).await {
        Ok(batches) => {
            for batch in batches {
                let actual_types: Vec<_> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| format!("{:?}", field.data_type()))
                    .collect();
                assert_eq!(types, actual_types, "{sql}");
                for row in 0..batch.num_rows() {
                    let values = batch
                        .columns()
                        .iter()
                        .map(|column| {
                            Ok(match ScalarValue::try_from_array(column, row)? {
                                value if value.is_null() => Value::Null,
                                ScalarValue::Binary(Some(bytes)) => json!(bytes),
                                value => json!(value.to_string()),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    rows.push(values);
                }
            }
        }
        Err(error) => return Ok(json!({ "error": error.to_string() })),
    }
    Ok(json!({ "types": types, "rows": rows }))
}

fn check_string_path(plan: &str, direct: bool, sql: &str) {
    assert_eq!(
        plan.contains("string_agg(") && !plan.contains("array_to_string("),
        direct,
        "{sql}\n{plan}",
    );
}

fn check_staging(observations: Vec<Value>) -> Result<()> {
    let reference: Value =
        serde_json::from_str(include_str!("fixtures/listagg-string-path/staging.json"))
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
    assert_eq!(Value::Array(observations), reference["observations"]);
    Ok(())
}

#[tokio::test]
async fn direct_listagg_matches_staging_in_both_ansi_modes() -> Result<()> {
    let session = make_session()?;
    let mut observations = Vec::new();
    for (id, sql, direct) in CASES {
        for ansi_mode in [false, true] {
            let result = observe(&session, sql, ansi_mode, *direct).await?;
            if *direct {
                assert!(result.get("rows").is_some(), "{id}: {result}");
            }
            observations.push(json!({ "id": id, "ansi_mode": ansi_mode, "result": result }));
        }
    }
    check_staging(observations)
}
