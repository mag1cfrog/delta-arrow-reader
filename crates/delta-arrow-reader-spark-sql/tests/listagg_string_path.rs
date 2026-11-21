//! Direct string aggregation preserves staged results and keeps other forms on the list path.

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::datasource::MemTable;
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
        "plain_groups_and_filter",
        "SELECT g, listagg(s, '|') FILTER (WHERE keep) FROM grouped_samples GROUP BY g ORDER BY g",
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
    ("plain_missing_value", "SELECT listagg()", false),
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
    let groups: ArrayRef = Arc::new(Int64Array::from(vec![0, 1, 2]));
    let strings: ArrayRef = Arc::new(StringArray::from(vec!["", "filtered", "repeat"]));
    let keep: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
    let group_batch = RecordBatch::try_from_iter([("g", groups), ("s", strings), ("keep", keep)])?;
    let grouped = MemTable::try_new(
        group_batch.schema(),
        vec![vec![group_batch.clone()], vec![group_batch]],
    )?;
    session.register_table("grouped_samples", Arc::new(grouped))?;
    let long_input: ArrayRef = Arc::new(StringArray::from_iter(
        std::iter::once(Some("kept")).chain(std::iter::repeat_n(None, 8191)),
    ));
    session.register_batch(
        "long_input",
        RecordBatch::try_from_iter([("s", long_input)])?,
    )?;
    Ok(session)
}

async fn observe(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
    direct: bool,
    case_id: &str,
) -> Result<Value> {
    let options = SparkSqlOptions {
        ansi_mode,
        ..Default::default()
    };
    let logical = match plan_query(session, sql, &options).await {
        Ok(plan) => plan,
        Err(error) => return Ok(json!({ "error": error.to_string() })),
    };
    let resolved = logical.logical_plan.display_indent().to_string();
    let physical = match logical.create_physical_plan(session).await {
        Ok(plan) => plan,
        Err(error) => return Ok(json!({ "error": error.to_string() })),
    };
    let plan = displayable(physical.as_ref())
        .indent(true)
        .to_string()
        .to_ascii_lowercase();
    check_string_path(&resolved, &plan, direct, case_id);
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
                assert_eq!(types, actual_types, "{case_id}");
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

fn check_string_path(logical: &str, physical: &str, direct: bool, case_id: &str) {
    let uses_string_aggregate =
        physical.contains("aggr=[string_agg(") || physical.contains("wdw=[string_agg(");
    if direct {
        assert!(
            uses_string_aggregate,
            "{case_id}: expected the direct accumulator"
        );
        assert!(
            physical.contains("utf8view("),
            "{case_id}: expected a Utf8View delimiter"
        );
        assert!(
            !logical.contains("array_to_string("),
            "{case_id}: list lowering still present"
        );
    } else {
        assert!(
            !uses_string_aggregate,
            "{case_id}: direct accumulator used on fallback"
        );
        assert!(
            logical.contains("array_to_string("),
            "{case_id}: expected the list lowering"
        );
    }
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
            let result = observe(&session, sql, ansi_mode, *direct, id).await?;
            if *direct {
                assert!(result.get("rows").is_some(), "{id}: {result}");
            }
            observations.push(json!({ "id": id, "ansi_mode": ansi_mode, "result": result }));
        }
    }
    // An offset-based array would try to repeat 2 GiB of delimiter bytes.
    let delimiter = "x".repeat(262_144);
    let sql = format!("SELECT listagg(s, '{delimiter}') FROM long_input");
    for ansi_mode in [false, true] {
        let id = "plain_long_delimiter";
        let result = observe(&session, &sql, ansi_mode, true, id).await?;
        assert_eq!(result, json!({"types":["Utf8"],"rows":[["kept"]]}));
        observations.push(json!({ "id": id, "ansi_mode": ansi_mode, "result": result }));
    }
    check_staging(observations)
}
