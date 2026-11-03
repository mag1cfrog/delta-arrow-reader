//! Integer-day DATE arithmetic preserves the entire Date32 representation.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Date32Type, Field};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::register_recorded_inputs;

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

fn date_metadata_stages() -> TestResult<Vec<Value>> {
    let records = include_str!(
        "fixtures/date-day-metadata-stages/date-day-metadata-stages-spark.jsonl"
    )
    .lines()
    .map(serde_json::from_str)
    .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len() - 1, 92);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert(case["original_observation"].to_string()));
        assert_eq!(case["original_matches"], true);
        for key in ["id", "sql", "ansi", "expected"] {
            assert_eq!(case[key], case["original_observation"][key]);
        }
        if let Some(after) = case.get("executed_schema_after_collect") {
            assert_eq!(case["executed_schema_before_collect"], *after);
        }
    }
    Ok(records)
}

pub(crate) async fn capture_date_result(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    let ansi_mode = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            session_timezone: "UTC".into(),
            ..Default::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let physical_schema = physical.schema();
    assert_eq!(query.column_names, ["r"]);
    assert_eq!(physical_schema.fields().len(), 1);
    let physical_field = physical_schema.field(0);
    let resolved_field = query.logical_plan.schema().field(0);
    assert_eq!(physical_field.name(), &query.column_names[0]);
    assert_eq!(resolved_field.data_type(), physical_field.data_type());
    if let Some(public) = case.get("public_schema") {
        assert_eq!(json!(resolved_field.metadata()), public["fields"][0]["metadata"]);
    }
    if let Some(executed) = case
        .get("executed_schema_after_collect")
        .or_else(|| case.get("executed_schema_before_collect"))
    {
        assert_eq!(
            json!(physical_field.metadata()),
            executed["fields"][0]["metadata"],
            "recorded date executed metadata"
        );
    } else {
        assert_eq!(resolved_field.metadata(), physical_field.metadata());
    }
    // Folding can narrow a physical field while the analyzed declaration stays nullable.
    assert!(!physical_field.is_nullable() || resolved_field.is_nullable());
    let physical_control = matches!(
        case["id"].as_str(),
        Some(
            "month_end"
                | "year_interval"
                | "day_interval"
                | "timestamp_day"
                | "timestamp_month"
                | "dateadd_three"
                | "date_add_three"
        )
    );
    let comparison_field = if physical_control {
        assert_eq!(resolved_field.data_type(), &DataType::Utf8);
        // These unchanged controls have conservative resolved fields before folding.
        assert!(resolved_field.is_nullable());
        physical_field
    } else {
        assert!(
            resolved_field.data_type() == &DataType::Date32
                || matches!(resolved_field.data_type(), DataType::List(child) if child.data_type() == &DataType::Date32)
        );
        resolved_field
    };
    let mut field = capture_field(comparison_field)?;
    field["name"] = json!(query.column_names[0]);
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        assert_eq!(batch.schema(), physical_schema);
        let column = batch.column(0);
        assert!(physical_field.is_nullable() || column.null_count() == 0);
        for row in 0..batch.num_rows() {
            if column.is_null(row) {
                assert!(resolved_field.is_nullable());
            }
            let value = capture_value(column.as_ref(), row)?;
            rows.push(vec![value]);
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({
        "resolved_schema":{"type":"struct","fields":[capture_field(resolved_field)?],
            "metadata":query.logical_plan.schema().metadata()},
        "physical_schema":{"type":"struct","fields":[capture_field(physical_field)?],
            "metadata":physical_schema.metadata()},
        "resolved_arrow_schema":format!("{:?}", query.logical_plan.schema()),
        "physical_arrow_schema":format!("{physical_schema:?}"),
        "comparison_stage":if physical_control { "physical" } else { "resolved" },
        "compared":{"schema":{"type":"struct","fields":[field]},"rows":rows}
    }))
}

fn capture_field(field: &Field) -> TestResult<Value> {
    Ok(
        json!({"name":field.name(),"type":capture_type(field.data_type())?,
        "nullable":field.is_nullable(),"metadata":field.metadata()}),
    )
}

fn capture_type(data_type: &DataType) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Date32 => json!("date"),
        DataType::Utf8 => json!("string"),
        DataType::List(child) => json!({"type":"array",
            "elementType":capture_type(child.data_type())?,"containsNull":child.is_nullable()}),
        other => return Err(format!("unexpected result type: {other}").into()),
    })
}

fn capture_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        // Read stored days directly, including dates outside calendar ranges.
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::List(child) => {
            let values = column.as_list::<i32>().value(row);
            assert!(child.is_nullable() || values.null_count() == 0);
            json!(
                (0..values.len())
                    .map(|i| capture_value(values.as_ref(), i))
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
        other => return Err(format!("unexpected result type: {other}").into()),
    })
}

#[tokio::test]
async fn date_day_arithmetic_matches_spark() -> TestResult {
    let stages = date_metadata_stages()?;
    let mut stage_comparisons = 0;
    let mut differences = Vec::new();
    for (capture, count, encoding) in [
        (
            include_str!("fixtures/date-day-arithmetic-spark.jsonl"),
            296,
            "unix_date",
        ),
        (
            include_str!("fixtures/date-day-admission-probes-spark.jsonl"),
            68,
            "unix_date",
        ),
        (
            include_str!("fixtures/date-day-correlated-spark.jsonl"),
            24,
            "unix_date",
        ),
        (
            include_str!("fixtures/date-day-lambda-spark.jsonl"),
            32,
            "unix_date_array",
        ),
        (
            include_str!("fixtures/date-day-closed-lambda-spark.jsonl"),
            12,
            "unix_date",
        ),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records[0]["timezone"], "UTC");
        assert_eq!(records[0]["date_encoding"], encoding);
        assert_eq!(records.len() - 1, count);
        for batch_size in [1, 32] {
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(
                            SessionConfig::new()
                                .with_batch_size(batch_size)
                                .with_target_partitions(2),
                        ),
                )
                .build(),
            );
            if records[0].get("inputs").is_some() {
                register_recorded_inputs(&session, &records[0], batch_size)?;
            }
            for case in &records[1..] {
                let stage = stages[1..]
                    .iter()
                    .find(|stage| stage["original_observation"] == *case);
                stage_comparisons += usize::from(stage.is_some());
                let actual = capture_date_result(&session, stage.unwrap_or(case)).await;
                eprintln!(
                    "date-day observation: {}",
                    json!({
                        "id":case["id"],"ansi":case["ansi"],"batch_size":batch_size,
                        "expected":case,
                        "outcome":match &actual {
                            Ok(value) => value.clone(),
                            Err(error) => json!({"error":error.to_string()}),
                        }
                    })
                );
                let expected = &case["expected"];
                let matches = match expected["condition"].as_str() {
                    Some(condition) => actual
                        .as_ref()
                        .is_err_and(|error| error.to_string().contains(&format!("[{condition}]"))),
                    None => actual
                        .as_ref()
                        .is_ok_and(|actual| actual["compared"] == *expected),
                };
                if !matches {
                    differences.push(format!(
                        "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert_eq!(stage_comparisons, 184);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "recorded date executed metadata")]
async fn date_stage_reference_rejects_unrecorded_metadata() {
    let stages = date_metadata_stages().unwrap();
    let mut case = stages[1..]
        .iter()
        .find(|case| case["id"] == "date_add_correlated_valid_text" && case["ansi"] == true)
        .unwrap()
        .clone();
    case["executed_schema_after_collect"]["fields"][0]["metadata"] =
        json!({"unrecorded":"metadata"});
    let header: Value = serde_json::from_str(
        include_str!("fixtures/date-day-correlated-spark.jsonl").lines().next().unwrap(),
    ).unwrap();
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    register_recorded_inputs(&session, &header, 32).unwrap();
    capture_date_result(&session, &case).await.unwrap();
}

#[tokio::test]
async fn spark_date_functions_preserve_native_calendar_arithmetic() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT DATE '2020-01-31' + INTERVAL '1 month' AS r",
        "SELECT DATE '2020-02-28' + INTERVAL '1 day' AS r",
        "SELECT DATE '2020-03-01' - INTERVAL '1 day' AS r",
        "SELECT TIMESTAMP '2020-02-28 23:59:59' + INTERVAL '1 day' AS r",
    ] {
        assert_eq!(
            configured.sql(sql).await?.collect().await?,
            native.sql(sql).await?.collect().await?,
            "{sql}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn missing_attributes_only_classify_scopes_without_suggestions() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for (sql, no_suggestions) in [
        ("SELECT missing AS r", true),
        ("SELECT (SELECT missing) AS r", true),
        ("SELECT missing AS r FROM (VALUES (1)) t(candidate)", false),
        (
            "SELECT t.missing AS r FROM (VALUES (1)) t(candidate)",
            false,
        ),
        (
            "SELECT (SELECT missing) AS r FROM (VALUES (1)) t(candidate)",
            false,
        ),
    ] {
        let error = plan_query(&session, sql, &SparkSqlOptions::default())
            .await
            .err()
            .ok_or("missing attribute unexpectedly resolved")?
            .to_string();
        assert!(
            error.contains("is missing from the schema: cannot resolve attribute"),
            "{error}"
        );
        assert_eq!(
            error.contains("[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION]"),
            no_suggestions,
            "{sql}: {error}"
        );
    }
    Ok(())
}
