//! Interval expression names retain the fields recorded independently by Spark.

use std::collections::BTreeSet;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::Value;

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

#[tokio::test]
async fn interval_null_coercion_matches_spark() -> TestResult {
    let records = include_str!("fixtures/interval-null-coercion-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 17);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["interval_encoding"], "cast_string");
    assert_eq!(records[0]["timestamp_encoding"], "cast_string");
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let mut identities = BTreeSet::new();
    let mut differences = Vec::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        assert!(case["expected"]["schema"].is_object());
        let actual = capture_spark_query_result(&session, case).await;
        if actual.as_ref().ok() != Some(&case["expected"]) {
            differences.push(format!(
                "{} ANSI={}: expected {}, got {actual:?}",
                case["id"], case["ansi"], case["expected"]
            ));
        }
    }
    assert_eq!(identities.len(), 16);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn mixed_family_interval_references_are_rejected_before_window_resolution() -> TestResult {
    let records = include_str!("fixtures/interval-qualifiers-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let session = SessionContext::new();
    register_recorded_inputs(&session, &records[0], 2)?;
    let mut count = 0;
    for case in &records[1..] {
        if case["expected"]["condition"] == "_LEGACY_ERROR_TEMP_0029" {
            count += 1;
            let result = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    ..SparkSqlOptions::default()
                },
            )
            .await;
            assert!(
                result.is_err_and(|error| error.to_string().contains("_LEGACY_ERROR_TEMP_0029")),
                "{}",
                case["id"]
            );
        }
    }
    assert_eq!(count, 8);
    Ok(())
}

fn recorded_scalar(
    value: &Value,
    spark_type: &str,
    storage_type: &DataType,
) -> TestResult<ScalarValue> {
    if value.is_null() {
        return Ok(ScalarValue::try_from(storage_type)?);
    }
    let text = value.as_str().ok_or("expected a recorded string value")?;
    if spark_type == "timestamp" {
        let array = arrow::compute::cast(&StringArray::from(vec![text]), storage_type)?;
        return Ok(ScalarValue::try_from_array(array.as_ref(), 0)?);
    }
    let body = text.split('\'').nth(1).ok_or("missing interval value")?;
    let negative = body.starts_with('-');
    let body = body.strip_prefix('-').unwrap_or(body);
    let fields = spark_type
        .strip_prefix("interval ")
        .ok_or("missing interval type")?;
    let (start, end) = fields.split_once(" to ").unwrap_or((fields, fields));
    let sign = if negative { -1_i128 } else { 1 };
    if matches!(start, "year" | "month") {
        let (years, months) = if start == "year" {
            let (years, months) = body.split_once('-').unwrap_or((body, "0"));
            (years.parse::<i128>()?, months.parse::<i128>()?)
        } else {
            (0, body.parse::<i128>()?)
        };
        return Ok(ScalarValue::IntervalYearMonth(Some(i32::try_from(
            sign * (years * 12 + months),
        )?)));
    }
    let units = [
        ("day", 86_400_000_000_i128),
        ("hour", 3_600_000_000),
        ("minute", 60_000_000),
        ("second", 1_000_000),
    ];
    let first = units
        .iter()
        .position(|(field, _)| *field == start)
        .ok_or("invalid first field")?;
    let last = units
        .iter()
        .position(|(field, _)| *field == end)
        .ok_or("invalid last field")?;
    let parts = body.split([' ', ':']).collect::<Vec<_>>();
    assert_eq!(parts.len(), last - first + 1);
    let mut microseconds = 0_i128;
    for (part, (_, unit)) in parts.iter().zip(&units[first..=last]) {
        let (whole, fraction) = part.split_once('.').unwrap_or((part, ""));
        microseconds += whole.parse::<i128>()? * unit;
        if !fraction.is_empty() {
            assert_eq!(*unit, 1_000_000);
            assert!(fraction.len() <= 6);
            microseconds += fraction.parse::<i128>()? * 10_i128.pow((6 - fraction.len()) as u32);
        }
    }
    Ok(ScalarValue::DurationMicrosecond(Some(i64::try_from(
        sign * microseconds,
    )?)))
}

#[tokio::test]
async fn aliased_timestamp_interval_ranges_match_spark() -> TestResult {
    let records = include_str!("fixtures/interval-qualifiers-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    register_recorded_inputs(&session, &records[0], 2)?;
    let mut count = 0;
    for case in &records[1..] {
        if case["context"] == "window_alias" && case["expected"]["schema"].is_object() {
            count += 1;
            assert_eq!(
                capture_spark_query_result(&session, case).await?,
                case["expected"],
                "{}",
                case["id"]
            );
        }
    }
    assert_eq!(count, 12);
    Ok(())
}

#[tokio::test]
async fn interval_expression_names_and_scoped_values_match_spark() -> TestResult {
    let records = include_str!("fixtures/interval-qualifiers-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 433);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["timestamp_encoding"], "cast_string");
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    register_recorded_inputs(&session, &records[0], 2)?;
    let mut identities = BTreeSet::new();
    let mut differences = Vec::new();
    let mut scalar_count = 0;
    let mut cast_value_gates = 0;
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].clone().to_string(),
            case["ansi"].clone().to_string()
        )));
        // Full OVER labels and DATE RANGE admission have separate integration owners.
        if !matches!(
            case["context"].as_str(),
            Some("literal" | "syntax" | "arithmetic" | "timestamp" | "cast")
        ) {
            continue;
        }
        scalar_count += 1;
        let id = case["id"].as_str().ok_or("missing ID")?;
        let label = format!("{id} ANSI={}", case["ansi"]);
        let gate_cast_values = matches!(
            id,
            "cast_year"
                | "cast_day"
                | "cast_day_hour"
                | "cast_day_minute"
                | "cast_hour"
                | "cast_hour_minute"
                | "cast_minute"
        );
        cast_value_gates += usize::from(gate_cast_values);
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        let planned = plan_query(
            &session,
            sql,
            &SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                ..SparkSqlOptions::default()
            },
        )
        .await;
        if let Some(condition) = case["expected"]["condition"].as_str() {
            assert_eq!(condition, "_LEGACY_ERROR_TEMP_0029");
            if !planned.is_err_and(|error| error.to_string().contains(condition)) {
                differences.push(format!("{label}: expected {condition}"));
            }
            continue;
        }
        let query = match planned {
            Ok(query) => query,
            Err(error) => {
                differences.push(format!("{label}: planning: {error}"));
                continue;
            }
        };
        let physical = match query.create_physical_plan(&session).await {
            Ok(physical) => physical,
            Err(error) => {
                differences.push(format!("{label}: physical planning: {error}"));
                continue;
            }
        };
        let schema = physical.schema();
        let expected = case["expected"]["schema"]["fields"]
            .as_array()
            .ok_or("missing fields")?;
        if schema.fields().len() != expected.len() || query.column_names.len() != expected.len() {
            differences.push(format!(
                "{label}: expected {} columns, got names {:?}, schema {schema:?}",
                expected.len(),
                query.column_names
            ));
            continue;
        }
        let mut expected_types = Vec::with_capacity(expected.len());
        for (index, (field, expected)) in schema.fields().iter().zip(expected).enumerate() {
            let expected_type = match expected["type"].as_str().ok_or("missing type")? {
                "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                kind if kind.starts_with("interval year") || kind == "interval month" => {
                    DataType::Interval(IntervalUnit::YearMonth)
                }
                kind if kind.starts_with("interval ") => DataType::Duration(TimeUnit::Microsecond),
                other => return Err(format!("unexpected interval expression type: {other}").into()),
            };
            if field.name() != expected["name"].as_str().ok_or("missing name")?
                || field.name() != &query.column_names[index]
                || field.data_type() != &expected_type
                || field.is_nullable()
                    != expected["nullable"]
                        .as_bool()
                        .ok_or("missing nullability")?
                || serde_json::to_value(field.metadata())? != expected["metadata"]
            {
                differences.push(format!("{label}: expected {expected}, got {field:?}"));
            }
            expected_types.push(expected_type);
            let logical = query
                .logical_plan
                .schema()
                .field(index)
                .as_ref()
                .clone()
                .with_name(query.column_names[index].clone());
            if &logical != field.as_ref() {
                differences.push(format!("{label}: visible logical field {logical:?} differs from physical field {field:?}"));
            }
        }
        if gate_cast_values {
            // #765 owns interval CAST truncation; every declared output field is checked above.
            continue;
        }
        let mut actual_rows = Vec::new();
        let batches = match collect(physical, session.task_ctx()).await {
            Ok(batches) => batches,
            Err(error) => {
                differences.push(format!("{label}: execution: {error}"));
                continue;
            }
        };
        for batch in batches {
            for row in 0..batch.num_rows() {
                actual_rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| ScalarValue::try_from_array(column.as_ref(), row))
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
        }
        let mut expected_rows = case["expected"]["rows"]
            .as_array()
            .ok_or("missing rows")?
            .iter()
            .map(|row| {
                let row = row.as_array().ok_or("missing row")?;
                assert_eq!(row.len(), expected.len());
                row.iter()
                    .zip(expected)
                    .zip(&expected_types)
                    .map(|((value, field), storage_type)| {
                        recorded_scalar(
                            value,
                            field["type"].as_str().ok_or("missing type")?,
                            storage_type,
                        )
                    })
                    .collect::<TestResult<Vec<_>>>()
            })
            .collect::<TestResult<Vec<_>>>()?;
        actual_rows.sort_by_key(|row| format!("{row:?}"));
        expected_rows.sort_by_key(|row| format!("{row:?}"));
        if actual_rows != expected_rows {
            differences.push(format!(
                "{label}: expected {expected_rows:?}, got {actual_rows:?}"
            ));
        }
    }
    assert_eq!(identities.len(), 432);
    assert_eq!(scalar_count, 392);
    assert_eq!(cast_value_gates, 14);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn interval_names_keep_native_sql_unchanged() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for (sql, valid) in [
        (
            "SELECT INTERVAL '2' DAY AS d, INTERVAL '3' MONTH AS m",
            true,
        ),
        (
            "SELECT TIMESTAMP '2020-01-01 00:00:00' + INTERVAL '1' DAY AS t",
            true,
        ),
        ("SELECT INTERVAL '1' DAY * 2 AS d", false),
    ] {
        let mut results = Vec::new();
        for session in [&native, &configured] {
            let result: datafusion::error::Result<_> = async {
                let frame = session.sql(sql).await?;
                let physical = frame.create_physical_plan().await?;
                collect(physical, session.task_ctx()).await
            }
            .await;
            results.push(result.map_err(|error| error.to_string()));
        }
        assert_eq!(results[0].is_ok(), valid, "{sql}");
        assert_eq!(results[0], results[1], "{sql}");
    }
    Ok(())
}
