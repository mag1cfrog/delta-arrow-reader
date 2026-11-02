//! Independent Spark references for exact year-month arithmetic and its declarations.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn references() -> TestResult<Vec<Value>> {
    Ok(include_str!("fixtures/year-month-scaling-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn recorded_type(kind: &str) -> TestResult<DataType> {
    Ok(match kind {
        "integer" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "interval year to month" => DataType::Interval(IntervalUnit::YearMonth),
        "interval day to second" => DataType::Duration(TimeUnit::Microsecond),
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        decimal if decimal.starts_with("decimal(") => {
            let (precision, scale) = decimal
                .strip_prefix("decimal(")
                .and_then(|body| body.strip_suffix(')'))
                .and_then(|body| body.split_once(','))
                .ok_or("invalid Decimal type")?;
            DataType::Decimal128(precision.parse()?, scale.parse()?)
        }
        other => return Err(format!("unexpected recorded type: {other}").into()),
    })
}

fn recorded_value(value: &Value, data_type: &DataType) -> TestResult<ScalarValue> {
    if value.is_null() {
        return Ok(ScalarValue::try_from(data_type)?);
    }
    let text = value.as_str().ok_or("missing recorded string")?;
    Ok(match data_type {
        DataType::Int32 => ScalarValue::Int32(Some(text.parse()?)),
        DataType::Int64 => ScalarValue::Int64(Some(text.parse()?)),
        DataType::Float32 => {
            ScalarValue::Float32(Some(f32::from_bits(u32::from_str_radix(text, 16)?)))
        }
        DataType::Float64 => {
            ScalarValue::Float64(Some(f64::from_bits(u64::from_str_radix(text, 16)?)))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let months = if text.starts_with("INTERVAL '") {
                let body = text.split('\'').nth(1).ok_or("missing interval body")?;
                let negative = body.starts_with('-');
                let (years, months) = body
                    .trim_start_matches('-')
                    .split_once('-')
                    .ok_or("missing year-month separator")?;
                let months = years.parse::<i64>()? * 12 + months.parse::<i64>()?;
                i32::try_from(if negative { -months } else { months })?
            } else {
                text.parse()?
            };
            ScalarValue::IntervalYearMonth(Some(months))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let body = text.split('\'').nth(1).ok_or("missing duration body")?;
            let negative = body.starts_with('-');
            let parts = body
                .trim_start_matches('-')
                .split([' ', ':'])
                .collect::<Vec<_>>();
            assert_eq!(parts.len(), 4);
            let (seconds, fraction) = parts[3].split_once('.').unwrap_or((parts[3], ""));
            assert!(fraction.len() <= 6);
            let micros = parts[0].parse::<i64>()? * 86_400_000_000
                + parts[1].parse::<i64>()? * 3_600_000_000
                + parts[2].parse::<i64>()? * 60_000_000
                + seconds.parse::<i64>()? * 1_000_000
                + if fraction.is_empty() {
                    0
                } else {
                    fraction.parse::<i64>()? * 10_i64.pow((6 - fraction.len()) as u32)
                };
            ScalarValue::DurationMicrosecond(Some(if negative { -micros } else { micros }))
        }
        DataType::Decimal128(_, _) | DataType::Timestamp(_, _) => {
            let array = arrow::compute::cast(&StringArray::from(vec![text]), data_type)?;
            ScalarValue::try_from_array(array.as_ref(), 0)?
        }
        other => return Err(format!("unexpected recorded value type: {other}").into()),
    })
}

fn register_inputs(session: &SessionContext, metadata: &Value) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let fields = input["schema"]["fields"]
            .as_array()
            .ok_or("missing input fields")?;
        let rows = input["rows"].as_array().ok_or("missing input rows")?;
        let fields = fields
            .iter()
            .map(|field| {
                Ok(Field::new(
                    field["name"].as_str().ok_or("missing input name")?,
                    recorded_type(field["type"].as_str().ok_or("missing input type")?)?,
                    field["nullable"]
                        .as_bool()
                        .ok_or("missing input nullability")?,
                )
                .with_metadata(serde_json::from_value(field["metadata"].clone())?))
            })
            .collect::<TestResult<Vec<_>>>()?;
        let arrays = fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let values = rows
                    .iter()
                    .map(|row| {
                        assert_eq!(row.as_array().map(Vec::len), Some(fields.len()));
                        recorded_value(&row[index], field.data_type())
                    })
                    .collect::<TestResult<Vec<_>>>()?;
                Ok(ScalarValue::iter_to_array(values)?)
            })
            .collect::<TestResult<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        let batches = (0..batch.num_rows())
            .step_by(2)
            .map(|offset| batch.slice(offset, 2.min(batch.num_rows() - offset)))
            .collect();
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(schema, vec![batches])?),
        )?;
    }
    Ok(())
}

fn output_field(field: &Field, visible_name: &str) -> TestResult<Value> {
    let kind = match field.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => "interval year to month",
        DataType::Duration(TimeUnit::Microsecond) => "interval day to second",
        DataType::Timestamp(TimeUnit::Microsecond, timezone)
            if timezone.as_deref() == Some("UTC") =>
        {
            "timestamp"
        }
        other => return Err(format!("unexpected output type: {other}").into()),
    };
    Ok(
        json!({"name":visible_name,"type":kind,"nullable":field.is_nullable(),"metadata":field.metadata()}),
    )
}

async fn observe(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let mut observed = json!({});
    let result: TestResult = async {
        let query = plan_query(session, case["sql"].as_str().ok_or("missing SQL")?, &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(), ..Default::default()
        }).await?;
        assert_eq!(query.column_names, ["r"]);
        observed["schema"] = json!({"type":"struct","fields":[output_field(query.logical_plan.schema().field(0), "r")?]});
        let physical = query.create_physical_plan(session).await?;
        let schema = physical.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "r");
        observed["physical_schema"] = json!({"type":"struct","fields":[output_field(schema.field(0), "r")?]});
        let mut rows = Vec::new();
        for batch in collect(physical, session.task_ctx()).await? {
            assert_eq!(batch.schema(), schema);
            assert!(schema.field(0).is_nullable() || batch.column(0).null_count() == 0);
            assert!(query.logical_plan.schema().field(0).is_nullable() || batch.column(0).null_count() == 0);
            for row in 0..batch.num_rows() {
                rows.push(ScalarValue::try_from_array(batch.column(0).as_ref(), row)?);
            }
        }
        rows.sort_by_key(|row| format!("{row:?}"));
        observed["rows"] = json!(rows.iter().map(|row| format!("{row:?}")).collect::<Vec<_>>());
        Ok(())
    }.await;
    if let Err(error) = result {
        observed["error"] = json!(error.to_string());
    }
    Ok(observed)
}

fn expected_rows(expected: &Value) -> TestResult<Value> {
    let data_type = recorded_type(
        expected["schema"]["fields"][0]["type"]
            .as_str()
            .ok_or("missing result type")?,
    )?;
    let mut rows = expected["rows"]
        .as_array()
        .ok_or("missing result rows")?
        .iter()
        .map(|row| {
            assert_eq!(row.as_array().map(Vec::len), Some(1));
            Ok(format!("{:?}", recorded_value(&row[0], &data_type)?))
        })
        .collect::<TestResult<Vec<_>>>()?;
    rows.sort();
    Ok(json!(rows))
}

fn matches_error(observed: &Value, expected: &Value) -> TestResult<bool> {
    let Some(error) = observed["error"].as_str() else {
        return Ok(false);
    };
    if let Some(condition) = expected["condition"].as_str() {
        return Ok(error.contains(&format!("[{condition}]")));
    }
    let reference = expected["error_message"]
        .as_str()
        .ok_or("missing reference error")?;
    let phrase = [
        "integer overflow",
        "long overflow",
        "rounded value is out of range",
        "input is infinite or NaN",
        "Overflow",
    ]
    .into_iter()
    .find(|phrase| reference.contains(phrase))
    .ok_or("unknown reference arithmetic error")?;
    Ok(error.contains(phrase))
}

#[tokio::test]
async fn year_month_scaling_matches_independent_spark_references() -> TestResult {
    let records = references()?;
    assert_eq!(records.len(), 267);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(
        records[0]["original_sha256"],
        "a69f5483344ac3a986eb3cbb35a4fe2dafb71cbd70301a32961ea840410386bf"
    );
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_batch_size(2)),
        )
        .build(),
    );
    register_inputs(&session, &records[0])?;
    let mut identities = BTreeSet::new();
    let mut differences = Vec::new();
    let mut original_consumers = 0;
    let mut controls = 0;
    let mut compared = 0;
    for case in &records[1..] {
        assert!(identities.insert((case["id"].to_string(), case["ansi"].to_string())));
        // #749 and #710 replay these retained unaliased consumers in the combined gate.
        if case["context"] == "original_unaliased" {
            original_consumers += 1;
            continue;
        }
        if case["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("day_time_"))
        {
            controls += 1;
            continue;
        }
        compared += 1;
        let expected = &case["expected"];
        assert_eq!(expected["schema"], expected["analyzed_schema"]);
        let observed = observe(&session, case).await?;
        eprintln!(
            "year-month observation: {}",
            json!({"id":case["id"],"ansi":case["ansi"],"observed":observed})
        );
        let label = format!("{} ANSI={}", case["id"], case["ansi"]);
        if observed["schema"] != expected["schema"] {
            differences.push(format!(
                "{label}: public schema: expected {}, got {}",
                expected["schema"], observed["schema"]
            ));
        }
        let mut later = observed["physical_schema"].clone();
        if later.is_object() {
            let public_nullable = expected["schema"]["fields"][0]["nullable"]
                .as_bool()
                .ok_or("missing nullability")?;
            let later_nullable = later["fields"][0]["nullable"]
                .as_bool()
                .ok_or("missing later nullability")?;
            if later_nullable && !public_nullable {
                differences.push(format!("{label}: physical schema widened requiredness"));
            }
            later["fields"][0]["nullable"] = json!(public_nullable);
            if later != expected["schema"] {
                differences.push(format!(
                    "{label}: physical schema: expected {}, got {}",
                    expected["schema"], observed["physical_schema"]
                ));
            }
        } else if expected.get("optimized_schema").is_some() {
            differences.push(format!("{label}: missing physical schema"));
        }
        if expected.get("error_phase").is_some() {
            if !matches_error(&observed, expected)? {
                differences.push(format!(
                    "{label}: expected error {}, got {observed}",
                    expected["error_message"]
                ));
            }
            continue;
        }
        if observed["rows"] != expected_rows(expected)? || observed.get("error").is_some() {
            differences.push(format!("{label}: expected {expected}, got {observed}"));
        }
    }
    assert_eq!(
        (identities.len(), original_consumers, controls, compared),
        (266, 24, 4, 238)
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn day_time_scaling_controls_keep_values_and_fields() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let mut count = 0;
    for case in &references()?[1..] {
        if !case["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("day_time_"))
        {
            continue;
        }
        count += 1;
        let observed = observe(&session, case).await?;
        let expected = &case["expected"];
        assert_eq!(observed["physical_schema"], expected["schema"]);
        assert_eq!(observed["rows"], expected_rows(expected)?);
        assert!(observed.get("error").is_none());
        // #782 owns the pre-existing public requiredness gap for day-time division.
        let mut public = expected["schema"].clone();
        public["fields"][0]["nullable"] = json!(case["id"] == "day_time_divide");
        assert_eq!(observed["schema"], public);
    }
    assert_eq!(count, 4);
    Ok(())
}

#[tokio::test]
async fn native_interval_arithmetic_keeps_its_existing_results_and_errors() -> TestResult {
    let native = SessionContext::new();
    let spark = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT INTERVAL '2' MONTH AS r",
        "SELECT TIMESTAMP '2020-01-01 00:00:00.123456'+INTERVAL '1' MONTH AS r",
        "SELECT INTERVAL '2' MONTH*2 AS r",
    ] {
        let left = async { native.sql(sql).await?.collect().await }.await;
        let right = async { spark.sql(sql).await?.collect().await }.await;
        match (left, right) {
            (Ok(left), Ok(right)) => assert_eq!(left, right),
            (Err(left), Err(right)) => assert_eq!(left.to_string(), right.to_string()),
            outcomes => {
                return Err(
                    format!("native interval behavior changed for {sql}: {outcomes:?}").into(),
                );
            }
        }
    }
    Ok(())
}
