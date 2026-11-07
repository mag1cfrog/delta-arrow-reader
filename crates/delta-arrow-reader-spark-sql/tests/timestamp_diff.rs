//! TIMESTAMPDIFF counts whole units between local date-times in the session time zone like
//! Spark, under every spelling, unit and argument type.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::{decode_recorded_batch, register_recorded_inputs};

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, validate_batch};

fn timestamp_diff_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/timestamp-diff/timestamp-diff-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 1868);
    Ok(records)
}

/// The Arrow type family DAR uses for a recorded Spark type.
fn spark_family(spark_type: &Value) -> TestResult<&'static str> {
    Ok(
        match spark_type.as_str().ok_or("expected a primitive type")? {
            "integer" => "int32",
            "long" => "int64",
            other => return Err(format!("unexpected Spark type {other}").into()),
        },
    )
}

fn dar_family(data_type: &DataType) -> TestResult<&'static str> {
    Ok(match data_type {
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        other => return Err(format!("unexpected DAR type {other}").into()),
    })
}

fn spark_fields(schema: &Value) -> TestResult<Value> {
    schema["fields"]
        .as_array()
        .ok_or("missing recorded fields")?
        .iter()
        .map(|field| {
            Ok(
                json!({"name": field["name"], "type": spark_family(&field["type"])?,
                "nullable": field["nullable"], "metadata": field["metadata"]}),
            )
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

fn dar_fields(fields: &arrow::datatypes::Fields, names: Option<&[String]>) -> TestResult<Value> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            Ok(json!({"name": name, "type": dar_family(field.data_type())?,
                "nullable": field.is_nullable(), "metadata": field.metadata()}))
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        other => return Err(format!("unexpected result type {other}").into()),
    })
}

/// Observe analyzed (public names), optimized and executed fields, typed rows and the
/// failing phase in the record's session time zone.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    let mut phase = "resolve";
    let result: TestResult<Vec<Vec<Value>>> = async {
        let options = SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: case["timezone"].as_str().ok_or("missing time zone")?.into(),
            ..SparkSqlOptions::default()
        };
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options,
        )
        .await?;
        observation["analyzed"] = dar_fields(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        )?;
        phase = "optimized";
        let mut state = session.state();
        let extensions = &mut state.config_mut().options_mut().extensions;
        extensions.insert(SparkSqlExpressionSemantics::default());
        extensions.insert(PreserveAggregateStatisticsSchema::default());
        let optimized = state.optimize(&query.logical_plan)?;
        observation["optimized"] =
            dar_fields(optimized.schema().fields(), Some(&query.column_names))?;
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None)?;
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                );
            }
        }
        rows.sort_by_key(|row| json!(row).to_string());
        Ok(rows)
    }
    .await;
    match result {
        Ok(rows) => observation["rows"] = json!(rows),
        Err(error) => {
            observation["error_phase"] = json!(phase);
            observation["error"] = json!(error.to_string());
        }
    }
    observation
}

/// DAR's timestamp parser reports a failed STRING conversion with its own message, the accepted
/// form of Spark's condition in the other temporal harnesses.
fn accepted_messages(condition: &str) -> &'static [&'static str] {
    match condition {
        "CAST_INVALID_INPUT" => &["error in SQL parser: found"],
        _ => &[],
    }
}

/// Match the failing stage and Spark's error condition, or Spark's exact message when Spark
/// raises a plain Java exception without one.
fn matches_expected_error(expectation: &Value, observation: &Value) -> bool {
    let expected_phase = match expectation["spark_error_phase"].as_str() {
        Some("sql") => "resolve",
        Some(phase) => phase,
        None => return false,
    };
    let Some(error) = observation["error"].as_str() else {
        return false;
    };
    let expected = &expectation["expected"];
    let message_matches = match expected["condition"].as_str() {
        Some(condition) => {
            error.contains(&format!("[{condition}]"))
                || accepted_messages(condition)
                    .iter()
                    .any(|message| error.contains(message))
        }
        None => expected["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty() && error.contains(message)),
    };
    observation["error_phase"] == expected_phase && message_matches
}

/// Differences outside TIMESTAMPDIFF's unit counting, each owned by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1051: `timestamp_diff` resolves as an alias where Spark has no such routine.
    TimestampDiffAlias,
    /// #1052: the zone table lists no Los Angeles transitions after 2099, so one elapsed hour
    /// across the 2100 spring transition counts as one local hour instead of two.
    DstAfter2099,
}

/// The length of each time-based unit in microseconds, by case-id prefix.
const TIME_UNIT_MICROS: [(&str, i64); 5] = [
    ("microsecond_", 1),
    ("millisecond_", 1_000),
    ("second_", 1_000_000),
    ("minute_", 60_000_000),
    ("hour_", 3_600_000_000),
];

fn time_unit_micros(id: &str) -> Option<i64> {
    TIME_UNIT_MICROS
        .iter()
        .find(|(prefix, _)| id.starts_with(prefix))
        .map(|(_, micros)| *micros)
}

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    if id == "timestamp_diff_name" {
        Some(RetainedDifference::TimestampDiffAlias)
    } else if id.ends_with("_far_future_gap")
        && case["timezone"] == "America/Los_Angeles"
        && time_unit_micros(id).is_some()
    {
        Some(RetainedDifference::DstAfter2099)
    } else {
        None
    }
}

/// Replace only the retained part of Spark's expectation with DAR's current result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    match difference {
        RetainedDifference::TimestampDiffAlias => {
            if case["expected"]["condition"] != "UNRESOLVED_ROUTINE"
                || case["spark_error_phase"] != "sql"
            {
                return Err(format!("expected Spark's unresolved routine: {case}").into());
            }
            current["expected"] = json!({"status": "ok", "rows": [["0"]]});
            current
                .as_object_mut()
                .ok_or("expected a record")?
                .remove("spark_error_phase");
        }
        RetainedDifference::DstAfter2099 => {
            let unit = time_unit_micros(case["id"].as_str().ok_or("missing id")?)
                .ok_or("expected a time-based unit")?;
            let hour = 3_600_000_000 / unit;
            if case["expected"]["rows"] != json!([[(2 * hour).to_string()]]) {
                return Err(format!("expected two local hours: {case}").into());
            }
            current["expected"]["rows"] = json!([[hour.to_string()]]);
        }
    }
    Ok(current)
}

/// The recorded stages and outcome that differ from the observation.
fn differing_parts(expectation: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differing = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if let Some(expected) = expectation["schema_stages"].get(stage)
            && observation[stage] != spark_fields(expected)?
        {
            differing.push(stage);
        }
    }
    let outcome_matches = match expectation["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == expectation["expected"]["rows"],
        Some("error") => matches_expected_error(expectation, observation),
        _ => false,
    };
    if !outcome_matches {
        differing.push("outcome");
    }
    Ok(differing)
}

/// Registers a `dict_<name>` copy of each recorded input with STRING columns, encoded as
/// Delta encodes a STRING partition column. Spark has no such encoding.
fn register_dictionary_string_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    let encoded = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let batch = decode_recorded_batch(input)?;
        let schema = batch.schema();
        if !schema
            .fields()
            .iter()
            .any(|field| field.data_type() == &DataType::Utf8)
        {
            continue;
        }
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            if field.data_type() == &DataType::Utf8 {
                fields.push(Field::clone(field).with_data_type(encoded.clone()));
                columns.push(cast(column, &encoded)?);
            } else {
                fields.push(Field::clone(field));
                columns.push(Arc::clone(column));
            }
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        let mut partitions = vec![Vec::new(), Vec::new()];
        for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[chunk % 2]
                .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        let name = input["name"].as_str().ok_or("missing table name")?;
        session.register_table(
            format!("dict_{name}"),
            Arc::new(MemTable::try_new(batch.schema(), partitions)?),
        )?;
    }
    Ok(())
}

/// The record's query over dictionary-encoded copies of its STRING inputs, for each record
/// that reads a STRING column.
fn dictionary_string_case(case: &Value) -> TestResult<Option<Value>> {
    let id = case["id"].as_str().ok_or("missing id")?;
    if !(id.ends_with("_string_columns") || id == "invalid_string_column") {
        return Ok(None);
    }
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    let dictionary_sql = sql
        .replace(" FROM diff_inputs", " FROM dict_diff_inputs")
        .replace(" FROM bad_strings", " FROM dict_bad_strings");
    assert_ne!(dictionary_sql, sql, "{id} reads no recorded STRING input");
    let mut case = case.clone();
    case["sql"] = json!(dictionary_sql);
    Ok(Some(case))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_diff_matches_spark() -> TestResult {
    let records = timestamp_diff_references()?;
    let mut retained = BTreeMap::new();
    let mut dictionary_cases = 0;
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
        if dictionary_string_case(case)?.is_some() {
            dictionary_cases += 1;
        }
    }
    assert_eq!(dictionary_cases, 20);
    assert_eq!(
        retained,
        BTreeMap::from([
            (RetainedDifference::TimestampDiffAlias, 4),
            (RetainedDifference::DstAfter2099, 10),
        ])
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(1),
                    ),
            )
            .build(),
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        register_dictionary_string_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let expectation = match retained_difference(case) {
                Some(difference) => current_expectation(case, difference)?,
                None => case.clone(),
            };
            let dictionary_case = dictionary_string_case(case)?;
            for observed_case in std::iter::once(case).chain(dictionary_case.as_ref()) {
                let observation = observe(&session, observed_case).await;
                let differing = differing_parts(&expectation, &observation)?;
                if !differing.is_empty() {
                    differences.push(json!({
                        "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                        "batch": batch_size, "sql": observed_case["sql"], "differing": differing,
                        "expected": case["expected"], "schema_stages": case["schema_stages"],
                        "spark_error_phase": case["spark_error_phase"], "observed": observation,
                    }));
                }
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    Ok(())
}
