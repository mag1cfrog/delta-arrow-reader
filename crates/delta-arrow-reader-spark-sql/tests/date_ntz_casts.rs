//! Casts between TIMESTAMP_NTZ and DATE are nullable like Spark's in CAST, DATE(),
//! TO_TIMESTAMP_NTZ, the date functions and EXTRACT fields that read a DATE, comparisons and
//! CASE, IF, COALESCE and UNION: at every stage over a column, and until constant folding over a
//! literal. TRY_CAST from DATE to TIMESTAMP_NTZ, which cannot fail, keeps its input's
//! nullability.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Fields, Int32Type, TimeUnit, TimestampMicrosecondType,
};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::decode_recorded_batch;

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, validate_batch};

fn cast_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/date-ntz-casts/date-ntz-casts-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 540);
    Ok(records)
}

/// Differences outside the casts' nullability, each named for DAR's current result and owned by
/// an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1087: TRUNC of the DATE in year 1 fails while collecting rows, where DataFusion converts
    /// it to a nanosecond TIMESTAMP; Spark returns the truncated DATE.
    TruncBeyondNanoseconds,
    /// #1188: UNIX_DATE casts a TIMESTAMP_NTZ argument to DATE and counts its days, where Spark
    /// rejects the argument during analysis.
    UnixDateOfTimestampNtz,
    /// #1190: a constant over an input proven empty keeps its analyzed nullability after
    /// optimization, where Spark folds it first.
    EmptyInputNullability,
    /// #1191: a DATE in an IN list of TIMESTAMP_NTZ values uses DataFusion's coercion and is NOT
    /// NULL, where Spark's cast makes it nullable.
    InListNullability,
    /// #1192: TRUNC to HOUR gives the DATE, where Spark gives NULL for that unit.
    TruncUnsupportedUnit,
    /// #1116: EXTRACT of DAYOFWEEK, DOW_ISO and YEAROFWEEK fails while collecting rows as an
    /// unsupported date part.
    UnsupportedExtractField,
}

const UNIX_DATE_RECORDS: [&str; 3] = [
    "required_unix_date",
    "nullable_unix_date",
    "literal_unix_date",
];

/// The EXTRACT fields DAR's date part rejects, by record suffix.
const UNSUPPORTED_EXTRACT_FIELDS: [(&str, &str); 3] = [
    ("_extract_dayofweek", "DAYOFWEEK"),
    ("_extract_dow_iso", "DOW_ISO"),
    ("_extract_yearofweek", "YEAROFWEEK"),
];

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    Some(match id {
        "required_trunc" => RetainedDifference::TruncBeyondNanoseconds,
        "literal_cast_to_date_where_false" => RetainedDifference::EmptyInputNullability,
        "required_date_in_timestamp_ntz" => RetainedDifference::InListNullability,
        "required_trunc_hour" => RetainedDifference::TruncUnsupportedUnit,
        id if UNIX_DATE_RECORDS.contains(&id) => RetainedDifference::UnixDateOfTimestampNtz,
        id if UNSUPPORTED_EXTRACT_FIELDS
            .iter()
            .any(|(suffix, _)| id.ends_with(suffix)) =>
        {
            RetainedDifference::UnsupportedExtractField
        }
        _ => return None,
    })
}

/// The day of each TIMESTAMP_NTZ value of `inputs` column `column` (1 `tn`, 2 `n`), keyed by
/// row, for rows the filter keeps.
fn days_of(metadata: &Value, column: usize, keep: impl Fn(&str) -> bool) -> TestResult<Value> {
    let inputs = metadata["inputs"][0]["rows"]
        .as_array()
        .ok_or("missing inputs rows")?;
    let mut rows = Vec::new();
    for row in inputs {
        let key = row[0].as_str().ok_or("missing key")?;
        if !keep(key) {
            continue;
        }
        let day = match row[column].as_str() {
            Some(micros) => json!(
                micros
                    .parse::<i64>()?
                    .div_euclid(86_400_000_000)
                    .to_string()
            ),
            None => Value::Null,
        };
        rows.push(json!([key, day]));
    }
    rows.sort_by_key(|row| row.to_string());
    Ok(json!(rows))
}

/// Set the result field's nullability at the given stages.
fn set_nullable(current: &mut Value, stages: &[&str], nullable: bool) -> TestResult {
    for stage in stages {
        let fields = current["schema_stages"][*stage]["fields"]
            .as_array_mut()
            .ok_or("missing fields")?;
        let field = fields.last_mut().ok_or("missing result field")?;
        field["nullable"] = json!(nullable);
    }
    Ok(())
}

/// DAR's current result for a retained record.
fn current_expectation(
    case: &Value,
    difference: RetainedDifference,
    metadata: &Value,
) -> TestResult<Value> {
    let mut current = case.clone();
    let id = case["id"].as_str().ok_or("missing id")?;
    match difference {
        RetainedDifference::TruncBeyondNanoseconds => {
            if case["expected"]["status"] != "ok" {
                return Err(format!("expected Spark's truncated DATE: {case}").into());
            }
            current["expected"] = json!({"status": "error",
                "dar_message": "Cannot cast Date32 value -719162 to Timestamp(ns)"});
            current["spark_error_phase"] = json!("collect");
            current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?
                .remove("executed");
        }
        RetainedDifference::UnixDateOfTimestampNtz => {
            if case["expected"]["condition"] != "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
                || case["spark_error_phase"] != "sql"
            {
                return Err(format!("expected Spark's rejected UNIX_DATE argument: {case}").into());
            }
            let rows = match id {
                "literal_unix_date" => json!([["19724"]]),
                "required_unix_date" => days_of(metadata, 1, |_| true)?,
                _ => days_of(metadata, 2, |_| true)?,
            };
            current["expected"] = json!({"status": "ok", "rows": rows});
            current
                .as_object_mut()
                .ok_or("expected a record")?
                .remove("spark_error_phase");
        }
        RetainedDifference::EmptyInputNullability => {
            if case["schema_stages"]["optimized"]["fields"][0]["nullable"] != false {
                return Err(format!("expected Spark's folded NOT NULL constant: {case}").into());
            }
            set_nullable(&mut current, &["optimized", "executed"], true)?;
        }
        RetainedDifference::InListNullability => {
            if case["schema_stages"]["analyzed"]["fields"][1]["nullable"] != true {
                return Err(format!("expected Spark's nullable IN list: {case}").into());
            }
            set_nullable(&mut current, &["analyzed", "optimized", "executed"], false)?;
        }
        RetainedDifference::TruncUnsupportedUnit => {
            let rows = case["expected"]["rows"].as_array().ok_or("expected rows")?;
            if rows.iter().any(|row| !row[1].is_null()) {
                return Err(format!("expected Spark's NULL truncation: {case}").into());
            }
            current["expected"]["rows"] = days_of(metadata, 1, |key| key != "3")?;
        }
        RetainedDifference::UnsupportedExtractField => {
            if case["expected"]["status"] != "ok" {
                return Err(format!("expected Spark's field values: {case}").into());
            }
            let field = UNSUPPORTED_EXTRACT_FIELDS
                .iter()
                .find_map(|(suffix, field)| id.ends_with(suffix).then_some(*field))
                .ok_or("missing field")?;
            current["expected"] = json!({"status": "error",
                "dar_message": format!("Date part '{field}' not supported")});
            current["spark_error_phase"] = json!("collect");
            current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?
                .remove("executed");
        }
    }
    Ok(current)
}

/// The Arrow type family DAR uses for a recorded Spark type.
fn spark_family(spark_type: &Value) -> TestResult<Value> {
    if spark_type["type"] == "array" {
        return Ok(json!({"array": spark_family(&spark_type["elementType"])?,
            "containsNull": spark_type["containsNull"]}));
    }
    let spark_type = spark_type
        .as_str()
        .ok_or("expected a primitive or ARRAY type")?;
    Ok(json!(match spark_type {
        "boolean" => "boolean",
        "integer" => "int32",
        "date" => "date32",
        "string" => "string",
        "timestamp_ntz" => "timestamp_ntz",
        other => return Err(format!("unexpected Spark type {other}").into()),
    }))
}

fn dar_family(data_type: &DataType) -> Value {
    json!(match data_type {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int32 => "int32".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz".to_owned(),
        other => format!("unexpected {other}"),
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

fn dar_fields(fields: &Fields, names: Option<&[String]>) -> Value {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            json!({"name": name, "type": dar_family(field.data_type()),
                "nullable": field.is_nullable(), "metadata": field.metadata()})
        })
        .collect()
}

/// Encode a value as the capture does: INT, DATE days and TIMESTAMP_NTZ wall-clock
/// microseconds as text, BOOLEAN as true or false text.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Boolean => json!(column.as_boolean().value(row).to_string()),
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        other => return Err(format!("unexpected result type {other}").into()),
    })
}

fn options(case: &Value) -> TestResult<SparkSqlOptions> {
    Ok(SparkSqlOptions {
        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        session_timezone: case["timezone"].as_str().ok_or("missing time zone")?.into(),
        ..SparkSqlOptions::default()
    })
}

/// Observe analyzed (public names), optimized and executed fields, typed rows and the
/// failing phase.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    // Spark records an analysis failure as the `sql` stage.
    let mut phase = "sql";
    let result: TestResult<Vec<Vec<Value>>> = async {
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options(case)?,
        )
        .await?;
        observation["analyzed"] = dar_fields(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        );
        phase = "optimized";
        let optimized = optimize(session, &query.logical_plan)?;
        observation["optimized"] =
            dar_fields(optimized.schema().fields(), Some(&query.column_names));
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None);
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

fn optimize(session: &SessionContext, plan: &LogicalPlan) -> TestResult<LogicalPlan> {
    let mut state = session.state();
    let extensions = &mut state.config_mut().options_mut().extensions;
    extensions.insert(SparkSqlExpressionSemantics::default());
    extensions.insert(PreserveAggregateStatisticsSchema::default());
    Ok(state.optimize(plan)?)
}

/// The text DAR's error must contain for a recorded failure: a retained record's pinned text,
/// Spark's error condition, or the recorded message of Java's `ArithmeticException`. Spark's
/// message quotes its plan.
fn expected_error_text(case: &Value) -> Option<String> {
    if let Some(message) = case["expected"]["dar_message"].as_str() {
        return Some(message.to_owned());
    }
    match case["expected"]["condition"].as_str() {
        Some(condition) => Some(format!("[{condition}]")),
        None if case["expected"]["exception"] == "java.lang.ArithmeticException" => {
            case["expected"]["java_message"].as_str().map(str::to_owned)
        }
        None => None,
    }
}

/// The recorded stages and outcome that differ from the observation. A failure must occur
/// at Spark's stage with DAR's message for it.
fn differing_parts(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differing = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if let Some(expected) = case["schema_stages"].get(stage)
            && observation[stage] != spark_fields(expected)?
        {
            differing.push(stage);
        }
    }
    let outcome_matches = match case["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == case["expected"]["rows"],
        Some("error") => {
            observation["error_phase"] == case["spark_error_phase"]
                && expected_error_text(case)
                    .zip(observation["error"].as_str())
                    .is_some_and(|(message, error)| error.contains(&message))
        }
        _ => false,
    };
    if !outcome_matches {
        differing.push("outcome");
    }
    Ok(differing)
}

fn session_context(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

fn register(
    session: &SessionContext,
    name: &str,
    batch: &RecordBatch,
    batch_size: usize,
) -> TestResult {
    let mut partitions = vec![Vec::new(), Vec::new()];
    for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
        partitions[chunk % 2].push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
    }
    session.register_table(
        name,
        Arc::new(MemTable::try_new(batch.schema(), partitions)?),
    )?;
    Ok(())
}

fn register_inputs(session: &SessionContext, metadata: &Value, batch_size: usize) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing table name")?;
        register(session, name, &decode_recorded_batch(input)?, batch_size)?;
    }
    Ok(())
}

/// The recorded inputs with every DATE column as Arrow's Date64, which another producer can
/// give DAR and Spark does not have.
fn register_date64_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing table name")?;
        let batch = decode_recorded_batch(input)?;
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if field.data_type() == &DataType::Date32 {
                fields.push(field.as_ref().clone().with_data_type(DataType::Date64));
                columns.push(arrow::compute::cast(column, &DataType::Date64)?);
            } else {
                fields.push(field.as_ref().clone());
                columns.push(column.clone());
            }
        }
        let batch = RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)?;
        register(session, name, &batch, batch_size)?;
    }
    Ok(())
}

/// A Date64 casts to TIMESTAMP_NTZ like a DATE, with Spark's nullability, so the Spark records
/// of those casts over DATE columns hold for Date64 copies of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date64_values_cast_to_timestamp_ntz_like_dates() -> TestResult {
    const RECORDS: [&str; 6] = [
        "required_cast_to_timestamp_ntz",
        "nullable_cast_to_timestamp_ntz",
        "required_try_cast_to_timestamp_ntz",
        "nullable_try_cast_to_timestamp_ntz",
        "required_to_timestamp_ntz",
        "nullable_to_timestamp_ntz",
    ];
    let records = cast_references()?;
    let cases = records[1..]
        .iter()
        .filter(|case| case["id"].as_str().is_some_and(|id| RECORDS.contains(&id)))
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), RECORDS.len() * 4);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_date64_inputs(&session, &records[0], batch_size)?;
        for case in &cases {
            let observation = observe(&session, case).await;
            let differing = differing_parts(case, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "batch": batch_size, "differing": differing, "observed": observation,
                }));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date_ntz_casts_match_spark() -> TestResult {
    let records = cast_references()?;
    let mut retained = std::collections::BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained,
        std::collections::BTreeMap::from([
            (RetainedDifference::TruncBeyondNanoseconds, 4),
            (RetainedDifference::UnixDateOfTimestampNtz, 12),
            (RetainedDifference::EmptyInputNullability, 4),
            (RetainedDifference::InListNullability, 4),
            (RetainedDifference::TruncUnsupportedUnit, 4),
            (RetainedDifference::UnsupportedExtractField, 36),
        ])
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let expectation = match retained_difference(case) {
                Some(difference) => current_expectation(case, difference, &records[0])?,
                None => case.clone(),
            };
            let observation = observe(&session, case).await;
            let differing = differing_parts(&expectation, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "batch": batch_size, "differing": differing,
                    "expected": expectation["expected"],
                    "schema_stages": expectation["schema_stages"],
                    "observed": observation,
                }));
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
