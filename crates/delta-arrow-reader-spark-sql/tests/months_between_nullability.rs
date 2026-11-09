//! MONTHS_BETWEEN declares Spark's nullability at every stage: NOT NULL for NOT NULL DATE,
//! TIMESTAMP and TIMESTAMP_NTZ arguments and `roundOff`, nullable for a nullable one or a STRING
//! argument, which Spark casts to TIMESTAMP, until constant folding replaces a literal's.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Fields, Float64Type, Int32Type, Int64Type, TimeUnit,
    TimestampMicrosecondType,
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

/// The number of recorded observations.
const RECORD_COUNT: usize = 248;

fn months_between_references() -> TestResult<Vec<Value>> {
    let records =
        include_str!("fixtures/months-between-nullability/months-between-nullability-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, RECORD_COUNT);
    Ok(records)
}

/// Values DAR still gives in America/Los_Angeles, each owned by an issue: a record, the row of
/// the value, Spark's value and DAR's. DAR casts a STRING argument to TIMESTAMP_NTZ (#942) and
/// keeps a TIMESTAMP_NTZ (#1228), not a TIMESTAMP in the session zone, so on a change day it
/// counts wall-clock seconds where Spark counts the instant's, an hour apart.
type RetainedValue = (&'static str, usize, &'static str, &'static str);

const RETAINED_VALUES: [RetainedValue; 3] = [
    (
        "column_string_by_nullable",
        1,
        "c010a4791e4ef586",
        "c010a5d976a954a3",
    ),
    (
        "literal_timestamp_ntz_spring_change_day",
        0,
        "3fd9b86e1b10a798",
        "3fd9ce73a0b6995a",
    ),
    (
        "literal_timestamp_ntz_autumn_change_day",
        0,
        "4018d81605d0cf66",
        "4018d6b5ad76704a",
    ),
];

fn retained_value(case: &Value) -> Option<&'static RetainedValue> {
    if case["timezone"] != "America/Los_Angeles" {
        return None;
    }
    RETAINED_VALUES.iter().find(|(id, ..)| case["id"] == *id)
}

/// #1229: DAR reads `roundOff` on every row, so with ANSI enabled the row whose argument is
/// NULL divides by zero, where Spark gives NULL without reading `roundOff`.
const ROUND_OFF_ORDER_RECORD: &str = "column_nullable_date_failing_round_off";
const ROUND_OFF_ORDER_ERROR: &str = "Divide by zero error";
/// Spark's rows for that record: zero months, and NULL for the NULL argument.
const ROUND_OFF_ORDER_SPARK_ROWS: [[Option<&str>; 2]; 3] = [
    [Some("0"), Some("0000000000000000")],
    [Some("1"), Some("0000000000000000")],
    [Some("2"), None],
];

fn is_round_off_order_record(case: &Value) -> bool {
    case["id"] == ROUND_OFF_ORDER_RECORD && case["ansi"] == true
}

/// The expectation for a record: Spark's, or for a retained record DAR's value in its row or
/// DAR's error.
fn expectation(case: &Value) -> TestResult<Value> {
    if is_round_off_order_record(case) {
        if case["expected"]["status"] != "ok"
            || case["expected"]["rows"] != json!(ROUND_OFF_ORDER_SPARK_ROWS)
        {
            return Err(format!("expected Spark's rows: {case}").into());
        }
        let mut current = case.clone();
        current["expected"] = json!({"status": "error", "dar_message": ROUND_OFF_ORDER_ERROR});
        current["spark_error_phase"] = json!("collect");
        current["schema_stages"]
            .as_object_mut()
            .ok_or("missing schema stages")?
            .remove("executed");
        return Ok(current);
    }
    let Some((_, row, spark, dar)) = retained_value(case) else {
        return Ok(case.clone());
    };
    let mut current = case.clone();
    let value = current["expected"]["rows"][*row]
        .as_array_mut()
        .and_then(|row| row.last_mut())
        .ok_or("missing retained row")?;
    if *value != *spark {
        return Err(format!("expected Spark's value {spark}: {case}").into());
    }
    *value = json!(dar);
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
        "long" => "int64",
        "double" => "float64",
        "date" => "date32",
        "string" => "string",
        "timestamp" => "timestamp",
        other => return Err(format!("unexpected Spark type {other}").into()),
    }))
}

/// A TIMESTAMP result must be annotated with the session zone, as Spark's Arrow output is.
fn dar_family(data_type: &DataType, timezone: &str) -> Value {
    json!(match data_type {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Float64 => "float64".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == timezone => {
            "timestamp".to_owned()
        }
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

fn dar_fields(fields: &Fields, names: Option<&[String]>, timezone: &str) -> Value {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            json!({"name": name, "type": dar_family(field.data_type(), timezone),
                "nullable": field.is_nullable(), "metadata": field.metadata()})
        })
        .collect()
}

/// Encode a value as the capture does: INT, BIGINT, DATE days and TIMESTAMP UTC-epoch
/// microseconds as text, and a DOUBLE as the hex of its IEEE 754 bits.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Float64 => json!(format!(
            "{:016x}",
            column.as_primitive::<Float64Type>().value(row).to_bits()
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
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
        let timezone = case["timezone"].as_str().ok_or("missing time zone")?;
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options(case)?,
        )
        .await?;
        observation["analyzed"] = dar_fields(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
            timezone,
        );
        phase = "optimized";
        let optimized = optimize(session, &query.logical_plan)?;
        observation["optimized"] = dar_fields(
            optimized.schema().fields(),
            Some(&query.column_names),
            timezone,
        );
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None, timezone);
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

/// The text DAR's error must contain for a recorded failure: a retained record's pinned text, or
/// Spark's error condition. Spark's message quotes its plan.
fn expected_error_text(case: &Value) -> Option<String> {
    if let Some(message) = case["expected"]["dar_message"].as_str() {
        return Some(message.to_owned());
    }
    case["expected"]["condition"]
        .as_str()
        .map(|condition| format!("[{condition}]"))
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
                && observation["error"]
                    .as_str()
                    .zip(expected_error_text(case))
                    .is_some_and(|(error, text)| error.contains(&text))
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

/// The type a DAR source gives a recorded column type, if it differs.
type Encoding = fn(&DataType) -> Option<DataType>;

/// The recorded inputs with each column `encoding` gives another type cast to that type, as
/// DAR's sources give it.
fn register_encoded_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
    encoding: Encoding,
) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing table name")?;
        let batch = decode_recorded_batch(input)?;
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if let Some(encoded) = encoding(field.data_type()) {
                fields.push(field.as_ref().clone().with_data_type(encoded.clone()));
                columns.push(arrow::compute::cast(column, &encoded)?);
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

/// Dictionary encoding, as Delta's provider gives a STRING partition column.
fn dictionary_encoding(data_type: &DataType) -> Option<DataType> {
    (data_type == &DataType::Utf8)
        .then(|| DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)))
}

/// String views, as Delta's reader gives a STRING data column by default.
fn string_view_encoding(data_type: &DataType) -> Option<DataType> {
    (data_type == &DataType::Utf8).then_some(DataType::Utf8View)
}

/// Large strings, as a registered Arrow table can give a STRING column.
fn large_string_encoding(data_type: &DataType) -> Option<DataType> {
    (data_type == &DataType::Utf8).then_some(DataType::LargeUtf8)
}

/// Arrow's Date64, which another producer can give for a DATE.
fn date64_encoding(data_type: &DataType) -> Option<DataType> {
    (data_type == &DataType::Date32).then_some(DataType::Date64)
}

async fn differences_over(
    records: &[Value],
    cases: &[&Value],
    encoding: Option<Encoding>,
) -> TestResult<Vec<Value>> {
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        match encoding {
            Some(encoding) => register_encoded_inputs(&session, &records[0], batch_size, encoding)?,
            None => register_inputs(&session, &records[0], batch_size)?,
        }
        for case in cases {
            let observation = observe(&session, case).await;
            let differing = differing_parts(&expectation(case)?, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "batch": batch_size, "differing": differing,
                    "expected": case["expected"], "schema_stages": case["schema_stages"],
                    "observed": observation,
                }));
            }
        }
    }
    Ok(differences)
}

fn assert_no_differences(differences: &[Value]) {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn months_between_matches_spark() -> TestResult {
    let records = months_between_references()?;
    let cases = records[1..].iter().collect::<Vec<_>>();
    assert_eq!(
        cases
            .iter()
            .filter(|case| retained_value(case).is_some())
            .count(),
        6
    );
    assert_eq!(
        cases
            .iter()
            .filter(|case| is_round_off_order_record(case))
            .count(),
        2
    );
    assert_no_differences(&differences_over(&records, &cases, None).await?);
    Ok(())
}

/// A Date64 behaves like a DATE, so every Spark record holds over Date64 copies of the DATE
/// columns `dt` and `dtn`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date64_arguments_match_their_date_records() -> TestResult {
    let records = months_between_references()?;
    let cases = records[1..].iter().collect::<Vec<_>>();
    assert_no_differences(&differences_over(&records, &cases, Some(date64_encoding)).await?);
    Ok(())
}

/// A dictionary-encoded, viewed or large STRING behaves like a STRING argument, so every Spark
/// record holds over copies of the inputs with their STRING columns so encoded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encoded_strings_match_their_string_records() -> TestResult {
    let records = months_between_references()?;
    let cases = records[1..].iter().collect::<Vec<_>>();
    let encodings: [Encoding; 3] = [
        dictionary_encoding,
        string_view_encoding,
        large_string_encoding,
    ];
    let mut differences = Vec::new();
    for encoding in encodings {
        differences.extend(differences_over(&records, &cases, Some(encoding)).await?);
    }
    assert_no_differences(&differences);
    Ok(())
}
