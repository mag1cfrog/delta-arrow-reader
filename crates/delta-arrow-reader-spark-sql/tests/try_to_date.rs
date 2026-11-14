//! TRY_TO_DATE over TIMESTAMP, DATE, TIMESTAMP_NTZ and STRING arguments, with and without a
//! format, gives Spark's values and nullability: a value it cannot parse is NULL in both ANSI
//! modes, and a formatted result over a column is nullable at every stage. TRY_TO_DATE and
//! TO_DATE parse dictionary-encoded, viewed and large STRING columns like STRING columns, and
//! report Spark's condition for the wrong number of arguments.

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

fn try_to_date_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/try-to-date/try-to-date-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 116);
    Ok(records)
}

/// Differences outside TRY_TO_DATE's own planning, each named for DAR's current result and owned
/// by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1057: TO_DATE with a format reports DAR's parser message for a string it cannot parse,
    /// without Spark's `CANNOT_PARSE_TIMESTAMP` condition. Which of two failing rows reports
    /// first varies with the batch size and, at batch size 1, between runs, so either message
    /// holds.
    ParserMessage,
    /// #1236: an ARRAY argument still fails after analysis, where Spark rejects it earlier.
    ArrayArgument,
}

/// DAR's parser messages for the strings `s` holds that TO_DATE cannot parse with a format.
const PARSER_MESSAGES: [&str; 2] = [
    "Execution error: invalid parsed date: input is out of range",
    "Execution error: expected numeric datetime field at byte offset 0",
];

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let ansi = case["ansi"].as_bool()?;
    Some(match case["id"].as_str()? {
        "nullable_to_date_string_format" if ansi => RetainedDifference::ParserMessage,
        "literal_try_to_date_array" => RetainedDifference::ArrayArgument,
        _ => return None,
    })
}

/// The expectation for a record: Spark's, or DAR's current result for a retained record.
fn expectation(case: &Value) -> TestResult<Value> {
    let Some(difference) = retained_difference(case) else {
        return Ok(case.clone());
    };
    let mut current = case.clone();
    match difference {
        RetainedDifference::ParserMessage => {
            if case["expected"]["condition"] != "CANNOT_PARSE_TIMESTAMP"
                || case["spark_error_phase"] != "collect"
            {
                return Err(format!("expected Spark's parse failure: {case}").into());
            }
            current["expected"] = json!({"status": "error", "dar_messages": PARSER_MESSAGES});
        }
        RetainedDifference::ArrayArgument => {
            if case["expected"]["condition"] != "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" {
                return Err(format!("expected Spark array rejection: {case}").into());
            }
            current["expected"] = json!({"status": "error",
                "dar_messages": ["Unsupported CAST from List(non-null Utf8) to Date32"]});
            current["spark_error_phase"] = json!("optimized");
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
        "timestamp" => "timestamp",
        other => return Err(format!("unexpected Spark type {other}").into()),
    }))
}

/// A TIMESTAMP result must be annotated with the session zone, as Spark's Arrow output is.
fn dar_family(data_type: &DataType, timezone: &str) -> Value {
    json!(match data_type {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int32 => "int32".to_owned(),
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

/// Encode a value as the capture does: INT, DATE days and TIMESTAMP UTC-epoch microseconds as
/// text.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
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

/// Final rows and executed DATE types. Parser failures need a parser diagnostic,
/// without requiring the same error wording or stage as Spark.
fn final_date_differences(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differing = Vec::new();
    if case["expected"]["status"] == "error" {
        let markers: &[&str] = if case["expected"]["condition"] == "CAST_INVALID_INPUT" {
            &["[CAST_INVALID_INPUT]"]
        } else {
            &[
                "[CANNOT_PARSE_TIMESTAMP]",
                "invalid parsed date",
                "expected numeric datetime field",
                "expected datetime literal",
                "datetime value does not match format",
            ]
        };
        if !observation["error"]
            .as_str()
            .is_some_and(|error| markers.iter().any(|marker| error.contains(marker)))
        {
            differing.push("parse failure");
        }
    } else {
        let expected: Vec<_> = spark_fields(&case["schema_stages"]["executed"])?
            .as_array()
            .ok_or("missing executed fields")?
            .iter()
            .map(|field| field["type"].clone())
            .collect();
        let actual: Vec<_> = observation["executed"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|field| field["type"].clone())
            .collect();
        if expected != actual {
            differing.push("executed types");
        }
        if observation["rows"] != case["expected"]["rows"] {
            differing.push("rows");
        }
    }
    Ok(differing)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date_argument_coercion_matches_spark() -> TestResult {
    let records = include_str!("fixtures/date-argument-coercion/spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    let request: Value = serde_json::from_str(include_str!(
        "fixtures/date-argument-coercion/requests.json"
    ))?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 296);
    let requested: Vec<_> = request["corpora"]
        .as_array()
        .ok_or("missing corpora")?
        .iter()
        .flat_map(|corpus| corpus["cases"].as_array().unwrap())
        .collect();
    assert_eq!(requested.len(), 296);
    for (case, definition) in records[1..].iter().zip(requested) {
        for key in ["corpus", "id", "sql", "ansi", "timezone", "ordered"] {
            assert_eq!(case[key], definition[key], "case identity {key}");
        }
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &request["corpora"][0]["metadata"], batch_size)?;
        for case in &records[1..] {
            let observation = observe(&session, case).await;
            println!(
                "date_argument_observation={}",
                json!({"id":case["id"],
                "timezone":case["timezone"], "ansi":case["ansi"], "batch":batch_size,
                "expected":case["expected"], "executed":case["schema_stages"]["executed"],
                "actual":observation})
            );
            let differing = final_date_differences(case, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({"id":case["id"], "timezone":case["timezone"],
                    "ansi":case["ansi"], "batch":batch_size,"differing":differing,
                    "expected":case["expected"], "actual":observation}));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences: {}",
        differences.len(),
        json!(differences)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn formatted_date_zones_match_spark() -> TestResult {
    let records = include_str!("fixtures/formatted-date-zones/spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    let request: Value =
        serde_json::from_str(include_str!("fixtures/formatted-date-zones/requests.json"))?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 136);
    let requested: Vec<_> = request["corpora"]
        .as_array()
        .ok_or("missing corpora")?
        .iter()
        .flat_map(|corpus| corpus["cases"].as_array().unwrap())
        .collect();
    assert_eq!(requested.len(), 136);
    for (case, definition) in records[1..].iter().zip(requested) {
        for key in ["corpus", "id", "sql", "ansi", "timezone", "ordered"] {
            assert_eq!(case[key], definition[key], "case identity {key}");
        }
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &request["corpora"][0]["metadata"], batch_size)?;
        for case in &records[1..] {
            let observation = observe(&session, case).await;
            println!(
                "date_zone_observation={}",
                json!({"id":case["id"],
                "timezone":case["timezone"], "ansi":case["ansi"], "batch":batch_size,
                "expected":case["expected"], "executed":case["schema_stages"]["executed"],
                "actual":observation})
            );
            let differing = final_date_differences(case, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({"id":case["id"], "timezone":case["timezone"],
                    "ansi":case["ansi"], "batch":batch_size,"differing":differing,
                    "expected":case["expected"], "actual":observation}));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences: {}",
        differences.len(),
        json!(differences)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_numeric_date_casts_keep_day_counts() -> TestResult {
    let session = session_context(32);
    let batches = session
        .sql("SELECT CAST(1 AS DATE) AS d")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Date32);
    validate_batch(batches[0].schema().as_ref(), &batches[0])?;
    assert_eq!(encode_value(batches[0].column(0).as_ref(), 0)?, json!("1"));
    Ok(())
}

/// The texts one of which DAR's error must contain for a recorded failure: a retained record's
/// pinned texts, or Spark's error condition. Spark's message quotes its plan.
fn expected_error_texts(case: &Value) -> Vec<String> {
    if let Some(messages) = case["expected"]["dar_messages"].as_array() {
        return messages
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
    }
    case["expected"]["condition"]
        .as_str()
        .map(|condition| vec![format!("[{condition}]")])
        .unwrap_or_default()
}

/// Preserve the original schema/error controls. Repaired scalar arguments use #1194's
/// narrower final-value and executed-type comparison.
fn differing_parts(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    if matches!(
        case["id"].as_str(),
        Some(
            "literal_try_to_date_int"
                | "literal_try_to_date_boolean"
                | "literal_try_to_date_null_format"
        )
    ) {
        return final_date_differences(case, observation);
    }
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
                && observation["error"].as_str().is_some_and(|error| {
                    expected_error_texts(case)
                        .iter()
                        .any(|text| error.contains(text.as_str()))
                })
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

/// A dictionary-encoded, viewed or large STRING parses like a STRING, and a Date64 like a DATE,
/// in TRY_TO_DATE and TO_DATE, so the Spark records over those columns hold for each encoded
/// copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encoded_arguments_parse_like_their_spark_types() -> TestResult {
    const RECORDS: [&str; 8] = [
        "column_try_to_date_string",
        "column_try_to_date_string_format",
        "nullable_try_to_date_string",
        "nullable_try_to_date_string_format",
        "nullable_to_date_string",
        "nullable_to_date_string_format",
        "column_try_to_date_date",
        "column_try_to_date_date_format",
    ];
    let encodings: [(&str, Encoding); 4] = [
        ("dictionary", dictionary_encoding),
        ("string view", string_view_encoding),
        ("large string", large_string_encoding),
        ("date64", date64_encoding),
    ];
    let records = try_to_date_references()?;
    let cases = records[1..]
        .iter()
        .filter(|case| case["id"].as_str().is_some_and(|id| RECORDS.contains(&id)))
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), RECORDS.len() * 4);
    let mut differences = Vec::new();
    for (encoding_name, encoding) in encodings {
        for batch_size in [1, 32] {
            let session = session_context(batch_size);
            register_encoded_inputs(&session, &records[0], batch_size, encoding)?;
            for case in &cases {
                let expectation = expectation(case)?;
                let observation = observe(&session, case).await;
                let differing = differing_parts(&expectation, &observation)?;
                if !differing.is_empty() {
                    differences.push(json!({
                        "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                        "encoding": encoding_name, "batch": batch_size, "differing": differing,
                        "observed": observation,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_to_date_matches_spark() -> TestResult {
    let records = try_to_date_references()?;
    let mut retained = std::collections::BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained,
        std::collections::BTreeMap::from([
            (RetainedDifference::ParserMessage, 2),
            (RetainedDifference::ArrayArgument, 4),
        ])
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let expectation = expectation(case)?;
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
