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
    /// without Spark's `CANNOT_PARSE_TIMESTAMP` condition. Which row fails first depends on the
    /// batch size, so either message holds.
    ParserMessage,
    /// #1194: an INT, BOOLEAN, untyped NULL with a format or ARRAY argument is cast to DATE or
    /// fails, where Spark converts it to STRING or rejects it during analysis.
    NonStringArgument,
    /// #1195: with a format outside UTC, a TIMESTAMP_NTZ gives its own DATE, where Spark gives the
    /// session-zone DATE of the instant whose UTC time reads as that value.
    FormattedTimestampNtzZone,
    /// #1196: TO_DATE with a format over a NOT NULL DATE outside ANSI mode is NOT NULL after
    /// optimization, where Spark's is nullable at every stage.
    FormattedToDateNullability,
}

/// DAR's parser messages for the strings `s` holds that TO_DATE cannot parse with a format.
const PARSER_MESSAGES: [&str; 2] = [
    "Execution error: invalid parsed date: input is out of range",
    "Execution error: expected numeric datetime field at byte offset 0",
];

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let ansi = case["ansi"].as_bool()?;
    let utc = case["timezone"] == "UTC";
    Some(match case["id"].as_str()? {
        "nullable_to_date_string_format" if ansi => RetainedDifference::ParserMessage,
        "literal_try_to_date_int"
        | "literal_try_to_date_boolean"
        | "literal_try_to_date_null_format"
        | "literal_try_to_date_array" => RetainedDifference::NonStringArgument,
        "column_try_to_date_timestamp_ntz_format" | "literal_try_to_date_timestamp_ntz_format"
            if !utc =>
        {
            RetainedDifference::FormattedTimestampNtzZone
        }
        "column_to_date_date_format" if !ansi => RetainedDifference::FormattedToDateNullability,
        _ => return None,
    })
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

/// DAR's current failure at `phase`, with text its error must contain; Spark's later stages are
/// dropped.
fn fail_at(current: &mut Value, phase: &str, message: &str) -> TestResult {
    current["expected"] = json!({"status": "error", "dar_messages": [message]});
    current["spark_error_phase"] = json!(phase);
    let stages = current["schema_stages"]
        .as_object_mut()
        .ok_or("missing schema stages")?;
    let later: &[&str] = match phase {
        "optimized" => &["optimized", "executed"],
        "collect" => &["executed"],
        _ => return Err(format!("unexpected phase {phase}").into()),
    };
    for stage in later {
        stages.remove(*stage);
    }
    Ok(())
}

/// The expectation for a record: Spark's, or DAR's current result for a retained record.
fn expectation(case: &Value, metadata: &Value) -> TestResult<Value> {
    let Some(difference) = retained_difference(case) else {
        return Ok(case.clone());
    };
    let mut current = case.clone();
    let spark_ok = case["expected"]["status"] == "ok";
    match difference {
        RetainedDifference::ParserMessage => {
            if case["expected"]["condition"] != "CANNOT_PARSE_TIMESTAMP"
                || case["spark_error_phase"] != "collect"
            {
                return Err(format!("expected Spark's parse failure: {case}").into());
            }
            current["expected"] = json!({"status": "error", "dar_messages": PARSER_MESSAGES});
        }
        RetainedDifference::NonStringArgument => match case["id"].as_str() {
            Some("literal_try_to_date_int") if spark_ok => {
                current["expected"] = json!({"status": "ok", "rows": [["1"]]});
                set_nullable(&mut current, &["analyzed", "optimized", "executed"], false)?;
            }
            Some("literal_try_to_date_boolean") if spark_ok => {
                fail_at(
                    &mut current,
                    "optimized",
                    "Unsupported CAST from Boolean to Date32",
                )?;
                set_nullable(&mut current, &["analyzed"], false)?;
            }
            Some("literal_try_to_date_null_format") if spark_ok => {
                fail_at(&mut current, "collect", "expected string scalar for `date`")?;
            }
            Some("literal_try_to_date_array")
                if case["expected"]["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" =>
            {
                current["expected"] = json!({"status": "error",
                    "dar_messages": ["Unsupported CAST from List(non-null Utf8) to Date32"]});
                current["spark_error_phase"] = json!("optimized");
            }
            _ => return Err(format!("expected Spark's converted argument: {case}").into()),
        },
        RetainedDifference::FormattedTimestampNtzZone => {
            // Each TIMESTAMP_NTZ value's own day: `tn` is column 4 of `inputs`, and the literal
            // is TIMESTAMP_NTZ '2024-01-03 03:00:00'.
            let rows = if case["id"] == "literal_try_to_date_timestamp_ntz_format" {
                json!([["19725"]])
            } else {
                let mut rows = metadata["inputs"][0]["rows"]
                    .as_array()
                    .ok_or("missing inputs rows")?
                    .iter()
                    .map(|row| {
                        let micros = row[4].as_str().ok_or("missing tn")?.parse::<i64>()?;
                        Ok(json!([
                            row[0],
                            micros.div_euclid(86_400_000_000).to_string()
                        ]))
                    })
                    .collect::<TestResult<Vec<_>>>()?;
                rows.sort_by_key(Value::to_string);
                json!(rows)
            };
            if !spark_ok || case["expected"]["rows"] == rows {
                return Err(format!("expected Spark's session-zone dates: {case}").into());
            }
            current["expected"] = json!({"status": "ok", "rows": rows});
        }
        RetainedDifference::FormattedToDateNullability => {
            if case["schema_stages"]["optimized"]["fields"][1]["nullable"] != true {
                return Err(format!("expected Spark's nullable result: {case}").into());
            }
            set_nullable(&mut current, &["optimized", "executed"], false)?;
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
                let expectation = expectation(case, &records[0])?;
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
            (RetainedDifference::NonStringArgument, 16),
            (RetainedDifference::FormattedTimestampNtzZone, 4),
            (RetainedDifference::FormattedToDateNullability, 2),
        ])
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let expectation = expectation(case, &records[0])?;
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
