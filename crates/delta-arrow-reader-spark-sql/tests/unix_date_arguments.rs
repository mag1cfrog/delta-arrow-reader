//! UNIX_DATE takes only a DATE or an untyped NULL, like Spark, which casts no argument to DATE:
//! TIMESTAMP, TIMESTAMP_NTZ, STRING, TIME, numbers, BOOLEAN, BINARY, intervals, ARRAY and NULLs
//! of those types fail while resolving, in projections and filters. DATE arguments keep their
//! day numbers, also as DAR's other encodings of them.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Fields, Int32Type};
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

fn unix_date_argument_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/unix-date-arguments/unix-date-arguments-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 132);
    Ok(records)
}

/// The Arrow type family DAR uses for a recorded Spark type.
fn spark_family(spark_type: &Value) -> TestResult<Value> {
    Ok(json!(match spark_type.as_str() {
        Some("integer") => "int32",
        _ => return Err(format!("unexpected Spark type {spark_type}").into()),
    }))
}

fn dar_family(data_type: &DataType) -> Value {
    json!(match data_type {
        DataType::Int32 => "int32".to_owned(),
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

/// Encode a value as the capture does: INT as text.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
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

/// The texts DAR's error must contain for a recorded failure: a retained record's pinned text;
/// otherwise Spark's error condition, and for an argument type the function and the argument's
/// position, as Spark's message names them. Spark's message also quotes its plan.
fn expected_error_texts(case: &Value) -> Option<Vec<String>> {
    if let Some(message) = case["expected"]["dar_message"].as_str() {
        return Some(vec![message.to_owned()]);
    }
    let condition = case["expected"]["condition"].as_str()?;
    let mut texts = vec![format!("[{condition}]")];
    if condition == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" {
        let message = case["expected"]["java_message"].as_str()?;
        if !(message.contains("\"unix_date(") && message.contains("The first parameter")) {
            return None;
        }
        texts.extend(["unix_date".to_owned(), "first parameter".to_owned()]);
    }
    Some(texts)
}

/// A rejected argument in a sort key retains the concrete DATE type requirement.
const SORT_KEY_RECORDS: [&str; 2] = ["column_order_by_string", "column_sort_by_string"];
const SORT_KEY_TEXT: &str = "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] `unix_date`: The first parameter requires the \"DATE\" type";

/// The expectation for a record: Spark's, or for a #1185 record DAR's error text at Spark's
/// stage.
fn expectation(case: &Value) -> TestResult<Value> {
    if !case["id"]
        .as_str()
        .is_some_and(|id| SORT_KEY_RECORDS.contains(&id))
    {
        return Ok(case.clone());
    }
    if case["spark_error_phase"] != "sql"
        || case["expected"]["condition"] != "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    {
        return Err(format!("expected Spark's rejected sort key: {case}").into());
    }
    let mut current = case.clone();
    current["expected"] = json!({"status": "error", "dar_message": SORT_KEY_TEXT});
    Ok(current)
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
                && expected_error_texts(case)
                    .zip(observation["error"].as_str())
                    .is_some_and(|(texts, error)| texts.iter().all(|text| error.contains(text)))
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

/// The differences between the records and DAR's observations over `register` inputs.
async fn differences_over(
    records: &[Value],
    cases: &[&Value],
    encoding: Option<(&str, Encoding)>,
) -> TestResult<Vec<Value>> {
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        match encoding {
            Some((_, encoding)) => {
                register_encoded_inputs(&session, &records[0], batch_size, encoding)?
            }
            None => register_inputs(&session, &records[0], batch_size)?,
        }
        for case in cases {
            let expectation = expectation(case)?;
            let observation = observe(&session, case).await;
            let differing = differing_parts(&expectation, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "encoding": encoding.map(|(name, _)| name), "batch": batch_size,
                    "differing": differing, "expected": case["expected"],
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

/// A Date64 resolves like a DATE, and a dictionary-encoded, viewed or large STRING fails like a
/// STRING, so the Spark records over those columns hold for each encoded copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encoded_arguments_resolve_like_their_spark_types() -> TestResult {
    const RECORDS: [&str; 6] = [
        "column_required_date",
        "column_nullable_date",
        "column_date_filter",
        "column_string",
        "column_string_filter",
        "column_string_unreached",
    ];
    let encodings: [(&str, Encoding); 4] = [
        ("dictionary", dictionary_encoding),
        ("string view", string_view_encoding),
        ("large string", large_string_encoding),
        ("date64", date64_encoding),
    ];
    let records = unix_date_argument_references()?;
    let cases = records[1..]
        .iter()
        .filter(|case| case["id"].as_str().is_some_and(|id| RECORDS.contains(&id)))
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), RECORDS.len() * 4);
    let mut differences = Vec::new();
    for encoding in encodings {
        differences.extend(differences_over(&records, &cases, Some(encoding)).await?);
    }
    assert_no_differences(&differences);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unix_date_arguments_resolve_like_spark() -> TestResult {
    let records = unix_date_argument_references()?;
    let cases = records[1..].iter().collect::<Vec<_>>();
    let retained = cases
        .iter()
        .filter(|case| {
            case["id"]
                .as_str()
                .is_some_and(|id| SORT_KEY_RECORDS.contains(&id))
        })
        .count();
    assert_eq!(retained, SORT_KEY_RECORDS.len() * 4);
    assert_no_differences(&differences_over(&records, &cases, None).await?);
    Ok(())
}
