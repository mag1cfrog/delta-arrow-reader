//! UNIX_TIMESTAMP and TO_UNIX_TIMESTAMP with a format over a DATE, TIMESTAMP or TIMESTAMP_NTZ
//! ignore the format and declare Spark's nullability: nullable at every stage over a column
//! outside ANSI mode, and in ANSI mode for a nullable argument or format, and over a literal
//! until constant folding, unless the format has a column. The format's aggregates and window
//! functions are computed, as Spark computes them, and a grouping set can make the result
//! nullable.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Fields, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
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
const RECORD_COUNT: usize = 608;

fn formatted_unix_timestamp_references() -> TestResult<Vec<Value>> {
    let records = include_str!(
        "fixtures/formatted-unix-timestamp-nullability/formatted-unix-timestamp-nullability-spark.jsonl"
    )
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

/// #1221: ORDER BY an aggregate that is not selected fails while analyzing, which the
/// format's kept aggregate now reaches; Spark sorts the rows.
const ORDER_BY_AGGREGATE_RECORDS: [&str; 2] = [
    "order_by_aggregate_unix_timestamp_nullable_format",
    "order_by_aggregate_to_unix_timestamp_nullable_format",
];

fn is_retained(case: &Value) -> bool {
    case["id"]
        .as_str()
        .is_some_and(|id| ORDER_BY_AGGREGATE_RECORDS.contains(&id))
}

/// #1233: Spark computes a derived format column below a subquery's sort, so with ANSI enabled the
/// failing CAST fails while collecting rows; DAR moves the call below the sort, drops the unused
/// format and returns the dates' seconds, in each session zone.
const SORTED_RECORDS: [&str; 2] = [
    "derived_sorted_unix_timestamp_format",
    "derived_sorted_to_unix_timestamp_format",
];
const SORTED_SECONDS: [(&str, [&str; 3]); 2] = [
    ("UTC", ["1704153600", "1710028800", "-86400"]),
    (
        "America/Los_Angeles",
        ["1704182400", "1710057600", "-57600"],
    ),
];

fn is_sorted_record(case: &Value) -> bool {
    case["ansi"] == true
        && case["id"]
            .as_str()
            .is_some_and(|id| SORTED_RECORDS.contains(&id))
}

/// The expectation for a record: Spark's, for a #1221 record DAR's schema error while
/// analyzing, or for a #1233 record DAR's rows.
fn expectation(case: &Value) -> TestResult<Value> {
    if is_sorted_record(case) {
        if case["expected"]["status"] != "error"
            || case["expected"]["condition"] != "CAST_INVALID_INPUT"
            || case["spark_error_phase"] != "collect"
        {
            return Err(format!("expected Spark's CAST_INVALID_INPUT: {case}").into());
        }
        let (_, seconds) = SORTED_SECONDS
            .iter()
            .find(|(zone, _)| case["timezone"] == *zone)
            .ok_or("missing session zone")?;
        let rows = seconds
            .iter()
            .enumerate()
            .map(|(key, seconds)| json!([key.to_string(), seconds]))
            .collect::<Vec<_>>();
        let mut current = case.clone();
        current["expected"] = json!({"status": "ok", "rows": rows});
        return Ok(current);
    }
    if !is_retained(case) {
        return Ok(case.clone());
    }
    if case["expected"]["status"] != "ok" {
        return Err(format!("expected Spark's sorted rows: {case}").into());
    }
    let mut current = case.clone();
    current["expected"] = json!({"status": "error",
        "dar_messages": ["Schema error: No field named"]});
    current["spark_error_phase"] = json!("sql");
    current["schema_stages"] = json!({});
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
/// microseconds as text.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
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
async fn formatted_unix_timestamp_nullability_matches_spark() -> TestResult {
    let records = formatted_unix_timestamp_references()?;
    let cases = records[1..].iter().collect::<Vec<_>>();
    assert_eq!(cases.iter().filter(|case| is_retained(case)).count(), 8);
    assert_eq!(
        cases.iter().filter(|case| is_sorted_record(case)).count(),
        4
    );
    assert_no_differences(&differences_over(&records, &cases, None).await?);
    Ok(())
}

/// A Date64 behaves like a DATE, so the Spark records over the DATE columns hold for Date64
/// copies of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date64_arguments_match_their_date_records() -> TestResult {
    let records = formatted_unix_timestamp_references()?;
    let cases = records[1..]
        .iter()
        .filter(|case| {
            case["id"].as_str().is_some_and(|id| {
                ["unix_timestamp", "to_unix_timestamp"]
                    .iter()
                    .any(|function| {
                        id.starts_with(&format!("column_{function}_date_"))
                            || id.starts_with(&format!("column_{function}_nullable_date_"))
                    })
            })
        })
        .collect::<Vec<_>>();
    assert!(!cases.is_empty());
    assert_no_differences(&differences_over(&records, &cases, Some(date64_encoding)).await?);
    Ok(())
}
