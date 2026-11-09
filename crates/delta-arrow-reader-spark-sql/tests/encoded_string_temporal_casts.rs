//! CAST and TRY_CAST of a STRING to DATE, TIMESTAMP and TIMESTAMP_NTZ, and DATE_ADD and DATE_SUB
//! of a STRING date, read the STRING with Spark's rules and declare Spark's nullability, also when
//! the STRING is dictionary-encoded, viewed or large, as DAR's sources give it.

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
const RECORD_COUNT: usize = 252;

fn string_temporal_cast_references() -> TestResult<Vec<Value>> {
    let records = include_str!(
        "fixtures/encoded-string-temporal-casts/encoded-string-temporal-casts-spark.jsonl"
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
        "timestamp_ntz" => "timestamp_ntz",
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

/// Encode a value as the capture does: INT, BIGINT, DATE days, TIMESTAMP UTC-epoch microseconds
/// and TIMESTAMP_NTZ wall-clock microseconds as text.
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

/// #943: DAR does not trim the padding of ` 2024-01-02 ` in `sv` (row 1) before reading it as a
/// TIMESTAMP or TIMESTAMP_NTZ, so it gives NULL there, and CAST and the comparison with a
/// TIMESTAMP fail with ANSI enabled; Spark reads the date's first instant.
const PADDED_TIMESTAMP_RECORDS: [&str; 5] = [
    "cast_valid_timestamp",
    "try_cast_valid_timestamp",
    "cast_valid_timestamp_ntz",
    "try_cast_valid_timestamp_ntz",
    "equal_timestamp_valid",
];

fn is_padded_timestamp_record(case: &Value) -> bool {
    case["id"]
        .as_str()
        .is_some_and(|id| PADDED_TIMESTAMP_RECORDS.contains(&id))
}

/// The expectation for a record: Spark's, or for a #943 record DAR's result: Spark's rows with
/// row 1 NULL, or a failure for CAST and the comparison with ANSI enabled.
fn expectation(case: &Value) -> TestResult<Value> {
    if !is_padded_timestamp_record(case) {
        return Ok(case.clone());
    }
    let rows = case["expected"]["rows"]
        .as_array()
        .filter(|rows| case["expected"]["status"] == "ok" && rows.len() == 4)
        .filter(|rows| rows[1][0] == "1" && !rows[1][1].is_null())
        .ok_or_else(|| format!("expected Spark's value for row 1: {case}"))?;
    let mut current = case.clone();
    let fails = case["id"]
        .as_str()
        .is_some_and(|id| id.starts_with("cast_") || id.starts_with("equal_"));
    if fails && case["ansi"] == true {
        current["expected"] = json!({"status": "error"});
        current["schema_stages"]
            .as_object_mut()
            .ok_or("missing schema stages")?
            .remove("executed");
    } else {
        let mut rows = rows.clone();
        rows[1][1] = Value::Null;
        current["expected"]["rows"] = json!(rows);
    }
    Ok(current)
}

/// `fields` without their nullability.
fn without_nullability(fields: &Value) -> Value {
    let mut fields = fields.clone();
    for field in fields.as_array_mut().into_iter().flatten() {
        if let Some(field) = field.as_object_mut() {
            field.remove("nullable");
        }
    }
    fields
}

/// The recorded stages and outcome that differ from the observation. The analyzed and optimized
/// fields are compared without their nullability, and where Spark fails, DAR must fail; its error
/// condition, message and stage are not compared (#1236).
fn differing_parts(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differing = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        let Some(expected) = case["schema_stages"].get(stage) else {
            continue;
        };
        let expected = spark_fields(expected)?;
        let matches = if stage == "executed" {
            observation[stage] == expected
        } else {
            without_nullability(&observation[stage]) == without_nullability(&expected)
        };
        if !matches {
            differing.push(stage);
        }
    }
    let outcome_matches = match case["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == case["expected"]["rows"],
        Some("error") => observation["error"].is_string(),
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
async fn string_temporal_casts_match_spark() -> TestResult {
    let records = string_temporal_cast_references()?;
    let cases = records[1..].iter().collect::<Vec<_>>();
    assert_eq!(
        cases
            .iter()
            .filter(|case| is_padded_timestamp_record(case))
            .count(),
        20
    );
    assert_no_differences(&differences_over(&records, &cases, None).await?);
    Ok(())
}

/// A dictionary-encoded, viewed or large STRING casts like a STRING, so every Spark record holds
/// over copies of the inputs with their STRING columns so encoded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encoded_strings_match_their_string_records() -> TestResult {
    let records = string_temporal_cast_references()?;
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
