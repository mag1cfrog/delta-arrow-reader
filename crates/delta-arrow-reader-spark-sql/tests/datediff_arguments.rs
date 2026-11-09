//! Two-argument DATEDIFF and DATE_DIFF take only arguments Spark casts to DATE: numbers,
//! BOOLEAN, intervals, BINARY, ARRAY and MAP fail while resolving, like Spark, in either
//! position, and so does TIME, whose cast fails. DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING and NULL
//! arguments keep their results, also as DAR's other encodings of them.

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

fn datediff_argument_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/datediff-arguments/datediff-arguments-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 288);
    Ok(records)
}

/// #1003: Spark declares the implicit cast of a TIMESTAMP_NTZ to DATE nullable during analysis,
/// where DAR declares it NOT NULL; constant folding makes both NOT NULL afterwards.
const NTZ_CAST_NULLABILITY_RECORDS: [&str; 1] = ["literal_date_diff_timestamp_ntz"];

/// #1185: a failing sort key reports DAR's dump of the unresolved expression, without Spark's
/// condition, as for an unknown routine there.
const SORT_KEY_RECORD: &str = "column_order_by_datediff_int";
const SORT_KEY_TEXT: &str = "invalid argument: sort expression: SortOrder { child: \
    UnresolvedFunction(UnresolvedFunction { function_name: ObjectName([Identifier(\"datediff\")])";

fn retained_records(case: &Value) -> bool {
    case["id"]
        .as_str()
        .is_some_and(|id| NTZ_CAST_NULLABILITY_RECORDS.contains(&id) || id == SORT_KEY_RECORD)
}

/// DAR's current result for a retained record: for #1003 Spark's, with the analyzed result NOT
/// NULL; for #1185 DAR's error text, at Spark's stage.
fn current_expectation(case: &Value) -> TestResult<Value> {
    let mut current = case.clone();
    if case["id"] == SORT_KEY_RECORD {
        if case["spark_error_phase"] != "sql" {
            return Err(format!("expected Spark's analysis failure: {case}").into());
        }
        current["expected"] = json!({"status": "error", "dar_message": SORT_KEY_TEXT});
        return Ok(current);
    }
    let result = &mut current["schema_stages"]["analyzed"]["fields"][0];
    if result["nullable"] != true
        || case["schema_stages"]["optimized"]["fields"][0]["nullable"] != false
    {
        return Err(format!("expected Spark's nullable analyzed cast: {case}").into());
    }
    result["nullable"] = json!(false);
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
        other => return Err(format!("unexpected Spark type {other}").into()),
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

/// The texts DAR's error must contain for a recorded Spark failure: a retained record's pinned
/// text; otherwise Spark's error condition and, for an argument type, the argument's position
/// as Spark names it; or the recorded message of Java's `ArithmeticException`. Spark's message
/// quotes its plan.
fn expected_error_texts(case: &Value) -> Option<Vec<String>> {
    if let Some(message) = case["expected"]["dar_message"].as_str() {
        return Some(vec![message.to_owned()]);
    }
    let position = |message: &str| {
        ["first", "second"]
            .into_iter()
            .find(|ordinal| message.contains(&format!("The {ordinal} parameter")))
            .map(|ordinal| format!("{ordinal} parameter"))
    };
    match case["expected"]["condition"].as_str() {
        Some(condition) => {
            let mut texts = vec![format!("[{condition}]")];
            if condition == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" {
                texts.extend(position(case["expected"]["java_message"].as_str()?));
            }
            Some(texts)
        }
        None if case["expected"]["exception"] == "java.lang.ArithmeticException" => {
            case["expected"]["java_message"]
                .as_str()
                .map(|message| vec![message.to_owned()])
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

/// The recorded inputs with each column `encoding` gives another type cast to that type, as
/// DAR's sources give it.
fn register_encoded_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
    encoding: fn(&DataType) -> Option<DataType>,
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

/// Dictionary encoding, as Delta's provider gives a STRING or BINARY partition column.
fn dictionary_encoding(data_type: &DataType) -> Option<DataType> {
    matches!(data_type, DataType::Utf8 | DataType::Binary)
        .then(|| DataType::Dictionary(Box::new(DataType::UInt16), Box::new(data_type.clone())))
}

/// String views, as Delta's reader gives a STRING data column by default.
fn string_view_encoding(data_type: &DataType) -> Option<DataType> {
    (data_type == &DataType::Utf8).then_some(DataType::Utf8View)
}

/// Arrow's Date64, which another producer can give for a DATE.
fn date64_encoding(data_type: &DataType) -> Option<DataType> {
    (data_type == &DataType::Date32).then_some(DataType::Date64)
}

/// A dictionary-encoded or viewed STRING casts to DATE like a STRING, a Date64 like a DATE,
/// and a dictionary-encoded BINARY fails like a BINARY, so the Spark records over those columns
/// hold for each encoded copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encoded_arguments_resolve_like_their_spark_types() -> TestResult {
    const RECORDS: [&str; 4] = [
        "column_datediff_string_end",
        "column_date_diff_string_start",
        "column_datediff_binary_end",
        "column_datediff_date",
    ];
    let encodings: [(&str, fn(&DataType) -> Option<DataType>); 3] = [
        ("dictionary", dictionary_encoding),
        ("string view", string_view_encoding),
        ("date64", date64_encoding),
    ];
    let records = datediff_argument_references()?;
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
                let observation = observe(&session, case).await;
                let differing = differing_parts(case, &observation)?;
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
async fn datediff_arguments_resolve_like_spark() -> TestResult {
    let records = datediff_argument_references()?;
    let retained = records[1..]
        .iter()
        .filter(|case| retained_records(case))
        .count();
    assert_eq!(retained, 8);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let expectation = if retained_records(case) {
                current_expectation(case)?
            } else {
                (*case).clone()
            };
            let observation = observe(&session, case).await;
            let differing = differing_parts(&expectation, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "batch": batch_size, "differing": differing,
                    "expected": case["expected"],
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
