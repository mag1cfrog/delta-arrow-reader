//! TIMESTAMP_DIFF and TIMESTAMP_ADD name no routine, like Spark: every call fails while
//! resolving, before its arguments, with the function named as written. TIMESTAMPDIFF,
//! TIMESTAMPADD and DATEADD keep their results.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Fields, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, LogicalPlan, Volatility, create_udf};
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

fn routine_name_references() -> TestResult<Vec<Value>> {
    let records =
        include_str!("fixtures/timestamp-routine-names/timestamp-routine-names-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 136);
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
        "timestamp" => "timestamp",
        other => return Err(format!("unexpected Spark type {other}").into()),
    }))
}

/// A TIMESTAMP result must be annotated with the session zone, as Spark's Arrow output is.
fn dar_family(data_type: &DataType, timezone: &str) -> Value {
    json!(match data_type {
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
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

/// Encode a value as the capture does: INT and BIGINT as text, TIMESTAMP as UTC-epoch
/// microseconds.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
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

/// The text DAR's error must contain for a recorded Spark failure: a retained difference's
/// pinned text; for an unresolved routine, Spark's whole message before its query position,
/// with DAR's session catalog in place of Spark's `spark_catalog`.`default`; otherwise Spark's
/// error condition, or the recorded message of Java's `ArithmeticException`.
fn expected_error_text(case: &Value) -> Option<String> {
    if let Some(message) = case["expected"]["dar_message"].as_str() {
        return Some(message.to_owned());
    }
    if case["expected"]["condition"] == "UNRESOLVED_ROUTINE" {
        let message = case["expected"]["java_message"].as_str()?;
        let (message, _) = message.split_once("; line ")?;
        return Some(message.replace("`spark_catalog`.`default`", "`datafusion`.`public`"));
    }
    match case["expected"]["condition"].as_str() {
        Some(condition) => Some(format!("[{condition}]")),
        None if case["expected"]["exception"] == "java.lang.ArithmeticException" => {
            case["expected"]["java_message"].as_str().map(str::to_owned)
        }
        None => None,
    }
}

/// #1185: TIMESTAMP_DIFF and TIMESTAMP_ADD in other call positions, beside an unresolved column
/// or under a qualified name, where Spark reports the routine (or, in LATERAL VIEW, the
/// generator's argument or a missing routine), with DAR's current error text.
const ROUTINE_POSITION_RECORDS: [(&str, &str); 8] = [
    (
        "column_window_timestamp_diff",
        "attribute ObjectName([Identifier(\"DAY\")]) is missing from the schema",
    ),
    (
        "column_window_timestamp_add",
        "not supported: unknown window function: timestamp_add",
    ),
    (
        "column_order_by_timestamp_diff",
        "invalid argument: sort expression: SortOrder { child: UnresolvedFunction(\
         UnresolvedFunction { function_name: ObjectName([Identifier(\"timestamp_diff\")])",
    ),
    (
        "column_sort_by_timestamp_add",
        "invalid argument: sort expression: SortOrder { child: UnresolvedFunction(\
         UnresolvedFunction { function_name: ObjectName([Identifier(\"timestamp_add\")])",
    ),
    (
        "column_lateral_view_timestamp_diff",
        "[UNRESOLVED_ROUTINE] Cannot resolve routine `timestamp_diff` on search path \
         [`system`.`builtin`, `system`.`session`, `datafusion`.`public`]. SQLSTATE: 42883",
    ),
    (
        "column_lateral_view_timestamp_add",
        "[UNRESOLVED_ROUTINE] Cannot resolve routine `timestamp_add` on search path \
         [`system`.`builtin`, `system`.`session`, `datafusion`.`public`]. SQLSTATE: 42883",
    ),
    (
        "literal_unresolved_column_beside_timestamp_diff",
        "attribute ObjectName([Identifier(\"nosuch\")]) is missing from the schema",
    ),
    (
        "literal_qualified_timestamp_add",
        "not supported: qualified function name",
    ),
];

fn routine_position_message(case: &Value) -> Option<&'static str> {
    ROUTINE_POSITION_RECORDS
        .iter()
        .find_map(|(id, message)| (case["id"] == *id).then_some(*message))
}

/// DAR's current failure for a #1185 record, while resolving like Spark's.
fn routine_position_expectation(case: &Value, message: &str) -> TestResult<Value> {
    if case["spark_error_phase"] != "sql" {
        return Err(format!("expected Spark's analysis failure: {case}").into());
    }
    let mut current = case.clone();
    current["expected"] = json!({"status": "error", "dar_message": message});
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_routine_names_resolve_like_spark() -> TestResult {
    let records = routine_name_references()?;
    let retained = records[1..]
        .iter()
        .filter(|case| routine_position_message(case).is_some())
        .count();
    assert_eq!(retained, ROUTINE_POSITION_RECORDS.len() * 4);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let expectation = match routine_position_message(case) {
                Some(message) => routine_position_expectation(case, message)?,
                None => case.clone(),
            };
            let observation = observe(&session, case).await;
            let differing = differing_parts(&expectation, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "batch": batch_size, "differing": differing,
                    "expected": expectation["expected"],
                    "schema_stages": case["schema_stages"],
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

/// A host may register a function named TIMESTAMP_DIFF or TIMESTAMP_ADD. It then resolves like
/// any registered function, under its own name and with its own arguments: no unit keyword is
/// read from its first argument, and it is not named or planned as TIMESTAMPDIFF or
/// TIMESTAMPADD.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_functions_named_timestamp_diff_and_add_resolve_as_registered() -> TestResult {
    let records = routine_name_references()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for name in ["timestamp_diff", "timestamp_add"] {
            // The host function returns its first argument.
            session.register_udf(create_udf(
                name,
                vec![DataType::Int32, DataType::Int32, DataType::Int32],
                DataType::Int32,
                Volatility::Immutable,
                Arc::new(|arguments: &[ColumnarValue]| Ok(arguments[0].clone())),
            ));
        }
        for name in ["timestamp_diff", "timestamp_add"] {
            for ansi in [false, true] {
                let case = json!({"sql": format!("SELECT {name}(k, k, k) FROM inputs"),
                    "ansi": ansi, "timezone": "UTC"});
                let observation = observe(&session, &case).await;
                let field = &observation["analyzed"][0];
                if field["name"] != format!("{name}(k, k, k)")
                    || field["type"] != "int32"
                    || observation["rows"] != json!([["0"], ["1"], ["2"]])
                {
                    differences.push(json!({"name": name, "ansi": ansi, "batch": batch_size,
                        "observed": observation}));
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
