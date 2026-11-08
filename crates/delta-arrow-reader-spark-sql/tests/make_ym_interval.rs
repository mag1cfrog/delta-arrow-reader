//! MAKE_YM_INTERVAL builds Spark's YEAR TO MONTH interval with Spark's argument casts, its
//! overflow error in both ANSI modes, and Spark's nullability at every stage.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Fields, Int32Type, Int64Type, IntervalUnit, IntervalYearMonthType,
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

fn make_ym_interval_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/make-ym-interval/make-ym-interval-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 232);
    Ok(records)
}

/// Differences outside MAKE_YM_INTERVAL's nullability, arithmetic and INT casts, each owned
/// by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1165: Spark casts a DECIMAL or DOUBLE argument to INT and rejects a BOOLEAN with
    /// `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`. DAR rejects all three while planning, without
    /// Spark's condition.
    ArgumentAdmission,
    /// #749: Spark names an unaliased call with its arguments and the defaulted months; DAR
    /// keeps the arguments as written.
    GeneratedName,
}

/// DAR's argument types in its planning error for each record of the #1165 class.
const ADMISSION_RECORDS: [(&str, &str); 3] = [
    ("literal_decimal", "Decimal128(2, 1)"),
    ("literal_double", "Float64"),
    ("literal_boolean", "Boolean"),
];

/// DAR's current name for each unaliased record of the #749 class.
const GENERATED_NAME_RECORDS: [(&str, &str); 1] = [("name_year", "make_ym_interval(1)")];

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    if ADMISSION_RECORDS.iter().any(|(record, _)| *record == id) {
        Some(RetainedDifference::ArgumentAdmission)
    } else if GENERATED_NAME_RECORDS
        .iter()
        .any(|(record, _)| *record == id)
    {
        Some(RetainedDifference::GeneratedName)
    } else {
        None
    }
}

/// Replace only the retained part of Spark's expectation with DAR's current result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    let id = case["id"].as_str().ok_or("missing record id")?;
    match difference {
        RetainedDifference::ArgumentAdmission => {
            let rejected = case["expected"]["condition"]
                == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
                && case["spark_error_phase"] == "sql";
            let cast = case["expected"]["rows"] == json!([["12"]]);
            if !(rejected || cast) {
                return Err(format!("expected Spark's cast or type mismatch: {case}").into());
            }
            let types = ADMISSION_RECORDS
                .iter()
                .find_map(|(record, types)| (*record == id).then_some(*types))
                .ok_or("missing DAR argument types")?;
            current["schema_stages"] = json!({});
            current["expected"] = json!({"status": "error", "dar_message": format!(
                "Function 'spark_make_ym_interval' user-defined coercion failed with: Error \
                 during planning: The arguments of Spark `make_ym_interval` must be integer or \
                 string or null. No function matches the given name and argument types \
                 'spark_make_ym_interval({types})'"
            )});
            current["spark_error_phase"] = json!("sql");
        }
        RetainedDifference::GeneratedName => {
            let name = GENERATED_NAME_RECORDS
                .iter()
                .find_map(|(record, name)| (*record == id).then_some(*name))
                .ok_or("missing DAR name")?;
            for stage in ["analyzed", "optimized", "executed"] {
                let fields = current["schema_stages"][stage]["fields"]
                    .as_array_mut()
                    .ok_or("missing fields")?;
                let field = fields.last_mut().ok_or("missing interval field")?;
                // Spark lists the defaulted months.
                if field["name"] != "make_ym_interval(1, 0)" {
                    return Err(format!("expected Spark's generated name: {case}").into());
                }
                field["name"] = json!(name);
            }
        }
    }
    Ok(current)
}

/// The Arrow type family DAR uses for a recorded Spark type.
fn spark_family(spark_type: &Value) -> TestResult<Value> {
    let spark_type = spark_type.as_str().ok_or("expected a primitive type")?;
    Ok(json!(match spark_type {
        "integer" => "int32",
        "long" => "int64",
        "string" => "string",
        "boolean" => "boolean",
        "interval year to month" => "interval year to month",
        other => return Err(format!("unexpected Spark type {other}").into()),
    }))
}

fn dar_family(data_type: &DataType) -> Value {
    json!(match data_type {
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_owned(),
        DataType::Boolean => "boolean".to_owned(),
        DataType::Interval(IntervalUnit::YearMonth) => "interval year to month".to_owned(),
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

/// Encode a value as the capture does: YEAR TO MONTH as its months.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Interval(IntervalUnit::YearMonth) => json!(
            column
                .as_primitive::<IntervalYearMonthType>()
                .value(row)
                .to_string()
        ),
        DataType::Boolean => json!(column.as_boolean().value(row)),
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
/// failing phase in the record's session time zone.
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

/// Conditions raised by the shared INT casts of MAKE_YM_INTERVAL's arguments. Their messages
/// give Spark's condition over the cast's own cause, not Spark's text (#1147).
const CAST_CONDITIONS: [&str; 2] = ["CAST_INVALID_INPUT", "CAST_OVERFLOW"];

/// The text DAR's error must contain for a recorded Spark failure: a retained difference's
/// pinned text; the condition alone for an analysis failure, whose message quotes Spark's
/// plan, or for an argument cast; or Spark's whole message, without the query context on its
/// later lines, for another condition.
fn expected_error_text(case: &Value) -> Option<String> {
    let expected = &case["expected"];
    if let Some(message) = expected["dar_message"].as_str() {
        return Some(message.to_owned());
    }
    let message = expected["java_message"].as_str()?.lines().next()?;
    match expected["condition"].as_str() {
        Some(condition)
            if case["spark_error_phase"] == "sql" || CAST_CONDITIONS.contains(&condition) =>
        {
            Some(format!("[{condition}]"))
        }
        Some(condition) if message.starts_with(&format!("[{condition}]")) => {
            Some(message.to_owned())
        }
        _ => None,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn make_ym_interval_matches_spark() -> TestResult {
    let records = make_ym_interval_references()?;
    let mut retained = BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained,
        BTreeMap::from([
            (RetainedDifference::ArgumentAdmission, 12),
            (RetainedDifference::GeneratedName, 4),
        ])
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let expectation = match retained_difference(case) {
                Some(difference) => current_expectation(case, difference)?,
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
