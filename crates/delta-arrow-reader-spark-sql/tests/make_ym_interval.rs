//! MAKE_YM_INTERVAL builds Spark's YEAR TO MONTH interval with Spark's argument casts, its
//! overflow error in both ANSI modes, and Spark's nullability at every stage.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Fields, Int32Type, Int64Type, IntervalUnit, IntervalYearMonthType, Schema,
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
    assert_eq!(records.len() - 1, 460);
    Ok(records)
}

/// Differences outside MAKE_YM_INTERVAL's nullability, arithmetic and INT casts, each owned
/// by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #749: Spark names an unaliased call with its arguments and the defaulted months; DAR
    /// keeps the arguments as written.
    GeneratedName,
    /// #1151: TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY and TRY_DIVIDE over an overflowing
    /// MAKE_YM_INTERVAL fail with its overflow, and under ANSI TRY_ADD over a failing argument
    /// cast fails with it, while optimizing for a constant and while collecting rows otherwise,
    /// where Spark's `TryEval` gives NULL.
    TryArithmetic,
    /// #1168: under ANSI, Spark's DECIMAL-to-INT cast moves a value just short of the next integer
    /// away from zero to that integer, as through a DOUBLE, where DAR truncates exactly.
    DecimalCastUnderAnsi,
    /// #1157: under ANSI, a constant call with a malformed STRING beside a NULL that a constant
    /// expression computes, `NULL + 1` or an explicit STRING-to-INT cast of NULL, fails while
    /// optimizing, where Spark's NullPropagation gives NULL.
    ComputedNullBesideFailingConstant,
}

/// DAR's current name for each unaliased record of the #749 class.
const GENERATED_NAME_RECORDS: [(&str, &str); 1] = [("name_year", "make_ym_interval(1)")];

/// Records of the #1151 class and the stage at which DAR fails.
const TRY_ARITHMETIC_RECORDS: [(&str, &str); 6] = [
    ("column_required_try_add_overflow", "collect"),
    ("literal_try_add_overflow", "optimized"),
    ("literal_try_subtract_overflow", "optimized"),
    ("literal_try_multiply_overflow", "optimized"),
    ("literal_try_divide_overflow", "optimized"),
    ("literal_try_add_date_overflow", "optimized"),
];

/// Records of the #1151 class under ANSI only, where an argument cast fails, and the stage at
/// which DAR fails.
const TRY_OVER_FAILING_CAST_RECORDS: [(&str, &str); 2] = [
    ("column_numbers_try_add_double_beyond_int", "collect"),
    ("literal_try_add_double_beyond_int", "optimized"),
];

/// DAR's error for the #1151 records whose DOUBLE argument is beyond INT.
const TRY_OVER_FAILING_CAST_MESSAGE: &str = "[CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: Can't cast value 10000000000.0 to type \
     Int32";

/// Spark's result for a record of the #1168 class.
#[derive(Clone, Copy)]
enum SparkDecimalCastResult {
    /// The rows, as recorded JSON.
    Rows(&'static str),
    /// The condition with which Spark fails while optimizing, where its moved value is beyond a
    /// limit.
    FoldedFailure(&'static str),
}

/// Records of the #1168 class under ANSI, with Spark's result and DAR's rows.
const DECIMAL_CAST_RECORDS: [(&str, SparkDecimalCastResult, &str); 4] = [
    (
        "literal_wide_decimal_near_one",
        SparkDecimalCastResult::Rows(r#"[["12"]]"#),
        r#"[["0"]]"#,
    ),
    (
        "column_wide_decimals",
        SparkDecimalCastResult::Rows(r#"[["0","1"],["1","-1"],["2","3"]]"#),
        r#"[["0","0"],["1","0"],["2","2"]]"#,
    ),
    (
        "literal_decimal_near_int_limit",
        SparkDecimalCastResult::FoldedFailure("CAST_OVERFLOW"),
        r#"[["2147483647"]]"#,
    ),
    (
        "literal_decimal_near_years_limit",
        SparkDecimalCastResult::FoldedFailure("INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION"),
        r#"[["2147483640"]]"#,
    ),
];

/// The #1157 records under ANSI.
const COMPUTED_NULL_RECORDS: [&str; 2] = [
    "literal_string_malformed_computed_null_month",
    "literal_string_malformed_cast_null_month",
];

/// Spark's MAKE_YM_INTERVAL overflow message, which DAR repeats.
const OVERFLOW_MESSAGE: &str = "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Integer \
    overflow while operating with intervals. Try devising appropriate values for the interval \
    parameters. SQLSTATE: 22015";

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    if GENERATED_NAME_RECORDS
        .iter()
        .any(|(record, _)| *record == id)
    {
        Some(RetainedDifference::GeneratedName)
    } else if TRY_ARITHMETIC_RECORDS
        .iter()
        .any(|(record, _)| *record == id)
        || (TRY_OVER_FAILING_CAST_RECORDS
            .iter()
            .any(|(record, _)| *record == id)
            && case["ansi"] == true)
    {
        Some(RetainedDifference::TryArithmetic)
    } else if DECIMAL_CAST_RECORDS
        .iter()
        .any(|(record, _, _)| *record == id)
        && case["ansi"] == true
    {
        Some(RetainedDifference::DecimalCastUnderAnsi)
    } else if COMPUTED_NULL_RECORDS.contains(&id) && case["ansi"] == true {
        Some(RetainedDifference::ComputedNullBesideFailingConstant)
    } else {
        None
    }
}

/// Replace only the retained part of Spark's expectation with DAR's current result, over plain and
/// dictionary-encoded STRING columns alike.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    let id = case["id"].as_str().ok_or("missing record id")?;
    match difference {
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
        RetainedDifference::TryArithmetic => {
            if case["expected"]["rows"]
                .as_array()
                .and_then(|rows| rows.first()?.as_array()?.last())
                != Some(&Value::Null)
            {
                return Err(format!("expected Spark's NULL: {case}").into());
            }
            let (phase, message) = TRY_ARITHMETIC_RECORDS
                .iter()
                .find_map(|(record, phase)| (*record == id).then_some((*phase, OVERFLOW_MESSAGE)))
                .or_else(|| {
                    TRY_OVER_FAILING_CAST_RECORDS
                        .iter()
                        .find_map(|(record, phase)| {
                            (*record == id).then_some((*phase, TRY_OVER_FAILING_CAST_MESSAGE))
                        })
                })
                .ok_or("missing DAR phase")?;
            let stages = current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?;
            stages.remove("executed");
            if phase == "optimized" {
                stages.remove("optimized");
            }
            current["expected"] = json!({"status": "error", "dar_message": message});
            current["spark_error_phase"] = json!(phase);
        }
        RetainedDifference::DecimalCastUnderAnsi => {
            let (spark, dar_rows) = DECIMAL_CAST_RECORDS
                .iter()
                .find_map(|(record, spark, rows)| (*record == id).then_some((*spark, *rows)))
                .ok_or("missing DAR rows")?;
            match spark {
                SparkDecimalCastResult::Rows(rows) => {
                    if case["expected"]["rows"] != serde_json::from_str::<Value>(rows)? {
                        return Err(format!("expected Spark's moved rows: {case}").into());
                    }
                }
                SparkDecimalCastResult::FoldedFailure(condition) => {
                    if case["expected"]["condition"] != condition
                        || case["spark_error_phase"] != "optimized"
                    {
                        return Err(format!("expected Spark's folded failure: {case}").into());
                    }
                    // DAR folds the constant, so its optimized and executed field is NOT NULL.
                    let mut folded = case["schema_stages"]["analyzed"].clone();
                    folded["fields"][0]["nullable"] = json!(false);
                    current["schema_stages"]["optimized"] = folded.clone();
                    current["schema_stages"]["executed"] = folded;
                    current["spark_error_phase"] = Value::Null;
                }
            }
            current["expected"] =
                json!({"status": "ok", "rows": serde_json::from_str::<Value>(dar_rows)?});
        }
        RetainedDifference::ComputedNullBesideFailingConstant => {
            if case["expected"]["rows"] != json!([[null]]) {
                return Err(format!("expected Spark's NULL: {case}").into());
            }
            let stages = current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?;
            stages.remove("optimized");
            stages.remove("executed");
            current["expected"] = json!({"status": "error", "dar_message":
                "[CAST_INVALID_INPUT]\ncaused by\nArrow error: Cast error: Cannot cast string \
                 'x' to value of Int32 type"});
            current["spark_error_phase"] = json!("optimized");
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
        "date" => "date32",
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
        DataType::Date32 => "date32".to_owned(),
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

/// The text with which DAR's error for an INT argument cast names the failing value in Spark's
/// message. The shared casts give Spark's condition over the cast's own cause, not Spark's text
/// (#1147).
fn dar_cast_failure_text(condition: &str, message: &str) -> Option<String> {
    match condition {
        "CAST_INVALID_INPUT" => {
            let (_, rest) = message.split_once("The value '")?;
            let (value, _) = rest.split_once("' of the type \"STRING\"")?;
            Some(format!(
                "Cannot cast string '{value}' to value of Int32 type"
            ))
        }
        "CAST_OVERFLOW" => {
            let (_, rest) = message.split_once("The value ")?;
            let (value, source) = rest.split_once(" of the type \"")?;
            if source.starts_with("BIGINT\"") {
                let value = value.strip_suffix('L')?;
                Some(format!("Can't cast value {value} to type Int32"))
            } else if source.starts_with("DECIMAL(") {
                // DAR names the DECIMAL's integer part.
                let value = value.strip_suffix("BD")?;
                let integer = value.split_once('.').map_or(value, |(integer, _)| integer);
                Some(format!("value of {integer} is out of range Int32"))
            } else if source.starts_with("DOUBLE\"") {
                // Java prints 1.0E10D and Infinity where Rust prints 10000000000.0 and inf.
                let value: f64 = value.strip_suffix('D').unwrap_or(value).parse().ok()?;
                Some(format!("Can't cast value {value:?} to type Int32"))
            } else if source.starts_with("FLOAT\"") {
                // DAR prints a FLOAT as Rust prints an f32.
                let value: f32 = value.parse().ok()?;
                Some(format!("Can't cast value {value:?} to type Int32"))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// The texts DAR's error must contain for a recorded Spark failure: a retained difference's
/// pinned text; the condition alone for an analysis failure, whose message quotes Spark's
/// plan; the condition and the failing value for an argument cast; or Spark's whole message,
/// without the query context on its later lines, for another condition.
fn expected_error_texts(case: &Value) -> Option<Vec<String>> {
    let expected = &case["expected"];
    if let Some(message) = expected["dar_message"].as_str() {
        return Some(vec![message.to_owned()]);
    }
    let message = expected["java_message"].as_str()?.lines().next()?;
    let condition = expected["condition"].as_str()?;
    if case["spark_error_phase"] == "sql" {
        return Some(vec![format!("[{condition}]")]);
    }
    if let Some(text) = dar_cast_failure_text(condition, message) {
        return Some(vec![format!("[{condition}]"), text]);
    }
    message
        .starts_with(&format!("[{condition}]"))
        .then(|| vec![message.to_owned()])
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

/// Dictionary-encode STRING columns as Delta's provider does for STRING partition columns,
/// keeping each field's name, nullability and metadata.
fn dictionary_encoded(batch: &RecordBatch) -> TestResult<RecordBatch> {
    let dictionary = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if field.data_type() == &DataType::Utf8 {
            fields.push(field.as_ref().clone().with_data_type(dictionary.clone()));
            columns.push(cast(column, &dictionary)?);
        } else {
            fields.push(field.as_ref().clone());
            columns.push(Arc::clone(column));
        }
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn register_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
    dictionary: bool,
) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing table name")?;
        let mut batch = decode_recorded_batch(input)?;
        if dictionary {
            batch = dictionary_encoded(&batch)?;
        }
        register(session, name, &batch, batch_size)?;
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
            (RetainedDifference::GeneratedName, 4),
            (RetainedDifference::TryArithmetic, 28),
            (RetainedDifference::DecimalCastUnderAnsi, 8),
            (RetainedDifference::ComputedNullBesideFailingConstant, 4),
        ])
    );
    let mut differences = Vec::new();
    // Every record runs over plain and dictionary-encoded STRING columns.
    for (batch_size, dictionary) in [(1, false), (32, false), (1, true), (32, true)] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size, dictionary)?;
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
                    "batch": batch_size, "dictionary": dictionary, "differing": differing,
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
