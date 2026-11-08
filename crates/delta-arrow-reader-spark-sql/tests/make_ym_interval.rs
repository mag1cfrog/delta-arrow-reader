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
    assert_eq!(records.len() - 1, 320);
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
    /// #1166: DAR plans INT() of a STRING as Arrow's cast, declared NOT NULL for a NOT NULL
    /// STRING where Spark's cast is nullable, so MAKE_YM_INTERVAL over it is NOT NULL too. IS
    /// NULL over it folds to false, where Spark gives true outside ANSI and fails under ANSI.
    /// Over a dictionary-encoded column, Arrow's cast converts every dictionary value, so a
    /// malformed value fails even in rows the filter removed.
    IntFunctionCast,
    /// #1151: TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY and TRY_DIVIDE over an overflowing
    /// MAKE_YM_INTERVAL fail with its overflow, while optimizing for a constant and while
    /// collecting rows otherwise, where Spark's `TryEval` gives NULL.
    TryArithmetic,
    /// #1157: under ANSI, a constant call with a malformed STRING beside a NULL that a constant
    /// expression computes, `NULL + 1` or an explicit STRING-to-INT cast of NULL, fails while
    /// optimizing, where Spark's NullPropagation gives NULL.
    ComputedNullBesideFailingConstant,
    /// #1148: under ANSI at batch size 32, DAR casts every row's years before the months, so a
    /// later row's malformed year is reported before an earlier row's malformed month. At batch
    /// size 1 DAR reports Spark's value.
    ArgumentErrorRowOrder,
}

/// DAR's argument types in its planning error for each record of the #1165 class.
const ADMISSION_RECORDS: [(&str, &str); 3] = [
    ("literal_decimal", "Decimal128(2, 1)"),
    ("literal_double", "Float64"),
    ("literal_boolean", "Boolean"),
];

/// DAR's current name for each unaliased record of the #749 class.
const GENERATED_NAME_RECORDS: [(&str, &str); 1] = [("name_year", "make_ym_interval(1)")];

/// Records of the #1166 class: MAKE_YM_INTERVAL of INT() over NOT NULL STRING values, and IS
/// NULL over it where the value is malformed.
const INT_FUNCTION_RECORD: &str = "column_required_int_function";
const INT_FUNCTION_IS_NULL_RECORD: &str = "column_required_int_function_is_null";

/// Records of the #1151 class and the stage at which DAR fails.
const TRY_ARITHMETIC_RECORDS: [(&str, &str); 6] = [
    ("column_required_try_add_overflow", "collect"),
    ("literal_try_add_overflow", "optimized"),
    ("literal_try_subtract_overflow", "optimized"),
    ("literal_try_multiply_overflow", "optimized"),
    ("literal_try_divide_overflow", "optimized"),
    ("literal_try_add_date_overflow", "optimized"),
];

/// The #1157 records under ANSI.
const COMPUTED_NULL_RECORDS: [&str; 2] = [
    "literal_string_malformed_computed_null_month",
    "literal_string_malformed_cast_null_month",
];

/// The #1148 record under ANSI: row 0 has a malformed month and row 3 a malformed year.
const ROW_ORDER_RECORD: &str = "column_required_string_failures_in_row_order";

/// Spark's MAKE_YM_INTERVAL overflow message, which DAR repeats.
const OVERFLOW_MESSAGE: &str = "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Integer \
    overflow while operating with intervals. Try devising appropriate values for the interval \
    parameters. SQLSTATE: 22015";

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    if ADMISSION_RECORDS.iter().any(|(record, _)| *record == id) {
        Some(RetainedDifference::ArgumentAdmission)
    } else if GENERATED_NAME_RECORDS
        .iter()
        .any(|(record, _)| *record == id)
    {
        Some(RetainedDifference::GeneratedName)
    } else if id == INT_FUNCTION_RECORD || id == INT_FUNCTION_IS_NULL_RECORD {
        Some(RetainedDifference::IntFunctionCast)
    } else if TRY_ARITHMETIC_RECORDS
        .iter()
        .any(|(record, _)| *record == id)
    {
        Some(RetainedDifference::TryArithmetic)
    } else if id == ROW_ORDER_RECORD && case["ansi"] == true {
        Some(RetainedDifference::ArgumentErrorRowOrder)
    } else if COMPUTED_NULL_RECORDS.contains(&id) && case["ansi"] == true {
        Some(RetainedDifference::ComputedNullBesideFailingConstant)
    } else {
        None
    }
}

/// Replace only the retained part of Spark's expectation with DAR's current result at
/// `batch_size`, over dictionary-encoded STRING columns if `dictionary`.
fn current_expectation(
    case: &Value,
    difference: RetainedDifference,
    batch_size: usize,
    dictionary: bool,
) -> TestResult<Value> {
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
        RetainedDifference::IntFunctionCast if id == INT_FUNCTION_RECORD => {
            for stage in ["analyzed", "optimized", "executed"] {
                let field = &mut current["schema_stages"][stage]["fields"][1];
                if field["type"] != "interval year to month" || field["nullable"] != true {
                    return Err(format!("expected Spark's nullable interval: {case}").into());
                }
                field["nullable"] = json!(false);
            }
            if dictionary {
                // The rows kept, 3 and -3, parse; ' 7 ' is in a removed row.
                current["schema_stages"]
                    .as_object_mut()
                    .ok_or("missing schema stages")?
                    .remove("executed");
                current["expected"] = json!({"status": "error", "dar_message":
                    "Arrow error: Cast error: Cannot cast string ' 7 ' to value of Int32 type"});
                current["spark_error_phase"] = json!("collect");
            }
        }
        RetainedDifference::IntFunctionCast => {
            let malformed = case["expected"]["rows"] == json!([["3", true]])
                || (case["expected"]["condition"] == "CAST_INVALID_INPUT"
                    && case["spark_error_phase"] == "collect");
            if !malformed {
                return Err(format!("expected Spark's NULL or cast failure: {case}").into());
            }
            let stages = current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?;
            let analyzed = stages.get("analyzed").cloned().ok_or("missing analyzed")?;
            stages.insert("executed".to_owned(), analyzed);
            current["expected"] = json!({"status": "ok", "rows": [["3", false]]});
            current["spark_error_phase"] = Value::Null;
        }
        RetainedDifference::TryArithmetic => {
            if case["expected"]["rows"]
                .as_array()
                .and_then(|rows| rows.first()?.as_array()?.last())
                != Some(&Value::Null)
            {
                return Err(format!("expected Spark's NULL: {case}").into());
            }
            let phase = TRY_ARITHMETIC_RECORDS
                .iter()
                .find_map(|(record, phase)| (*record == id).then_some(*phase))
                .ok_or("missing DAR phase")?;
            let stages = current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?;
            stages.remove("executed");
            if phase == "optimized" {
                stages.remove("optimized");
            }
            current["expected"] = json!({"status": "error", "dar_message": OVERFLOW_MESSAGE});
            current["spark_error_phase"] = json!(phase);
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
        RetainedDifference::ArgumentErrorRowOrder => {
            if !case["expected"]["java_message"]
                .as_str()
                .is_some_and(|message| message.starts_with("[CAST_INVALID_INPUT] The value 'bad'"))
            {
                return Err(format!("expected Spark's malformed month: {case}").into());
            }
            if batch_size != 1 {
                current["expected"]["dar_message"] = json!(
                    "[CAST_INVALID_INPUT]\ncaused by\nArrow error: Cast error: Cannot cast \
                     string 'x' to value of Int32 type"
                );
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
            let (value, _) = rest.split_once("L of the type \"BIGINT\"")?;
            Some(format!("Can't cast value {value} to type Int32"))
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
            (RetainedDifference::ArgumentAdmission, 12),
            (RetainedDifference::GeneratedName, 4),
            (RetainedDifference::IntFunctionCast, 8),
            (RetainedDifference::TryArithmetic, 24),
            (RetainedDifference::ArgumentErrorRowOrder, 2),
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
                Some(difference) => current_expectation(case, difference, batch_size, dictionary)?,
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
