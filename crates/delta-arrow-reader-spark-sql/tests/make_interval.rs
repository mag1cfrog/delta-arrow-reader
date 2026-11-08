//! MAKE_INTERVAL builds Spark's calendar interval with Spark's values, errors and nullability
//! in both ANSI modes.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Fields, Int32Type, Int64Type,
    IntervalMonthDayNanoType, IntervalUnit, TimeUnit, TimestampMicrosecondType,
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

fn make_interval_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/make-interval/make-interval-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 340);
    Ok(records)
}

/// Differences outside MAKE_INTERVAL's own arithmetic, each owned by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1145: Spark returns microseconds beyond the BIGINT nanoseconds of Arrow's calendar
    /// intervals, from INT hours or DECIMAL seconds, or outside ANSI from a BIGINT hour count
    /// that wraps to an INT. DAR fails, while optimizing a constant under ANSI and while
    /// collecting rows otherwise.
    NanosecondRange,
    /// #1133: inside TRY_ADD beside a non-constant operand, Spark folds a constant
    /// MAKE_INTERVAL outside TRY and fails while optimizing under ANSI. DAR returns NULL, as
    /// TRY does for the same MAKE_INTERVAL over columns.
    ConstantOutsideTry,
    /// #1124: under ANSI, the cast of seconds beyond DECIMAL(18, 6) fails at Spark's stage with
    /// Arrow's precision error, without Spark's `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`,
    /// and the cast of a malformed STRING without Spark's `CAST_INVALID_INPUT`.
    DecimalCastCondition,
}

/// DAR's current error for each record of the #1124 Decimal cast class.
const DECIMAL_CAST_RECORDS: [(&str, &str); 5] = [
    (
        "literal_large_seconds",
        "9223372036854.775807 is too large to store in a Decimal128 of precision 18",
    ),
    (
        "literal_bigint_seconds_large",
        "9223372036854775807.000000 is too large to store in a Decimal128 of precision 18",
    ),
    (
        "literal_double_seconds_large",
        "100000000000000004764.729344 is too large to store in a Decimal128 of precision 18",
    ),
    (
        "column_double_seconds_large",
        "100000000000000004764.729344 is too large to store in a Decimal128 of precision 18",
    ),
    (
        "literal_invalid_string_seconds",
        "Cannot cast Some(\"x\") to DECIMAL(18,6)",
    ),
];

fn decimal_cast_message(id: &str) -> Option<&'static str> {
    DECIMAL_CAST_RECORDS
        .iter()
        .find_map(|(record, message)| (*record == id).then_some(*message))
}

/// Records whose microseconds are beyond Arrow's nanoseconds in both ANSI modes.
const NANOSECOND_RANGE_RECORDS: [&str; 3] = [
    "literal_int_hours_beyond_nanoseconds",
    "literal_largest_seconds",
    "column_int_hours_beyond_nanoseconds",
];

/// Records whose BIGINT hour count wraps beyond Arrow's nanoseconds outside ANSI.
const WRAPPED_NANOSECOND_RANGE_RECORDS: [&str; 2] = [
    "literal_bigint_hours_beyond_int",
    "literal_bigint_hours_beyond_int_with_seconds",
];

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    if NANOSECOND_RANGE_RECORDS.contains(&id)
        || (WRAPPED_NANOSECOND_RANGE_RECORDS.contains(&id) && case["ansi"] == false)
    {
        Some(RetainedDifference::NanosecondRange)
    } else if id == "column_try_add_constant_overflow" && case["ansi"] == true {
        Some(RetainedDifference::ConstantOutsideTry)
    } else if decimal_cast_message(id).is_some() && case["ansi"] == true {
        Some(RetainedDifference::DecimalCastCondition)
    } else {
        None
    }
}

/// Replace only the retained part of Spark's expectation with DAR's current result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    match difference {
        RetainedDifference::NanosecondRange => {
            // Spark's single row ends with the interval [months, days, microseconds].
            let micros = case["expected"]["rows"]
                .as_array()
                .filter(|rows| rows.len() == 1)
                .and_then(|rows| rows[0].as_array()?.last()?.get(2)?.as_str())
                .ok_or_else(|| format!("expected one Spark interval: {case}"))?;
            let constant_under_ansi = case["ansi"] == true
                && case["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("literal_"));
            let stages = current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?;
            let analyzed = stages.get("analyzed").cloned().ok_or("missing analyzed")?;
            if constant_under_ansi {
                stages.remove("optimized");
            } else {
                // DAR does not fold the failing value, so its optimized fields keep the
                // analyzed nullability.
                stages.insert("optimized".to_owned(), analyzed);
            }
            stages.remove("executed");
            current["expected"] = json!({"status": "error", "dar_message": format!(
                "MAKE_INTERVAL microseconds {micros} are beyond the calendar interval range of \
                 Arrow's nanoseconds"
            )});
            current["spark_error_phase"] = json!(if constant_under_ansi {
                "optimized"
            } else {
                "collect"
            });
        }
        RetainedDifference::ConstantOutsideTry => {
            if case["expected"]["condition"] != "ARITHMETIC_OVERFLOW"
                || case["spark_error_phase"] != "optimized"
            {
                return Err(format!("expected Spark's folded overflow: {case}").into());
            }
            let stages = current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?;
            let analyzed = stages.get("analyzed").cloned().ok_or("missing analyzed")?;
            stages.insert("optimized".to_owned(), analyzed.clone());
            stages.insert("executed".to_owned(), analyzed);
            // Each row of `required` gives NULL.
            current["expected"] = json!({"status": "ok", "rows": [["0", null], ["1", null]]});
            current["spark_error_phase"] = Value::Null;
        }
        RetainedDifference::DecimalCastCondition => {
            let condition = case["expected"]["condition"].as_str();
            if !matches!(
                condition,
                Some("NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION" | "CAST_INVALID_INPUT")
            ) {
                return Err(format!("expected Spark's DECIMAL cast failure: {case}").into());
            }
            let id = case["id"].as_str().ok_or("missing record id")?;
            let message = decimal_cast_message(id).ok_or("missing DAR message")?;
            current["expected"] = json!({"status": "error", "dar_message": message});
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
        "integer" => "int32",
        "long" => "int64",
        "date" => "date32",
        "string" => "string",
        "timestamp" => "timestamp",
        "timestamp_ntz" => "timestamp_ntz",
        "interval day to second" => "interval day to second",
        "interval" => "calendar interval",
        other => return Err(format!("unexpected Spark type {other}").into()),
    }))
}

/// A TIMESTAMP result must be annotated with the session zone, as Spark's Arrow output is.
fn dar_family(data_type: &DataType, timezone: &str) -> Value {
    json!(match data_type {
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == timezone => {
            "timestamp".to_owned()
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz".to_owned(),
        DataType::Duration(TimeUnit::Microsecond) => "interval day to second".to_owned(),
        DataType::Interval(IntervalUnit::MonthDayNano) => "calendar interval".to_owned(),
        DataType::List(item) => {
            return json!({"array": dar_family(item.data_type(), timezone),
                "containsNull": item.is_nullable()});
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

/// Encode a value as the capture does: TIMESTAMP as UTC-epoch and TIMESTAMP_NTZ as
/// wall-clock microseconds, DAY TO SECOND as microseconds and ARRAY as a list.
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
        DataType::Duration(TimeUnit::Microsecond) => json!(
            column
                .as_primitive::<DurationMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let interval = column.as_primitive::<IntervalMonthDayNanoType>().value(row);
            if interval.nanoseconds % 1_000 != 0 {
                return Err(format!("sub-microsecond calendar interval {interval:?}").into());
            }
            json!([
                interval.months.to_string(),
                interval.days.to_string(),
                (interval.nanoseconds / 1_000).to_string()
            ])
        }
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        DataType::List(_) => {
            let items = column.as_list::<i32>().value(row);
            json!(
                (0..items.len())
                    .map(|item| encode_value(items.as_ref(), item))
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
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

/// Conditions raised by the shared casts of MAKE_INTERVAL's arguments. Their messages give
/// Spark's condition over the cast's own cause, not Spark's text: #1147 owns the INT casts and
/// #1124 the DECIMAL casts.
const CAST_CONDITIONS: [&str; 3] = [
    "CAST_INVALID_INPUT",
    "CAST_OVERFLOW",
    "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION",
];

/// The text DAR's error must contain for a recorded Spark failure: a retained difference's
/// pinned text; the condition alone for an analysis failure, whose message quotes Spark's
/// plan, or for an argument cast; or Spark's whole message for MAKE_INTERVAL's own condition
/// or Java's `ArithmeticException`.
fn expected_error_text(case: &Value) -> Option<String> {
    let expected = &case["expected"];
    if let Some(message) = expected["dar_message"].as_str() {
        return Some(message.to_owned());
    }
    let message = expected["java_message"].as_str()?;
    match expected["condition"].as_str() {
        Some(condition)
            if case["spark_error_phase"] == "sql" || CAST_CONDITIONS.contains(&condition) =>
        {
            Some(format!("[{condition}]"))
        }
        Some(condition) if message.starts_with(&format!("[{condition}]")) => {
            Some(message.to_owned())
        }
        None if expected["exception"] == "java.lang.ArithmeticException" => {
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
async fn make_interval_matches_spark() -> TestResult {
    let records = make_interval_references()?;
    let mut retained = BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained,
        BTreeMap::from([
            (RetainedDifference::NanosecondRange, 16),
            (RetainedDifference::ConstantOutsideTry, 2),
            (RetainedDifference::DecimalCastCondition, 10),
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

/// An unreached CASE branch with a constant overflow does not fail over an empty batch. Spark's
/// `column_case_overflow_without_rows` filters every row out; here the empty batch reaches the
/// CASE itself, and the result must still be Spark's: no rows and the same fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreached_overflow_skips_empty_batches() -> TestResult {
    let records = make_interval_references()?;
    let required = records[0]["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .find(|input| input["name"] == "required")
        .ok_or("missing required table")?;
    let empty = decode_recorded_batch(required)?.slice(0, 0);
    let cases = records[1..]
        .iter()
        .filter(|case| case["id"] == "column_case_overflow_without_rows")
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 4);
    for case in cases {
        let session = session_context(32);
        session.register_table(
            "required",
            Arc::new(MemTable::try_new(
                empty.schema(),
                vec![vec![empty.clone()]],
            )?),
        )?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        let unfiltered = sql
            .strip_suffix(" WHERE k < 0")
            .ok_or("expected the record's filter")?;
        let mut case = case.clone();
        case["sql"] = json!(unfiltered);
        let observation = observe(&session, &case).await;
        let differing = differing_parts(&case, &observation)?;
        assert!(differing.is_empty(), "{differing:?}: {observation}");
    }
    Ok(())
}
