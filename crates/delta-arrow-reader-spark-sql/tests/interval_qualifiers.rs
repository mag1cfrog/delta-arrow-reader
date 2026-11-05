//! Interval literals and their consumers retain Spark's generated names, fields and values.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Int32Type, Int64Type, IntervalMonthDayNanoType,
    IntervalUnit, IntervalYearMonthType, TimeUnit, TimestampMicrosecondType,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::register_recorded_inputs;

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, validate_batch};

fn interval_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/interval-qualifiers/interval-qualifiers-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 1756);
    Ok(records)
}

/// The Arrow type family Spark itself exports for a recorded Spark type.
///
/// Spark's Arrow conversion maps every day-time interval range to
/// `Duration(Microsecond)` and every year-month range to `Interval(YearMonth)`,
/// so the start and end fields are compared through generated names instead.
fn spark_arrow_family(spark_type: &Value) -> TestResult<&'static str> {
    Ok(
        match spark_type
            .as_str()
            .ok_or("expected a primitive Spark type")?
        {
            "boolean" => "boolean",
            "integer" => "int32",
            "long" => "int64",
            "date" => "date32",
            "timestamp" => "timestamp",
            "timestamp_ntz" => "timestamp_ntz",
            "void" => "null",
            "interval" => "calendar_interval",
            "interval year" | "interval year to month" | "interval month" => "year_month_interval",
            other if other.starts_with("interval ") => "day_time_interval",
            other => return Err(format!("unexpected Spark type {other}").into()),
        },
    )
}

fn dar_arrow_family(data_type: &DataType) -> TestResult<&'static str> {
    Ok(match data_type {
        DataType::Boolean => "boolean",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::Date32 => "date32",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz",
        DataType::Null => "null",
        DataType::Interval(IntervalUnit::MonthDayNano) => "calendar_interval",
        DataType::Interval(IntervalUnit::YearMonth) => "year_month_interval",
        DataType::Duration(TimeUnit::Microsecond) => "day_time_interval",
        other => return Err(format!("unexpected DAR type {other}").into()),
    })
}

/// Describe fields as Spark records them, with types reduced to Arrow families.
fn spark_fields(schema: &Value) -> TestResult<Value> {
    schema["fields"]
        .as_array()
        .ok_or("missing recorded fields")?
        .iter()
        .map(|field| {
            Ok(
                json!({"name": field["name"], "type": spark_arrow_family(&field["type"])?,
                "nullable": field["nullable"], "metadata": field["metadata"]}),
            )
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

fn dar_fields(fields: &arrow::datatypes::Fields, names: Option<&[String]>) -> TestResult<Value> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            Ok(
                json!({"name": name, "type": dar_arrow_family(field.data_type())?,
                "nullable": field.is_nullable(), "metadata": field.metadata()}),
            )
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

/// Encode one value by its Arrow representation, matching the capture's encoding.
fn encode_typed_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Boolean => json!(column.as_boolean().value(row).to_string()),
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Interval(IntervalUnit::YearMonth) => json!(
            column
                .as_primitive::<IntervalYearMonthType>()
                .value(row)
                .to_string()
        ),
        DataType::Duration(TimeUnit::Microsecond) => json!(
            column
                .as_primitive::<DurationMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let value = column.as_primitive::<IntervalMonthDayNanoType>().value(row);
            if value.nanoseconds % 1_000 != 0 {
                return Err(format!("calendar interval below microseconds: {value:?}").into());
            }
            json!([
                value.months.to_string(),
                value.days.to_string(),
                (value.nanoseconds / 1_000).to_string()
            ])
        }
        other => return Err(format!("unexpected interval result type: {other}").into()),
    })
}

/// Observe analyzed (public names), optimized and executed fields, typed rows,
/// and the failing phase.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    let mut phase = "resolve";
    let result: TestResult<Vec<Vec<Value>>> = async {
        let options = SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        };
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options,
        )
        .await?;
        observation["analyzed"] = dar_fields(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        )?;
        phase = "optimized";
        let mut state = session.state();
        let extensions = &mut state.config_mut().options_mut().extensions;
        extensions.insert(SparkSqlExpressionSemantics::default());
        extensions.insert(PreserveAggregateStatisticsSchema::default());
        let optimized = state.optimize(&query.logical_plan)?;
        observation["optimized"] =
            dar_fields(optimized.schema().fields(), Some(&query.column_names))?;
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None)?;
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_typed_value(column.as_ref(), row))
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

/// DAR must fail in Spark's phase, naming Spark's error condition as `[CONDITION]`. For a
/// plain `java.lang.ArithmeticException`, which has no condition, DAR's error must end with
/// the exception's whole message instead. A retained difference names DAR's current error.
fn matches_expected_error(case: &Value, observation: &Value) -> bool {
    let expected_phase = match case["spark_error_phase"].as_str() {
        Some("sql") => "resolve",
        Some(phase) => phase,
        None => return false,
    };
    let expected = &case["expected"];
    let Some(error) = observation["error"].as_str() else {
        return false;
    };
    let identified = if let Some(current) = expected["current_error"].as_str() {
        error.contains(current)
    } else if let Some(condition) = expected["condition"].as_str() {
        error.contains(&format!("[{condition}]"))
    } else if expected["exception"] == "java.lang.ArithmeticException"
        && let Some(message) = expected["exception_message"].as_str()
    {
        error.ends_with(&format!(": {message}"))
    } else {
        false
    };
    observation["error_phase"] == expected_phase && identified
}

/// Compare every recorded stage, typed rows and the required error.
fn compare(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differences = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if let Some(expected) = case["schema_stages"].get(stage)
            && observation[stage] != spark_fields(expected)?
        {
            differences.push(stage);
        }
    }
    let values_match = match case["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == case["expected"]["rows"],
        Some("error") => matches_expected_error(case, observation),
        _ => false,
    };
    if !values_match {
        differences.push("values");
    }
    Ok(differences)
}

/// DAR's current rows for the RANGE offsets retained under #947: a running sum.
const CUMULATIVE_WINDOW_ROWS: [[&str; 2]; 4] = [["0", "2"], ["1", "5"], ["2", "5"], ["3", "10"]];
/// DAR's current error for each literal that Spark rejects at or past a storage limit,
/// retained under #955. Spark fails at the same stage but names a condition that depends on
/// the literal form, or raises a plain `java.lang.ArithmeticException` for multi-field
/// day-time literals. Unit syntax reads the magnitude before the sign, so Spark also rejects
/// the minimum `INTERVAL -9223372036854775808 MICROSECONDS`.
const LIMIT_ERRORS: [(&str, &str); 27] = [
    ("limit_year_below_min", "interval: -178956971"),
    ("limit_year_above_max", "interval: 178956971"),
    ("limit_year_month_below_min", "interval: -178956970-9"),
    ("limit_year_month_above_max", "interval: 178956970-8"),
    ("limit_month_below_min", "interval: -2147483649"),
    ("limit_month_above_max", "interval: 2147483648"),
    ("limit_day_below_min", "interval: -106751992"),
    ("limit_day_above_max", "interval: 106751992"),
    ("limit_day_hour_below_min", "interval: -106751991 05"),
    ("limit_day_hour_above_max", "interval: 106751991 05"),
    ("limit_day_minute_below_min", "interval: -106751991 04:01"),
    ("limit_day_minute_above_max", "interval: 106751991 04:01"),
    (
        "limit_day_second_below_min",
        "interval: -106751991 04:00:54.775809",
    ),
    (
        "limit_day_second_above_max",
        "interval: 106751991 04:00:54.775808",
    ),
    ("limit_hour_below_min", "interval: -2562047789"),
    ("limit_hour_above_max", "interval: 2562047789"),
    ("limit_hour_minute_below_min", "interval: -2562047788:01"),
    ("limit_hour_minute_above_max", "interval: 2562047788:01"),
    (
        "limit_hour_second_below_min",
        "interval: -2562047788:00:54.775809",
    ),
    (
        "limit_hour_second_above_max",
        "interval: 2562047788:00:54.775808",
    ),
    ("limit_minute_below_min", "interval: -153722867281"),
    ("limit_minute_above_max", "interval: 153722867281"),
    (
        "limit_minute_second_below_min",
        "interval: -153722867280:54.775809",
    ),
    (
        "limit_minute_second_above_max",
        "interval: 153722867280:54.775808",
    ),
    ("limit_second_below_min", "interval: -9223372036854.775809"),
    ("limit_second_above_max", "interval: 9223372036854.775808"),
    (
        "limit_microseconds_min_units",
        "literal: 9223372036854775808",
    ),
];

/// DAR's current error for each failing interval scaling or negation constant in inline
/// VALUES, by record identifier prefix, retained under #722. Spark wraps that error in its
/// inline-table condition.
const INLINE_TABLE_CAUSES: [(&str, &str); 8] = [
    ("scaling_divide_zero_values", "[INTERVAL_DIVIDED_BY_ZERO]"),
    ("scaling_multiply_overflow_values", "integer overflow"),
    (
        "scaling_divide_minimum_values",
        "[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION]",
    ),
    (
        "dt_scaling_divide_zero_values",
        "[INTERVAL_DIVIDED_BY_ZERO]",
    ),
    ("dt_scaling_multiply_overflow_values", "long overflow"),
    (
        "dt_scaling_divide_minimum_values",
        "[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION]",
    ),
    (
        "negation_year_month_minimum_values",
        "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION]",
    ),
    (
        "negation_day_time_minimum_values",
        "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION]",
    ),
];

/// Existing differences outside the qualifier-name contract, each with its owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #765: a narrowing interval CAST keeps the source value.
    CastTruncation,
    /// #727: an unaliased window keeps its function-call name.
    WindowName,
    /// #947: RANGE offsets are not typed against the ORDER BY key.
    RangeOffsetTyping,
    /// #955: a literal at or past a storage limit fails without Spark's error condition.
    LimitErrorCondition,
    /// #722: a failing inline VALUES expression reports its own error while optimizing,
    /// without Spark's inline-table condition from analysis.
    InlineTableCondition,
    /// #979: ANSI division by a zero constant fails without Spark's `DIVIDE_BY_ZERO`.
    DivideByZeroCondition,
}

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    match case["context"].as_str()? {
        "cast"
            if [
                "cast_year",
                "cast_day",
                "cast_day_hour",
                "cast_day_minute",
                "cast_hour",
                "cast_hour_minute",
                "cast_minute",
            ]
            .contains(&id) =>
        {
            Some(RetainedDifference::CastTruncation)
        }
        "window" | "window_alias" if id.starts_with("range_numeric") => {
            Some(RetainedDifference::RangeOffsetTyping)
        }
        "window"
            if [
                "range_day",
                "range_hour",
                "range_day_hour",
                "range_day_second",
                "range_year",
                "range_month",
            ]
            .contains(&id) =>
        {
            Some(RetainedDifference::WindowName)
        }
        "date_window"
            if ["date_range_day", "date_range_hour", "date_range_day_hour"].contains(&id) =>
        {
            Some(RetainedDifference::RangeOffsetTyping)
        }
        "limit" if LIMIT_ERRORS.iter().any(|(limit, _)| id == *limit) => {
            Some(RetainedDifference::LimitErrorCondition)
        }
        "scaling_error" | "dt_scaling_error" | "negation_error"
            if id.ends_with("_values") || id.ends_with("_values_filtered") =>
        {
            Some(RetainedDifference::InlineTableCondition)
        }
        "scaling_operand" | "dt_scaling_operand"
            if id.contains("_operand_number_divide_zero")
                && case["expected"]["condition"] == "DIVIDE_BY_ZERO" =>
        {
            Some(RetainedDifference::DivideByZeroCondition)
        }
        _ => None,
    }
}

/// Replace only the retained part of Spark's expectation with the current DAR result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    match difference {
        RetainedDifference::CastTruncation => {
            // The untruncated source: '2-3' YEAR TO MONTH or '2 03:04:05.123456' DAY TO SECOND.
            let source = if case["id"] == "cast_year" {
                "27"
            } else {
                "183845123456"
            };
            current["expected"]["rows"] = json!([[source]]);
        }
        RetainedDifference::WindowName => {
            for stage in ["analyzed", "optimized", "executed"] {
                current["schema_stages"][stage]["fields"][1]["name"] = json!("SUM(v)");
            }
        }
        RetainedDifference::RangeOffsetTyping => {
            current
                .as_object_mut()
                .ok_or("expected a record")?
                .remove("spark_error_phase");
            current["expected"] = json!({"status": "ok", "rows": CUMULATIVE_WINDOW_ROWS});
        }
        RetainedDifference::LimitErrorCondition => {
            let (_, error) = LIMIT_ERRORS
                .iter()
                .find(|(limit, _)| case["id"] == *limit)
                .ok_or("expected a pinned limit error")?;
            if case["expected"]["status"] != "error" {
                return Err(format!("expected a Spark error: {case}").into());
            }
            current["expected"] = json!({"status": "error",
                "current_error": format!("External error: invalid argument: {error}")});
        }
        RetainedDifference::InlineTableCondition => {
            let (_, cause) = INLINE_TABLE_CAUSES
                .iter()
                .find(|(prefix, _)| case["id"].as_str().is_some_and(|id| id.starts_with(prefix)))
                .ok_or("expected a pinned inline-table cause")?;
            if case["expected"]["condition"]
                != "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION"
            {
                return Err(format!("expected Spark's inline-table condition: {case}").into());
            }
            current["spark_error_phase"] = json!("optimized");
            current["expected"] = json!({"status": "error", "current_error": cause});
        }
        RetainedDifference::DivideByZeroCondition => {
            current["expected"] =
                json!({"status": "error", "current_error": "Arrow error: Divide by zero error"});
        }
    }
    Ok(current)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interval_qualifiers_match_spark() -> TestResult {
    let records = interval_references()?;
    let mut retained = std::collections::BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained.into_iter().collect::<Vec<_>>(),
        [
            (RetainedDifference::CastTruncation, 14),
            (RetainedDifference::WindowName, 12),
            (RetainedDifference::RangeOffsetTyping, 10),
            (RetainedDifference::LimitErrorCondition, 54),
            (RetainedDifference::InlineTableCondition, 28),
            (RetainedDifference::DivideByZeroCondition, 12),
        ]
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
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
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let observation = observe(&session, case).await;
            let expectation = match retained_difference(case) {
                Some(difference) => current_expectation(case, difference)?,
                None => case.clone(),
            };
            let differing = compare(&expectation, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "context": case["context"], "ansi": case["ansi"],
                    "batch": batch_size, "sql": case["sql"], "differing": differing,
                    "expected": case["expected"], "spark_error_phase": case["spark_error_phase"],
                    "schema_stages": case["schema_stages"], "observed": observation,
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

/// Interval strings converted at run time choose their family by non-zero values, so a
/// zero-month `window` duration is the same one-hour day-time duration as `'1 hour'`, and
/// both place the timestamp in the window from 00:00 to 01:00.
#[tokio::test]
async fn window_durations_accept_zero_month_interval_strings() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let options = SparkSqlOptions {
        session_timezone: "UTC".into(),
        ..SparkSqlOptions::default()
    };
    let mut windows = Vec::new();
    for duration in ["0 months 1 hour", "1 hour"] {
        let sql = format!("SELECT window(TIMESTAMP '2020-01-01 00:30:00', '{duration}') AS w");
        let query = plan_query(&session, &sql, &options).await?;
        let physical = query.create_physical_plan(&session).await?;
        let batches = collect(physical, session.task_ctx()).await?;
        windows.push(datafusion::arrow::util::pretty::pretty_format_batches(&batches)?.to_string());
    }
    assert_eq!(windows[0], windows[1]);
    assert!(
        windows[0].contains("2020-01-01T00:00:00") && windows[0].contains("2020-01-01T01:00:00"),
        "{}",
        windows[0]
    );
    Ok(())
}

/// Spark configuration must not change interval SQL planned by native DataFusion.
#[tokio::test]
async fn native_interval_sql_is_unchanged_by_spark_configuration() -> TestResult {
    let sql = "SELECT INTERVAL '2' YEAR AS a, INTERVAL '51' HOUR AS b, \
        INTERVAL '1 day' AS c, TIMESTAMP '2020-01-01 00:00:00' + INTERVAL '2' DAY AS d";
    let plain = SessionContext::new().sql(sql).await?.collect().await?;
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    )
    .sql(sql)
    .await?
    .collect()
    .await?;
    assert_eq!(plain, configured);
    assert_eq!(plain.len(), 1);
    Ok(())
}

/// Spark configuration must not change how native DataFusion SQL scales intervals, whether
/// DataFusion returns a value or an error.
#[tokio::test]
async fn native_interval_scaling_is_unchanged_by_spark_configuration() -> TestResult {
    async fn outcome(session: &SessionContext, sql: &str) -> String {
        match session.sql(sql).await {
            Ok(frame) => match frame.collect().await {
                Ok(batches) => datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                    .map_or_else(|error| error.to_string(), |table| table.to_string()),
                Err(error) => error.to_string(),
            },
            Err(error) => error.to_string(),
        }
    }
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT INTERVAL '2' YEAR * 2",
        "SELECT 2 * INTERVAL '27' MONTH",
        "SELECT INTERVAL '27' MONTH / 2",
        "SELECT INTERVAL '2' DAY * 2",
        "SELECT INTERVAL '2' SECOND / 3",
    ] {
        assert_eq!(
            outcome(&SessionContext::new(), sql).await,
            outcome(&configured, sql).await,
            "{sql}"
        );
    }
    Ok(())
}
