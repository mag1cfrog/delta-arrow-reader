//! TIMESTAMP values convert to TIMESTAMP_NTZ at their local date-time in the session time
//! zone like Spark, in casts, one-argument TO_TIMESTAMP_NTZ and CONVERT_TIMEZONE.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Date32Type, Field, Fields, Int32Type, Int64Type, Schema, TimeUnit,
    TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
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

/// Zones DAR annotates the registered instants with. Delta reads TIMESTAMP as UTC; the others
/// are a named zone with historical offsets and its current fixed offset.
const ANNOTATIONS: [&str; 3] = ["UTC", "Asia/Kathmandu", "+05:45"];

fn conversion_references() -> TestResult<Vec<Value>> {
    let records =
        include_str!("fixtures/timestamp-ntz-conversion/timestamp-ntz-conversion-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 542);
    Ok(records)
}

/// The Arrow type family DAR uses for a recorded Spark type.
fn spark_family(spark_type: &Value) -> TestResult<String> {
    let spark_type = spark_type.as_str().ok_or("expected a primitive type")?;
    Ok(match spark_type {
        "integer" => "int32".to_owned(),
        "long" => "int64".to_owned(),
        "boolean" => "boolean".to_owned(),
        "date" => "date32".to_owned(),
        "string" => "string".to_owned(),
        "timestamp" => "timestamp".to_owned(),
        "timestamp_ntz" => "timestamp_ntz".to_owned(),
        other => return Err(format!("unexpected Spark type {other}").into()),
    })
}

/// A TIMESTAMP result must be annotated with the session zone, as Spark's Arrow output is.
fn dar_family(data_type: &DataType, timezone: &str) -> String {
    match data_type {
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Boolean => "boolean".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == timezone => {
            "timestamp".to_owned()
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz".to_owned(),
        other => format!("unexpected {other}"),
    }
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
/// wall-clock microseconds.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Boolean => json!(column.as_boolean().value(row).to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
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
    let mut phase = "resolve";
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
        let mut windows = 0;
        optimized.apply(|plan| {
            if matches!(plan, LogicalPlan::Window(_)) {
                windows += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        observation["window_operators"] = json!(windows);
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

/// The message DAR reports for a recorded Spark failure. Spark's overflow while storing a
/// local date-time is Java's `ArithmeticException`, whose message DAR repeats.
fn dar_message(case: &Value) -> Option<&str> {
    match case["expected"]["exception"].as_str() {
        Some("java.lang.ArithmeticException") => Some("long overflow"),
        _ => case["expected"]["dar_message"].as_str(),
    }
}

/// Preserve the original schema/error controls; repaired #1107 cases compare final values
/// and executed types without metadata, nullable flags or error-stage matching.
/// An omitted stage makes no claim; an explicit NULL requires that stage to be unavailable.
fn differing_parts(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    if matches!(
        case["id"].as_str(),
        Some(
            "column_to_timestamp_ntz_format"
                | "column_to_timestamp_format"
                | "literal_to_timestamp_ntz_format"
                | "column_timestamp_function"
                | "literal_timestamp_function"
        )
    ) {
        return temporal_argument_differences(case, observation);
    }
    let mut differing = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if let Some(expected) = case["schema_stages"].get(stage) {
            let matches = if expected.is_null() {
                observation[stage].is_null()
            } else {
                observation[stage] == spark_fields(expected)?
            };
            if !matches {
                differing.push(stage);
            }
        }
    }
    let outcome_matches = match case["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == case["expected"]["rows"],
        Some("error") => {
            observation["error_phase"] == case["spark_error_phase"]
                && dar_message(case)
                    .zip(observation["error"].as_str())
                    .is_some_and(|(message, error)| error.contains(message))
        }
        _ => false,
    };
    if !outcome_matches {
        differing.push("outcome");
    }
    Ok(differing)
}

/// Differences outside this conversion, each owned by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1029: in a session zone Arrow cannot parse, such as `PST` or `GMT+08:00`, a cast
    /// keeps each instant's UTC date-time.
    UnparsedSessionZone,
    /// #1103: a cast of an ARRAY, STRUCT or MAP keeps the UTC date-time of its TIMESTAMP
    /// values.
    NestedUtcDateTime,
    /// #1030: a TIMESTAMP_NTZ beyond the date library's years converts to NULL in a named
    /// zone, as a cast to TIMESTAMP or in CONVERT_TIMEZONE.
    OutOfRangeZoneConversion,
    /// #1104: for a TIMESTAMP_NTZ beyond the date library's years, HOUR is NULL, CAST to
    /// STRING gives `ERROR` and CAST to DATE fails while collecting rows.
    OutOfRangeCalendar,
    /// #1105: a comparison of a converted value with a TIMESTAMP_NTZ literal compares local
    /// date-times, where Spark's optimizer compares the TIMESTAMP with the literal's earlier
    /// instant. A later instant of a repeated local date-time equals the literal, and an
    /// extreme instant fails with `long overflow`, or is NULL under TRY_CAST.
    LocalDateTimeComparison,
    /// #1106: a constant that overflows inside CONVERT_TIMEZONE fails while collecting rows.
    LateConvertTimezoneOverflow,
    /// #1106: TRY_CAST of a NOT NULL TIMESTAMP that overflows puts NULL in a NOT NULL field,
    /// which fails while collecting rows. Spark returns -1.
    NullInRequiredTryCast,
}

/// Comparisons with the repeated Los Angeles local date-time 2024-11-03 01:30 that Spark
/// answers differently for its later instant, key 4.
const DIFFERING_OVERLAP_COMPARISONS: [&str; 9] = [
    "column_overlap_equals",
    "column_overlap_not_equals",
    "column_overlap_null_safe",
    "column_overlap_at_most",
    "column_overlap_greater",
    "column_overlap_reversed",
    "column_overlap_string",
    "column_overlap_try_cast",
    "column_overlap_to_timestamp_ntz",
];

/// Registered instants this far from the epoch lie beyond the date library's years,
/// +-262143; the other registered instants lie within years 1 to 9999.
const OUT_OF_RANGE_MICROS: i64 = 8_000_000_000_000_000_000;

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    let named_zone = case["timezone"] != "UTC";
    let los_angeles = case["timezone"] == "America/Los_Angeles";
    let kathmandu = case["timezone"] == "Asia/Kathmandu";
    if (los_angeles
        && (DIFFERING_OVERLAP_COMPARISONS.contains(&id)
            || matches!(
                id,
                "column_local_mean_time_equals"
                    | "filter_equals"
                    | "extreme_min_at_least"
                    | "extreme_min_try_at_least"
            )))
        || (kathmandu && matches!(id, "extreme_max_at_least" | "extreme_max_try_at_least"))
    {
        return Some(RetainedDifference::LocalDateTimeComparison);
    }
    // The extreme instant whose local date-time overflows: the smallest west of UTC and the
    // largest east of it.
    let overflowing =
        |west: &str, east: &str| (los_angeles && id == west) || (kathmandu && id == east);
    if overflowing(
        "literal_convert_timezone_micros_min",
        "literal_convert_timezone_micros_max",
    ) {
        return Some(RetainedDifference::LateConvertTimezoneOverflow);
    }
    if overflowing(
        "required_extreme_min_try_cast",
        "required_extreme_max_try_cast",
    ) {
        return Some(RetainedDifference::NullInRequiredTryCast);
    }
    match id {
        "column_cast" if matches!(case["timezone"].as_str(), Some("PST" | "GMT+08:00")) => {
            Some(RetainedDifference::UnparsedSessionZone)
        }
        "column_array_cast" | "column_struct_cast" | "column_map_cast" if named_zone => {
            Some(RetainedDifference::NestedUtcDateTime)
        }
        "column_round_trip" if named_zone => Some(RetainedDifference::OutOfRangeZoneConversion),
        "column_convert_timezone_tokyo" | "column_convert_timezone_session" => {
            Some(RetainedDifference::OutOfRangeZoneConversion)
        }
        "column_hour" | "column_cast_string" | "column_cast_date" => {
            Some(RetainedDifference::OutOfRangeCalendar)
        }
        _ => None,
    }
}

/// The registered `ntz_inputs` instants by key.
fn registered_instants(metadata: &Value) -> TestResult<BTreeMap<String, Value>> {
    let input = metadata["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .find(|input| input["name"] == "ntz_inputs")
        .ok_or("missing ntz_inputs")?;
    input["rows"]
        .as_array()
        .ok_or("missing rows")?
        .iter()
        .map(|row| {
            let key = row[0].as_str().ok_or("missing key")?.to_owned();
            Ok((key, row[1].clone()))
        })
        .collect()
}

fn out_of_range(instant: &Value) -> TestResult<bool> {
    Ok(match instant.as_str() {
        Some(micros) => micros.parse::<i64>()?.abs() > OUT_OF_RANGE_MICROS,
        None => false,
    })
}

/// DAR's current result for a record with a retained difference: Spark's record with only
/// the retained part replaced.
fn current_expectation(
    case: &Value,
    difference: RetainedDifference,
    metadata: &Value,
) -> TestResult<Value> {
    let instants = registered_instants(metadata)?;
    let mut current = case.clone();
    let id = case["id"].as_str().ok_or("missing id")?;
    if difference == RetainedDifference::LateConvertTimezoneOverflow {
        current["spark_error_phase"] = json!("collect");
        return Ok(current);
    }
    let failure = match difference {
        // The first out-of-range row fails the query.
        RetainedDifference::OutOfRangeCalendar if id == "column_cast_date" => {
            Some("Cannot convert")
        }
        RetainedDifference::LocalDateTimeComparison
            if matches!(id, "extreme_min_at_least" | "extreme_max_at_least") =>
        {
            Some("long overflow")
        }
        RetainedDifference::NullInRequiredTryCast => {
            Some("declared as non-nullable but contains null values")
        }
        _ => None,
    };
    if let Some(message) = failure {
        current["expected"] = json!({"status": "error", "dar_message": message});
        current["spark_error_phase"] = json!("collect");
        if let Some(stages) = current["schema_stages"].as_object_mut() {
            stages.remove("executed");
        }
        return Ok(current);
    }
    if id == "filter_equals" {
        // Both instants of the repeated hour read 01:30.
        current["expected"]["rows"] = json!([["3"], ["4"]]);
        return Ok(current);
    }
    // The later instant of the repeated local date-time each comparison names.
    let later_instant = match id {
        "column_local_mean_time_equals" => "7",
        _ => "4",
    };
    let rows = current["expected"]["rows"]
        .as_array_mut()
        .ok_or("missing rows")?;
    for row in rows.iter_mut() {
        let key = row[0].as_str().ok_or("missing key")?.to_owned();
        let instant = instants.get(&key).ok_or("unregistered key")?;
        let value = &mut row[1];
        match difference {
            RetainedDifference::UnparsedSessionZone | RetainedDifference::NestedUtcDateTime => {
                // The UTC date-time in microseconds is the instant itself.
                *value = instant.clone();
            }
            RetainedDifference::OutOfRangeZoneConversion if out_of_range(instant)? => {
                *value = Value::Null;
            }
            RetainedDifference::OutOfRangeCalendar if out_of_range(instant)? => {
                *value = if case["id"] == "column_cast_string" {
                    json!("ERROR")
                } else {
                    Value::Null
                };
            }
            // The extreme instants overflow under TRY_CAST.
            RetainedDifference::LocalDateTimeComparison if id.starts_with("extreme_") => {
                *value = Value::Null;
            }
            RetainedDifference::LocalDateTimeComparison if key == later_instant => {
                *value = json!(if *value == "true" { "false" } else { "true" });
            }
            _ => {}
        }
    }
    rows.sort_by_key(|row| row.to_string());
    Ok(current)
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

/// The recorded input with its TIMESTAMP column annotated with `annotation`. The stored
/// instants do not change.
fn annotated_input(input: &Value, annotation: &str) -> TestResult<RecordBatch> {
    let batch = decode_recorded_batch(input)?;
    let annotated = DataType::Timestamp(TimeUnit::Microsecond, Some(annotation.into()));
    let schema = batch.schema();
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if matches!(field.data_type(), DataType::Timestamp(_, Some(_))) {
            fields.push(Field::clone(field).with_data_type(annotated.clone()));
            columns.push(cast(column, &annotated)?);
        } else {
            fields.push(Field::clone(field));
            columns.push(Arc::clone(column));
        }
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Register the recorded inputs with every TIMESTAMP annotated with `annotation`.
fn register_inputs(
    session: &SessionContext,
    metadata: &Value,
    annotation: &str,
    batch_size: usize,
) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing table name")?;
        register(
            session,
            name,
            &annotated_input(input, annotation)?,
            batch_size,
        )?;
    }
    Ok(())
}

/// #1107 compares final values and executed types, including multiplicity from ignored
/// aggregate formats. Schema metadata, nullable flags and error stages are not compared.
fn temporal_argument_differences(
    case: &Value,
    observation: &Value,
) -> TestResult<Vec<&'static str>> {
    assert_eq!(case["expected"]["status"], "ok");
    let expected: Vec<_> = spark_fields(&case["schema_stages"]["executed"])?
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| field["type"].clone())
        .collect();
    let actual: Vec<_> = observation["executed"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|field| field["type"].clone())
        .collect();
    let mut differences = Vec::new();
    if expected != actual {
        differences.push("executed types");
    }
    if observation["rows"] != case["expected"]["rows"] {
        differences.push("rows");
    }
    if case["id"]
        .as_str()
        .is_some_and(|id| id.ends_with("_window"))
        && observation["window_operators"] != 1
    {
        differences.push("window binding");
    }
    Ok(differences)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_temporal_arguments_match_spark() -> TestResult {
    let records = include_str!("fixtures/timestamp-temporal-arguments/spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    let request: Value = serde_json::from_str(include_str!(
        "fixtures/timestamp-temporal-arguments/requests.json"
    ))?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 188);
    let requested: Vec<_> = request["corpora"]
        .as_array()
        .ok_or("missing corpora")?
        .iter()
        .flat_map(|corpus| corpus["cases"].as_array().unwrap())
        .collect();
    assert_eq!(requested.len(), 188);
    for (case, definition) in records[1..].iter().zip(requested) {
        for key in ["corpus", "id", "sql", "ansi", "timezone", "ordered"] {
            assert_eq!(case[key], definition[key], "case identity {key}");
        }
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(
            &session,
            &request["corpora"][0]["metadata"],
            "UTC",
            batch_size,
        )?;
        for case in &records[1..] {
            let observation = observe(&session, case).await;
            println!(
                "timestamp_argument_observation={}",
                json!({"id":case["id"],
                "timezone":case["timezone"], "ansi":case["ansi"], "batch":batch_size,
                "expected":case["expected"], "executed":case["schema_stages"]["executed"],
                "actual":observation})
            );
            let differing = temporal_argument_differences(case, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({"id":case["id"], "timezone":case["timezone"],
                    "ansi":case["ansi"], "batch":batch_size,"differing":differing,
                    "expected":case["expected"], "actual":observation}));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences: {}",
        differences.len(),
        json!(differences)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_timestamp_cast_keeps_its_unzoned_type() -> TestResult {
    let session = session_context(32);
    let batches = session
        .sql("SELECT CAST(1 AS TIMESTAMP) AS r")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    validate_batch(batches[0].schema().as_ref(), &batches[0])?;
    let counts = cast(batches[0].column(0).as_ref(), &DataType::Int64)?;
    assert_eq!(counts.as_primitive::<Int64Type>().value(0), 1_000_000_000);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_ntz_conversions_follow_the_session_zone_like_spark() -> TestResult {
    let records = conversion_references()?;
    let mut retained = BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained,
        BTreeMap::from([
            (RetainedDifference::UnparsedSessionZone, 4),
            (RetainedDifference::NestedUtcDateTime, 12),
            (RetainedDifference::OutOfRangeZoneConversion, 16),
            (RetainedDifference::OutOfRangeCalendar, 18),
            (RetainedDifference::LocalDateTimeComparison, 30),
            (RetainedDifference::LateConvertTimezoneOverflow, 4),
            (RetainedDifference::NullInRequiredTryCast, 4),
        ])
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        for annotation in ANNOTATIONS {
            let session = session_context(batch_size);
            register_inputs(&session, &records[0], annotation, batch_size)?;
            for case in &records[1..] {
                let expectation = match retained_difference(case) {
                    Some(difference) => current_expectation(case, difference, &records[0])?,
                    None => case.clone(),
                };
                let observation = observe(&session, case).await;
                let differing = differing_parts(&expectation, &observation)?;
                if !differing.is_empty() {
                    differences.push(json!({
                        "id": case["id"], "timezone": case["timezone"],
                        "ansi": case["ansi"], "batch": batch_size,
                        "annotation": annotation, "differing": differing,
                        "expected": expectation["expected"],
                        "schema_stages": expectation["schema_stages"],
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

/// Native DataFusion SQL keeps Arrow's cast, which reads the UTC date-time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sql_keeps_the_utc_date_time() -> TestResult {
    let records = conversion_references()?;
    let session = session_context(32);
    register_inputs(&session, &records[0], "America/Los_Angeles", 32)?;
    let frame = session
        .sql("SELECT ts, CAST(ts AS TIMESTAMP) AS local FROM ntz_inputs WHERE k = 0")
        .await?;
    let batches = frame.collect().await?;
    let batch = batches.first().ok_or("missing batch")?;
    assert_eq!(
        batch.schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    let instant = batch.column(0).as_primitive::<TimestampMicrosecondType>();
    let local = batch.column(1).as_primitive::<TimestampNanosecondType>();
    assert_eq!(local.value(0), instant.value(0) * 1_000);
    Ok(())
}
