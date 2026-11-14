//! TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS results are annotated with the
//! session time zone like Spark, so zone-dependent functions over them read local date-times.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray, TimestampMicrosecondArray};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, DecimalType, Field, Fields, Float64Type, Int32Type,
    Int64Type, Schema, TimeUnit, TimestampMicrosecondType,
};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan};
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

fn epoch_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/timestamp-from-epoch/timestamp-from-epoch-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 552);
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
/// wall-clock microseconds, DOUBLE as its IEEE 754 bits and DECIMAL as plain text.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Boolean => json!(column.as_boolean().value(row).to_string()),
        DataType::Float64 => json!(format!(
            "{:016x}",
            column.as_primitive::<Float64Type>().value(row).to_bits()
        )),
        DataType::Decimal128(precision, scale) => json!(Decimal128Type::format_decimal(
            column.as_primitive::<Decimal128Type>().value(row),
            *precision,
            *scale,
        )),
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

/// The message DAR reports for a recorded Spark failure: Java's `ArithmeticException`
/// message for an overflow.
fn dar_message(case: &Value) -> Option<&str> {
    (case["expected"]["exception"] == "java.lang.ArithmeticException").then_some("long overflow")
}

/// The recorded stages and outcome that differ from the observation. A failure must occur
/// at Spark's stage with DAR's message for it.
fn differing_parts(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let id = text(case, "id")?;
    if id.starts_with("seconds_double_")
        || id.starts_with("seconds_decimal_")
        || matches!(id, "literal_seconds_fraction" | "literal_seconds_double")
        || id.ends_with("_overflow")
    {
        return numeric_conversion_differences(case, observation);
    }
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

/// Differences outside the session zone, each named for DAR's current behavior and owned by
/// an issue. A record can have several.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #939: DATE_FORMAT prints year 0 as 0001 and year 10000 without its sign.
    YearOfEraText,
    /// #1016: a literal formatted or cast to STRING is nullable at analysis.
    NullableTextLiterals,
}

fn text<'v>(value: &'v Value, name: &str) -> TestResult<&'v str> {
    value[name]
        .as_str()
        .ok_or_else(|| format!("missing {name}").into())
}

/// DAR's current result for a record: Spark's record with each retained part replaced by
/// DAR's result, and the differences that changed it.
fn current_expectation(case: &Value) -> TestResult<(Value, Vec<RetainedDifference>)> {
    let mut current = case.clone();
    let mut retained = Vec::new();
    let id = text(case, "id")?;
    if id.ends_with("date_format")
        && let Some(rows) = current["expected"]["rows"].as_array_mut()
    {
        let mut changed = false;
        for row in rows.iter_mut() {
            let value = row.as_array_mut().ok_or("missing row")?.last_mut();
            let Some(value) = value else { continue };
            let Some(formatted) = value.as_str() else {
                continue;
            };
            let year_of_era = if let Some(unsigned) = formatted.strip_prefix('+') {
                unsigned.to_owned()
            } else if let Some(rest) = formatted.strip_prefix("0000-") {
                format!("0001-{rest}")
            } else {
                continue;
            };
            *value = json!(year_of_era);
            changed = true;
        }
        if changed {
            rows.sort_by_key(|row| row.to_string());
            retained.push(RetainedDifference::YearOfEraText);
        }
    }
    if id.starts_with("literal_")
        && (id.ends_with("cast_string") || id.ends_with("date_format"))
        && current["schema_stages"]["analyzed"]["fields"][0]["nullable"] == false
    {
        set_result_nullable(&mut current, "analyzed", true);
        retained.push(RetainedDifference::NullableTextLiterals);
    }
    Ok((current, retained))
}

fn set_result_nullable(case: &mut Value, stage: &str, nullable: bool) {
    if let Some(fields) = case["schema_stages"][stage]["fields"].as_array_mut() {
        for field in fields.iter_mut().filter(|field| field["name"] == "r") {
            field["nullable"] = json!(nullable);
        }
    }
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

/// #1111 compares final values and executed types; schema metadata and failure stages are
/// outside its acceptance. Overflow must fail instead of producing a wrapped timestamp.
fn numeric_conversion_differences(
    case: &Value,
    observation: &Value,
) -> TestResult<Vec<&'static str>> {
    let mut differing = Vec::new();
    if case["expected"]["status"] == "error" {
        let marker = if case["id"] == "decimal_submicro" {
            "fractional microseconds"
        } else {
            "overflow"
        };
        if !observation["error"]
            .as_str()
            .is_some_and(|error| error.contains(marker))
        {
            differing.push("numeric failure");
        }
    } else {
        let expected: Vec<_> = spark_fields(&case["schema_stages"]["executed"])?
            .as_array()
            .ok_or("missing expected fields")?
            .iter()
            .map(|field| field["type"].clone())
            .collect();
        let actual: Vec<_> = observation["executed"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|field| field["type"].clone())
            .collect();
        if actual != expected {
            differing.push("executed types");
        }
        if observation["rows"] != case["expected"]["rows"] {
            differing.push("rows");
        }
    }
    Ok(differing)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn numeric_epoch_conversions_match_spark() -> TestResult {
    let records = include_str!("fixtures/epoch-fractional-seconds/spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    let requests: Value = serde_json::from_str(include_str!(
        "fixtures/epoch-fractional-seconds/requests.json"
    ))?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 92);
    let requested: Vec<_> = requests["corpora"]
        .as_array()
        .ok_or("missing corpora")?
        .iter()
        .flat_map(|corpus| corpus["cases"].as_array().unwrap())
        .collect();
    assert_eq!(requested.len(), 92);
    for (case, request) in records[1..].iter().zip(requested) {
        for key in ["corpus", "id", "sql", "ansi", "timezone", "ordered"] {
            assert_eq!(case[key], request[key], "case identity {key}");
        }
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &requests["corpora"][0]["metadata"], batch_size)?;
        for case in &records[1..] {
            let observation = observe(&session, case).await;
            println!(
                "epoch_observation={}",
                json!({"id": case["id"],
                "timezone": case["timezone"], "ansi": case["ansi"], "batch": batch_size,
                "expected": case["expected"], "executed": case["schema_stages"]["executed"],
                "actual": observation})
            );
            let differing = numeric_conversion_differences(case, &observation)?;
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
async fn fractional_epoch_literal_bounds_remain_folded() -> TestResult {
    let session = session_context(32);
    let data_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![Field::new("t", data_type, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(
            TimestampMicrosecondArray::from(vec![1_704_200_645_250_000]).with_timezone("UTC"),
        )],
    )?;
    register(&session, "events", &batch, 32)?;
    for argument in ["1704200645.25", "CAST(1704200645.25 AS DOUBLE)"] {
        let query = plan_query(
            &session,
            &format!("SELECT t FROM events WHERE t >= timestamp_seconds({argument})"),
            &SparkSqlOptions::default(),
        )
        .await?;
        let optimized = optimize(&session, &query.logical_plan)?;
        let mut bounds = Vec::new();
        optimized.apply(|plan| {
            if let LogicalPlan::Filter(filter) = plan {
                bounds.push(matches!(&filter.predicate, Expr::BinaryExpr(binary)
                    if matches!(binary.right.as_ref(),
                        Expr::Literal(ScalarValue::TimestampMicrosecond(
                            Some(1_704_200_645_250_000), Some(zone)), _) if zone.as_ref() == "UTC")));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(bounds, vec![true], "literal timestamp bound: {optimized}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_timestamps_follow_the_session_zone_like_spark() -> TestResult {
    let records = epoch_references()?;
    let mut retained = BTreeMap::new();
    let mut expectations = Vec::new();
    for case in &records[1..] {
        let (expectation, differences) = current_expectation(case)?;
        for difference in differences {
            *retained.entry(difference).or_insert(0) += 1;
        }
        expectations.push(expectation);
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for (case, expectation) in records[1..].iter().zip(&expectations) {
            let observation = observe(&session, case).await;
            let differing = differing_parts(expectation, &observation)?;
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
    assert_eq!(
        retained,
        BTreeMap::from([
            (RetainedDifference::YearOfEraText, 28),
            (RetainedDifference::NullableTextLiterals, 18),
        ])
    );
    Ok(())
}

/// Spark rejects a STRING argument of TIMESTAMP_MICROS during analysis (#1111). DAR keeps
/// reading it as a UTC date-time, labelled UTC, instead of in the session zone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_micros_arguments_keep_the_utc_reading() -> TestResult {
    let session = session_context(32);
    let options = SparkSqlOptions {
        session_timezone: "America/Los_Angeles".into(),
        ..SparkSqlOptions::default()
    };
    let query = plan_query(
        &session,
        "SELECT timestamp_micros('2024-01-02 13:04:05') AS r",
        &options,
    )
    .await?;
    let physical = query.create_physical_plan(&session).await?;
    let batches = collect(physical, session.task_ctx()).await?;
    let batch = batches.first().ok_or("missing batch")?;
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        encode_value(batch.column(0).as_ref(), 0)?,
        json!("1704200645000000")
    );
    Ok(())
}
