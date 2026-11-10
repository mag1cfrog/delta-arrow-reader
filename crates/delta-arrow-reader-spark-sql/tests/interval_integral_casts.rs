//! CAST and TRY_CAST of year-month and day-time intervals to TINYINT, SMALLINT, INT, BIGINT and
//! DECIMAL count the interval's end field, as Spark does, with Spark's truncation, rounding,
//! overflow errors and nullability (#1152). The qualifier comes from the SQL of a literal or a
//! CAST, from the function that computed the interval, and from the columns of subqueries,
//! common table expressions, VALUES and set operations.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Decimal128Type, Fields, Int8Type, Int16Type, Int32Type, Int64Type,
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

fn interval_integral_cast_references() -> TestResult<Vec<Value>> {
    let records =
        include_str!("fixtures/interval-integral-casts/interval-integral-casts-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 1728);
    Ok(records)
}

/// Differences retained at DAR's exact current result, each owned by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1236: a CAST of a STRING to a single-field day-time qualifier fails in DAR's interval
    /// parser while optimizing, so `CAST(CAST('3' AS INTERVAL DAY) AS INT)` fails where Spark
    /// gives 3.
    StringToQualifier,
    /// #1090: GREATEST and LEAST of NOT NULL intervals are nullable, so the cast of one is too.
    NullableGreatest,
    /// #1236: an ARRAY element or exploded value carries no qualifier, so a DAY interval there
    /// counts seconds: `array(d2 - d1)[0]` overflows INT, and an exploded DAY gives 86400, and
    /// is nullable.
    NestedQualifier,
}

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    if id.starts_with("literal_cast_string_day_") {
        Some(RetainedDifference::StringToQualifier)
    } else if matches!(
        id,
        "expression_greatest_int"
            | "expression_greatest_bigint"
            | "expression_least_hour_int"
            | "expression_least_hour_bigint"
    ) {
        Some(RetainedDifference::NullableGreatest)
    } else if matches!(id, "column_array_element" | "column_explode") {
        Some(RetainedDifference::NestedQualifier)
    } else {
        None
    }
}

/// Replace only the retained part of Spark's expectation with DAR's current result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    match difference {
        RetainedDifference::StringToQualifier => {
            if case["expected"]["status"] != "ok" {
                return Err(format!("expected Spark's count: {case}").into());
            }
            current["schema_stages"] = json!({"analyzed": case["schema_stages"]["analyzed"]});
            current["expected"] = json!({"status": "error",
                "java_message": "error in SQL parser: found end of input"});
        }
        RetainedDifference::NullableGreatest => {
            let field = &mut current["schema_stages"]["executed"]["fields"][1];
            if field["nullable"] != false {
                return Err(format!("expected Spark's NOT NULL result: {case}").into());
            }
            field["nullable"] = json!(true);
        }
        RetainedDifference::NestedQualifier if case["id"] == "column_explode" => {
            let mut rows = case["expected"]["rows"]
                .as_array()
                .ok_or("expected Spark rows")?
                .iter()
                .map(|row| {
                    let days = row[0].as_str().ok_or("expected a count")?.parse::<i64>()?;
                    Ok(json!([(days * 86_400).to_string()]))
                })
                .collect::<TestResult<Vec<_>>>()?;
            rows.sort_by_key(|row| row.to_string());
            current["expected"]["rows"] = json!(rows);
            current["schema_stages"]["executed"]["fields"][0]["nullable"] = json!(true);
        }
        RetainedDifference::NestedQualifier => {
            // 3652058 days in seconds overflow INT.
            current["schema_stages"] = json!({"analyzed": case["schema_stages"]["analyzed"],
                "optimized": case["schema_stages"]["optimized"]});
            current["expected"] = json!({"status": "error", "java_message":
                "[CAST_OVERFLOW] The value INTERVAL '3652058 00:00:00' DAY TO SECOND of the type \
                 \"INTERVAL DAY TO SECOND\" cannot be cast to \"INT\" due to an overflow."});
        }
    }
    Ok(current)
}

/// The Arrow type DAR uses for a recorded Spark result type.
fn spark_family(spark_type: &Value) -> TestResult<Value> {
    let spark_type = spark_type.as_str().ok_or("expected a primitive type")?;
    Ok(json!(match spark_type {
        "byte" => "int8".to_owned(),
        "short" => "int16".to_owned(),
        "integer" => "int32".to_owned(),
        "long" => "int64".to_owned(),
        decimal if decimal.starts_with("decimal(") => decimal.replace(' ', ""),
        other => return Err(format!("unexpected Spark type {other}").into()),
    }))
}

fn dar_family(data_type: &DataType) -> Value {
    json!(match data_type {
        DataType::Int8 => "int8".to_owned(),
        DataType::Int16 => "int16".to_owned(),
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
        other => format!("unexpected {other}"),
    })
}

/// Names, type families and metadata of a recorded schema, with nullability only where it is
/// compared: in the executed schema.
fn spark_fields(schema: &Value, nullability: bool) -> TestResult<Value> {
    schema["fields"]
        .as_array()
        .ok_or("missing recorded fields")?
        .iter()
        .map(|field| {
            let mut field_json = json!({"name": field["name"],
                "type": spark_family(&field["type"])?, "metadata": field["metadata"]});
            if nullability {
                field_json["nullable"] = field["nullable"].clone();
            }
            Ok(field_json)
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

fn dar_fields(fields: &Fields, names: Option<&[String]>, nullability: bool) -> Value {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            let mut field_json = json!({"name": name, "type": dar_family(field.data_type()),
                "metadata": field.metadata()});
            if nullability {
                field_json["nullable"] = json!(field.is_nullable());
            }
            field_json
        })
        .collect()
}

/// A DECIMAL as Java's `BigDecimal` prints it, with every digit of its scale.
fn decimal_text(unscaled: i128, scale: i8) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let sign = if unscaled < 0 { "-" } else { "" };
    let Ok(scale) = usize::try_from(scale) else {
        return format!("{sign}{digits}");
    };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let digits = format!("{digits:0>width$}", width = scale + 1);
    let (integer, fraction) = digits.split_at(digits.len() - scale);
    format!("{sign}{integer}.{fraction}")
}

/// Encode a value as the capture does.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int8 => json!(column.as_primitive::<Int8Type>().value(row).to_string()),
        DataType::Int16 => json!(column.as_primitive::<Int16Type>().value(row).to_string()),
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Decimal128(_, scale) => json!(decimal_text(
            column.as_primitive::<Decimal128Type>().value(row),
            *scale
        )),
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
            false,
        );
        let optimized = optimize(session, &query.logical_plan)?;
        observation["optimized"] = dar_fields(
            optimized.schema().fields(),
            Some(&query.column_names),
            false,
        );
        let physical = query.create_physical_plan(session).await?;
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None, true);
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
        Err(error) => observation["error"] = json!(error.to_string()),
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

/// The texts DAR's error must contain for a recorded Spark failure: Spark's first line without
/// its query context. Over registered rows, which row fails first is not compared, so the
/// failing value is left out: the condition and the text after the value must match. The
/// stage of a failure is not compared.
fn expected_error_texts(case: &Value) -> Option<Vec<String>> {
    let message = case["expected"]["java_message"].as_str()?.lines().next()?;
    let text = message
        .split_once("; line ")
        .map_or(message, |(text, _)| text);
    let over_rows = case["sql"].as_str()?.contains(" FROM ");
    let value_end = [" of the type ", " cannot be represented as "]
        .into_iter()
        .find_map(|marker| text.find(marker));
    match (over_rows, text.split_once("] "), value_end) {
        (true, Some((condition, _)), Some(end)) => {
            Some(vec![format!("{condition}] "), text[end..].to_owned()])
        }
        _ => Some(vec![text.to_owned()]),
    }
}

/// The recorded stages and outcome that differ from the observation.
fn differing_parts(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differing = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if let Some(expected) = case["schema_stages"].get(stage)
            && observation[stage] != spark_fields(expected, stage == "executed")?
        {
            differing.push(stage);
        }
    }
    let outcome_matches = match case["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == case["expected"]["rows"],
        Some("error") => expected_error_texts(case)
            .zip(observation["error"].as_str())
            .is_some_and(|(texts, error)| texts.iter().all(|text| error.contains(text))),
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
async fn interval_integral_casts_match_spark() -> TestResult {
    let records = interval_integral_cast_references()?;
    let mut retained = std::collections::BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained,
        std::collections::BTreeMap::from([
            (RetainedDifference::StringToQualifier, 12),
            (RetainedDifference::NullableGreatest, 16),
            (RetainedDifference::NestedQualifier, 8),
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
