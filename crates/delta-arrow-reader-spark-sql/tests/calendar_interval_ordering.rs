//! Ordering and comparing calendar intervals fails during analysis like Spark, while DAY TO
//! SECOND and YEAR TO MONTH intervals keep their ordering, in both ANSI modes (#1149).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Fields, Int32Type, Int64Type,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
    TimestampMicrosecondType,
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

fn calendar_interval_ordering_references() -> TestResult<Vec<Value>> {
    let records =
        include_str!("fixtures/calendar-interval-ordering/calendar-interval-ordering-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 748);
    Ok(records)
}

/// Differences outside sort keys and comparisons, each owned by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1160: aggregates and array functions that order their values accept calendar
    /// intervals, where Spark rejects them during analysis. DAR's fields and rows are pinned.
    OrderingConsumer,
    /// #1090: GREATEST over a NOT NULL DAY TO SECOND interval and NULL is nullable, where Spark
    /// declares it NOT NULL.
    GreatestNullability,
    /// #1176: DAR coerces STRUCT and ARRAY comparison and IN operands with DataFusion's
    /// comparison coercion, which accepts field pairs Spark rejects and fails some it accepts,
    /// and compares STRUCT fields by position where Spark requires equal names to widen one.
    /// Beside a calendar interval those pairs get the ordering error, and the controls without
    /// one give DAR's results.
    NestedComparisonCoercion,
    /// #1122: DATE beside TIMESTAMP in a STRUCT comparison starts the DATE at UTC midnight, not
    /// at the session zone's, so the America/Los_Angeles control is false where Spark's is
    /// true.
    DateTimestampZone,
}

/// DAR's current result for a record of the #1176 class.
enum NestedCoercionResult {
    /// The ordering error during analysis, where Spark reports a type mismatch.
    OrderingError,
    /// Rows of `k` and a NOT NULL BOOLEAN `r`, as JSON.
    Rows(&'static str),
    /// A failure at `phase` with DAR's `message`, after the recorded `stages`.
    Failure {
        phase: &'static str,
        message: &'static str,
        stages: &'static [&'static str],
    },
}

/// Records of the #1176 class and DAR's result for each.
const NESTED_COERCION_RECORDS: [(&str, NestedCoercionResult); 16] = [
    (
        "field_int_decimal_struct_equal",
        NestedCoercionResult::OrderingError,
    ),
    (
        "field_int_string_struct_equal",
        NestedCoercionResult::OrderingError,
    ),
    (
        "field_int_date_struct_equal",
        NestedCoercionResult::OrderingError,
    ),
    (
        "field_string_date_struct_equal",
        NestedCoercionResult::OrderingError,
    ),
    (
        "field_array_int_string_struct_equal",
        NestedCoercionResult::OrderingError,
    ),
    (
        "field_int_date_struct_in",
        NestedCoercionResult::OrderingError,
    ),
    (
        "field_renamed_widened_struct_equal",
        NestedCoercionResult::OrderingError,
    ),
    (
        "field_int_date_in_subquery",
        NestedCoercionResult::OrderingError,
    ),
    (
        "field_int_decimal_control",
        NestedCoercionResult::Rows(r#"[["0","false"],["1","false"],["2","false"]]"#),
    ),
    (
        "field_int_string_control",
        NestedCoercionResult::Rows(r#"[["0","true"],["1","true"],["2","true"]]"#),
    ),
    (
        "field_int_date_control",
        NestedCoercionResult::Rows(r#"[["0","false"],["1","false"],["2","false"]]"#),
    ),
    (
        "field_string_date_control",
        NestedCoercionResult::Rows(r#"[["0","true"],["1","true"],["2","true"]]"#),
    ),
    (
        "field_array_int_string_control",
        NestedCoercionResult::Failure {
            phase: "collect",
            message: "While casting struct field 'a'\ncaused by\nArrow error: Cast error: Cannot cast \
             string 'a' to value of Int32 type",
            stages: &["analyzed", "optimized"],
        },
    ),
    (
        "field_struct_names_control",
        NestedCoercionResult::Failure {
            phase: "optimized",
            message: "This feature is not implemented: Unsupported CAST from Struct(\"a\": non-null \
             Struct(\"y\": non-null Int32), \"b\": non-null Int32) to Struct(\"a\": non-null \
             Struct(\"x\": non-null Int32), \"b\": non-null Int32)",
            stages: &["analyzed"],
        },
    ),
    (
        "field_int_boolean_control",
        NestedCoercionResult::Failure {
            phase: "sql",
            message: "Cannot infer common argument type for comparison operation Struct(\"a\": non-null \
             Int32, \"b\": non-null Int32) = Struct(\"a\": non-null Boolean, \"b\": non-null \
             Int32)",
            stages: &[],
        },
    ),
    (
        "field_int_array_control",
        NestedCoercionResult::Failure {
            phase: "sql",
            message: "Cannot infer common argument type for comparison operation Struct(\"a\": non-null \
             Int32, \"b\": non-null Int32) = Struct(\"a\": non-null List(non-null Int32), \
             \"b\": non-null Int32)",
            stages: &[],
        },
    ),
];

/// The fields of `k` and a NOT NULL BOOLEAN `r` at every stage, as Spark records them.
fn key_and_boolean_stages() -> Value {
    let schema = json!({"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "r", "type": "boolean", "nullable": false, "metadata": {}}]});
    json!({"analyzed": schema, "optimized": schema, "executed": schema})
}

/// DAR's current rows for each record of the #1160 class.
fn ordering_consumer_rows(id: &str) -> Option<Value> {
    let days = |days: &str| json!(["0", days, "0"]);
    Some(match id {
        "max" => json!([[days("3")]]),
        "min" => json!([[days("-2")]]),
        "max_by" => json!([["2"]]),
        "sort_array" | "array_sort" => {
            json!([["0", [days("1")]], ["1", [days("-2")]], ["2", [days("3")]]])
        }
        "array_max" => json!([["0", days("1")], ["1", days("-2")], ["2", days("3")]]),
        _ => return None,
    })
}

/// DAR's current fields, at every stage, for each record of the #1160 class. MAKE_INTERVAL is
/// nullable outside ANSI, so an ARRAY of it holds nullable elements there.
fn ordering_consumer_schema(id: &str, ansi: bool) -> Option<Value> {
    let field = |name: &str, data_type: Value, nullable: bool| json!({"name": name, "type": data_type, "nullable": nullable, "metadata": {}});
    let k = field("k", json!("integer"), false);
    let intervals = json!({"type": "array", "elementType": "interval", "containsNull": !ansi});
    let fields = match id {
        "max" | "min" => vec![field("r", json!("interval"), true)],
        "max_by" => vec![field("r", json!("integer"), true)],
        "sort_array" => vec![k, field("r", intervals, false)],
        "array_sort" => vec![k, field("r", intervals, true)],
        "array_max" => vec![k, field("r", json!("interval"), true)],
        _ => return None,
    };
    let schema = json!({"type": "struct", "fields": fields});
    Some(json!({"analyzed": schema, "optimized": schema, "executed": schema}))
}

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    if ordering_consumer_rows(id).is_some() {
        Some(RetainedDifference::OrderingConsumer)
    } else if id == "greatest_day_time_null_control" {
        Some(RetainedDifference::GreatestNullability)
    } else if NESTED_COERCION_RECORDS
        .iter()
        .any(|(record, _)| *record == id)
    {
        Some(RetainedDifference::NestedComparisonCoercion)
    } else if id == "field_date_timestamp_control" && case["timezone"] == "America/Los_Angeles" {
        Some(RetainedDifference::DateTimestampZone)
    } else {
        None
    }
}

/// Replace only the retained part of Spark's expectation with DAR's current result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    match difference {
        RetainedDifference::NestedComparisonCoercion => {
            let id = case["id"].as_str().ok_or("missing record id")?;
            let result = NESTED_COERCION_RECORDS
                .iter()
                .find_map(|(record, result)| (*record == id).then_some(result))
                .ok_or("missing DAR result")?;
            let spark_ok = case["expected"]["status"] == "ok";
            let spark_mismatch = case["spark_error_phase"] == "sql"
                && matches!(
                    case["expected"]["condition"].as_str(),
                    Some(
                        "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES"
                            | "DATATYPE_MISMATCH.DATA_DIFF_TYPES"
                            | "DATATYPE_MISMATCH.IN_SUBQUERY_DATA_TYPE_MISMATCH"
                    )
                );
            if !(spark_ok || spark_mismatch) {
                return Err(format!("expected Spark's rows or type mismatch: {case}").into());
            }
            match result {
                NestedCoercionResult::OrderingError => {
                    current["expected"] = json!({"status": "error",
                        "condition": "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE"});
                    current["spark_error_phase"] = json!("sql");
                }
                NestedCoercionResult::Rows(rows) => {
                    current["schema_stages"] = key_and_boolean_stages();
                    current["expected"] =
                        json!({"status": "ok", "rows": serde_json::from_str::<Value>(rows)?});
                    current["spark_error_phase"] = Value::Null;
                }
                NestedCoercionResult::Failure {
                    phase,
                    message,
                    stages,
                } => {
                    let all = key_and_boolean_stages();
                    current["schema_stages"] = stages
                        .iter()
                        .map(|stage| ((*stage).to_owned(), all[*stage].clone()))
                        .collect::<serde_json::Map<_, _>>()
                        .into();
                    current["expected"] = json!({"status": "error", "dar_message": message});
                    current["spark_error_phase"] = json!(phase);
                }
            }
        }
        RetainedDifference::DateTimestampZone => {
            if case["expected"]["rows"] != json!([["0", "true"], ["1", "true"], ["2", "true"]]) {
                return Err(format!("expected Spark's equal rows: {case}").into());
            }
            current["expected"]["rows"] = json!([["0", "false"], ["1", "false"], ["2", "false"]]);
        }
        RetainedDifference::OrderingConsumer => {
            if case["expected"]["condition"] != "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE" {
                return Err(format!("expected Spark's ordering rejection: {case}").into());
            }
            let id = case["id"].as_str().ok_or("missing record id")?;
            let rows = ordering_consumer_rows(id).ok_or("missing DAR rows")?;
            current["schema_stages"] =
                ordering_consumer_schema(id, case["ansi"] == true).ok_or("missing DAR fields")?;
            current["expected"] = json!({"status": "ok", "rows": rows});
            current["spark_error_phase"] = Value::Null;
        }
        RetainedDifference::GreatestNullability => {
            for stage in ["analyzed", "optimized", "executed"] {
                let field = &mut current["schema_stages"][stage]["fields"][1];
                if field["type"] != "interval day to second" || field["nullable"] != false {
                    return Err(format!("expected Spark's NOT NULL interval: {case}").into());
                }
                field["nullable"] = json!(true);
            }
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
        "boolean" => "boolean",
        "interval day to second" => "interval day to second",
        "interval year to month" => "interval year to month",
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
        DataType::Boolean => "boolean".to_owned(),
        DataType::Duration(TimeUnit::Microsecond) => "interval day to second".to_owned(),
        DataType::Interval(IntervalUnit::YearMonth) => "interval year to month".to_owned(),
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
        DataType::Boolean => json!(column.as_boolean().value(row).to_string()),
        DataType::Interval(IntervalUnit::YearMonth) => json!(
            column
                .as_primitive::<IntervalYearMonthType>()
                .value(row)
                .to_string()
        ),
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
        // Keep the row order of an ORDER BY whose order Spark's reference records.
        if case["ordered"] != true {
            rows.sort_by_key(|row| json!(row).to_string());
        }
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
/// pinned text, or the condition. Every recorded Spark failure is an analysis failure, whose
/// message quotes Spark's plan.
fn expected_error_text(case: &Value) -> Option<String> {
    if let Some(message) = case["expected"]["dar_message"].as_str() {
        return Some(message.to_owned());
    }
    if case["spark_error_phase"] != "sql" {
        return None;
    }
    let condition = case["expected"]["condition"].as_str()?;
    Some(format!("[{condition}]"))
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
async fn calendar_interval_ordering_matches_spark() -> TestResult {
    let records = calendar_interval_ordering_references()?;
    let mut retained = BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained,
        BTreeMap::from([
            (RetainedDifference::OrderingConsumer, 24),
            (RetainedDifference::GreatestNullability, 4),
            (RetainedDifference::NestedComparisonCoercion, 64),
            (RetainedDifference::DateTimestampZone, 2),
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
