//! Capture full query rows and schemas for comparison with independent Spark records.

use std::error::Error;

use arrow::array::{Array, AsArray, ListArray, StructArray};
use arrow::datatypes::{
    DataType, Fields, Float32Type, Float64Type, IntervalUnit, IntervalYearMonthType, Schema,
    TimeUnit, TimestampMicrosecondType,
};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "c06_receipts.rs"]
pub(crate) mod c06_receipts;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[allow(
    dead_code,
    reason = "Integration tests select individual comparison contracts."
)]
pub(crate) fn remove_nullability_and_metadata(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for key in ["nullable", "metadata", "containsNull", "valueContainsNull"] {
                object.remove(key);
            }
            object
                .values_mut()
                .for_each(remove_nullability_and_metadata);
        }
        Value::Array(values) => values.iter_mut().for_each(remove_nullability_and_metadata),
        _ => {}
    }
}

/// Collect values and the executed schema without comparing logical annotations.
#[allow(
    dead_code,
    reason = "Integration tests select individual comparison contracts."
)]
pub(crate) async fn capture_executed_query_result(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let physical_schema = physical.schema();
    let schema = capture_schema(physical_schema.fields(), None)?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        validate_batch(physical_schema.as_ref(), &batch)?;
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
    Ok(json!({"schema":schema,"rows":rows}))
}

#[allow(
    dead_code,
    reason = "Integration tests select individual shared reference checks."
)]
pub(crate) const STAGES: [(&str, &str); 5] = [
    ("public", "public_schema"),
    ("analyzed", "public_schema"),
    ("optimized", "optimized_schema"),
    ("executed_before_collect", "before_collect"),
    ("executed", "physical_schema"),
];

#[allow(
    dead_code,
    reason = "Integration tests select individual shared reference checks."
)]
pub(crate) fn case_identity(case: &Value) -> TestResult<(String, String, bool)> {
    Ok((
        case["corpus"].as_str().ok_or("missing corpus")?.into(),
        case["id"].as_str().ok_or("missing ID")?.into(),
        case["ansi"].as_bool().ok_or("missing ANSI")?,
    ))
}

fn multiset(rows: &Value) -> TestResult<Value> {
    let mut rows = rows.as_array().ok_or("missing rows")?.clone();
    rows.sort_by_key(Value::to_string);
    Ok(json!(rows))
}

fn complete_schema(schema: &Value) -> bool {
    schema["type"] == "struct"
        && schema["fields"]
            .as_array()
            .is_some_and(|fields| !fields.is_empty())
        && complete_data_type(schema)
}

fn complete_data_type(data_type: &Value) -> bool {
    if data_type.is_string() {
        return true;
    }
    match data_type["type"].as_str() {
        Some("array") => {
            complete_data_type(&data_type["elementType"]) && data_type["containsNull"].is_boolean()
        }
        Some("map") => {
            complete_data_type(&data_type["keyType"])
                && complete_data_type(&data_type["valueType"])
                && data_type["valueContainsNull"].is_boolean()
        }
        Some("struct") => data_type["fields"].as_array().is_some_and(|fields| {
            fields.iter().all(|field| {
                field["name"].is_string()
                    && complete_data_type(&field["type"])
                    && field["nullable"].is_boolean()
                    && field["metadata"].is_object()
            })
        }),
        _ => false,
    }
}

fn complete_recorded_value(value: &Value, data_type: &Value, nullable: bool) -> bool {
    if value.is_null() {
        return nullable;
    }
    if data_type.is_string() {
        return value.is_string();
    }
    let Some(values) = value.as_array() else {
        return false;
    };
    match data_type["type"].as_str() {
        Some("array") => data_type["containsNull"].as_bool().is_some_and(|nullable| {
            values
                .iter()
                .all(|value| complete_recorded_value(value, &data_type["elementType"], nullable))
        }),
        Some("struct") => data_type["fields"].as_array().is_some_and(|fields| {
            values.len() == fields.len()
                && values.iter().zip(fields).all(|(value, field)| {
                    field["nullable"].as_bool().is_some_and(|nullable| {
                        complete_recorded_value(value, &field["type"], nullable)
                    })
                })
        }),
        Some("map") => data_type["valueContainsNull"]
            .as_bool()
            .is_some_and(|nullable| {
                values.iter().all(|entry| {
                    entry.as_array().is_some_and(|entry| {
                        entry.len() == 2
                            && complete_recorded_value(&entry[0], &data_type["keyType"], false)
                            && complete_recorded_value(&entry[1], &data_type["valueType"], nullable)
                    })
                })
            }),
        _ => false,
    }
}

#[test]
fn complete_spark_schema_requires_nested_type_and_field_declarations() -> TestResult {
    let schema = json!({"type":"struct","fields":[{"name":"nested","nullable":true,"metadata":{},
        "type":{"type":"array","containsNull":false,"elementType":{"type":"map","keyType":"integer",
        "valueContainsNull":true,"valueType":{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}}}}]});
    assert!(complete_schema(&schema));
    for pointer in [
        "/fields/0/type/containsNull",
        "/fields/0/type/elementType/valueContainsNull",
        "/fields/0/type/elementType/keyType",
        "/fields/0/type/elementType/valueType/fields/0/metadata",
    ] {
        let mut changed = schema.clone();
        *changed
            .pointer_mut(pointer)
            .ok_or("nested schema fault target")? = Value::Null;
        assert!(!complete_schema(&changed), "accepted lost {pointer}");
    }
    let mut changed = schema;
    changed["fields"][0]["type"] = json!({"type":"unknown"});
    assert!(!complete_schema(&changed));
    Ok(())
}

#[test]
fn recorded_nested_values_keep_shape_and_nullability() {
    let data_type = json!({"type":"array","containsNull":false,"elementType":{"type":"map",
        "keyType":"integer","valueContainsNull":false,"valueType":{"type":"struct","fields":[
        {"name":"value","type":"integer","nullable":false,"metadata":{}}]}}});
    assert!(complete_recorded_value(
        &json!([[["0", ["70"]]]]),
        &data_type,
        false
    ));
    for value in [
        json!([null]),
        json!([{"0":["70"]}]),
        json!([[["0", []]]]),
        json!([[[null, ["70"]]]]),
        json!([[["0", null]]]),
        json!([[["0", [null]]]]),
        json!([[["0", [70]]]]),
        json!([[["0", ["70"], "extra"]]]),
    ] {
        assert!(
            !complete_recorded_value(&value, &data_type, false),
            "accepted malformed nested value {value}"
        );
    }
    let mut nullable = data_type.clone();
    nullable["containsNull"] = json!(true);
    assert!(complete_recorded_value(&json!([null]), &nullable, false));
    let mut nullable = data_type.clone();
    nullable["elementType"]["valueContainsNull"] = json!(true);
    assert!(complete_recorded_value(
        &json!([[["0", null]]]),
        &nullable,
        false
    ));
    let mut nullable = data_type;
    nullable["elementType"]["valueType"]["fields"][0]["nullable"] = json!(true);
    assert!(complete_recorded_value(
        &json!([[["0", [null]]]]),
        &nullable,
        false
    ));
}

#[allow(
    dead_code,
    reason = "Integration tests select individual shared reference checks."
)]
pub(crate) fn validate_spark_reference_result(spark_reference: &Value) -> TestResult {
    let case = spark_reference;
    let key = case_identity(case)?;
    let schemas = case["schema_stages"]
        .as_object()
        .ok_or("missing schema stages")?;
    let availability = case["schema_stage_availability"]
        .as_object()
        .ok_or("missing schema availability")?;
    if availability.len() != STAGES.len()
        || schemas
            .keys()
            .any(|stage| !STAGES.iter().any(|(name, _)| *name == stage.as_str()))
    {
        return Err("Spark reference schema stage inventory differs".into());
    }
    for (stage, _) in STAGES {
        let state = &case["schema_stage_availability"][stage];
        match state["available"].as_bool() {
            Some(true) if complete_schema(&case["schema_stages"][stage]) => {}
            Some(false)
                if case["status"] == "error"
                    && !schemas.contains_key(stage)
                    && state["reason"]
                        .as_str()
                        .is_some_and(|reason| !reason.is_empty()) => {}
            _ => return Err(format!("incomplete Spark reference stage {stage}: {key:?}").into()),
        }
    }
    if case["status"] == "ok" {
        let fields = case["schema_stages"]["public"]["fields"]
            .as_array()
            .ok_or("missing public Fields")?;
        if schemas.len() != STAGES.len()
            || case["expected"]["schema"] != case["schema_stages"]["public"]
            || case["expected"].get("condition").is_some()
            || ["rows_raw", "rows_multiset"].iter().any(|field| {
                !case[*field].as_array().is_some_and(|rows| {
                    rows.iter().all(|row| {
                        row.as_array().is_some_and(|row| {
                            row.len() == fields.len()
                                && row.iter().zip(fields).all(|(value, field)| {
                                    field["nullable"].as_bool().is_some_and(|nullable| {
                                        complete_recorded_value(value, &field["type"], nullable)
                                    })
                                })
                        })
                    })
                })
            })
            || multiset(&case["rows_raw"])? != case["rows_multiset"]
            || case["expected"].get("rows")
                != Some(if case["ordered"] == true {
                    &case["rows_raw"]
                } else {
                    &case["rows_multiset"]
                })
        {
            return Err(format!("incomplete Spark reference success or rows: {key:?}").into());
        }
    } else {
        let details = &case["spark_error_details"];
        if !case["expected"]["condition"].is_string()
            || case["expected"].get("rows").is_some()
            || !case["spark_error_phase"].is_string()
            || details["condition"] != case["expected"]["condition"]
            || details["phase"] != case["spark_error_phase"]
            || !details["message"].is_string()
            || !details["java_causes"].as_array().is_some_and(|causes| {
                !causes.is_empty()
                    && causes.iter().all(|cause| {
                        cause["class"].is_string()
                            && cause["message"].is_string()
                            && cause["parameters"].is_object()
                            && cause.get("condition").is_some()
                            && cause.get("sql_state").is_some()
                    })
            })
            || details["cause_chain_complete"] != true
            || ["parameters", "sql_state", "python_class"]
                .iter()
                .any(|field| details.get(*field).is_none())
        {
            return Err(format!("incomplete Spark reference failure: {key:?}").into());
        }
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Integration tests select individual shared reference checks."
)]
pub(crate) fn native_result_matches_spark_reference(
    native_result: &Value,
    spark_reference: &Value,
) -> bool {
    native_result["matches"] == true
        && native_result["optimized_matches"] == true
        && STAGES.iter().all(|(stage, observed)| {
            match spark_reference["schema_stages"].get(*stage) {
                Some(expected) => native_result.get(*observed) == Some(expected),
                None => native_result.get(*observed).is_some_and(Value::is_null),
            }
        })
}

/// Compare the observed Int32 cast cause with Spark's recorded failing input.
/// Only CAST_INVALID_INPUT records require this additional input check.
#[allow(
    dead_code,
    reason = "Integration tests select individual shared result checks."
)]
pub(crate) fn matches_spark_int32_cast_input(case: &Value, observation: &Value) -> bool {
    if case["expected"]["condition"] != "CAST_INVALID_INPUT" {
        return true;
    }
    let Some(input) = case["spark_error_details"]["message_parameters"]["expression"].as_str()
    else {
        return false;
    };
    let expected = format!("Cannot cast string {input} to value of Int32 type");
    observation["error"].as_str().is_some_and(|error| {
        error
            .lines()
            .filter_map(|line| line.strip_prefix("Arrow error: Cast error: "))
            .any(|cause| cause == expected)
    })
}

/// Observe native schemas, rows and the exact failure phase, including expected errors.
#[allow(
    dead_code,
    reason = "Integration tests select individual shared observers."
)]
pub(crate) async fn capture_native_query_observation(
    session: &SessionContext,
    sql: &str,
) -> TestResult<Value> {
    let mut phase = "sql";
    let mut public = None;
    let mut before = None;
    let mut executed = None;
    let result: TestResult<Vec<Vec<Value>>> = async {
        let frame = session.sql(sql).await?;
        phase = "public_schema";
        public = Some(capture_schema(frame.schema().fields(), None)?);
        phase = "create_physical_plan";
        let physical = frame.create_physical_plan().await?;
        phase = "physical_schema";
        before = Some(capture_schema(physical.schema().fields(), None)?);
        let batches = collect(physical.clone(), session.task_ctx()).await;
        phase = "executed_schema";
        let schema = physical.schema();
        executed = Some(capture_schema(schema.fields(), None)?);
        phase = "collect";
        let batches = batches?;
        phase = "encode";
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
    Ok(
        json!({"public_schema":public,"before_collect":before,"physical_schema":executed,
        "rows":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string),
        "phase":phase}),
    )
}

#[allow(
    dead_code,
    reason = "Integration tests select individual shared result observers."
)]
pub(crate) async fn capture_spark_query_result(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    Ok(capture_spark_query_observation(session, case).await?["result"].take())
}

#[allow(
    dead_code,
    reason = "Integration tests select individual shared result observers."
)]
pub(crate) async fn capture_spark_query_observation(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    let resolved = query.logical_plan.schema().fields();
    let public_schema = capture_schema(resolved, Some(&query.column_names))?;
    let physical_schema = capture_schema(schema.fields(), None)?;
    if let Some(expected) = case
        .get("executed_schema_after_collect")
        .or_else(|| case.get("executed_schema_before_collect"))
    {
        if physical_schema != *expected {
            return Err(
                format!("executed schema: expected {expected}, got {physical_schema}").into(),
            );
        }
    } else {
        validate_physical_schema(resolved, &query.column_names, schema.fields())?;
    }
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
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
    Ok(json!({"result":{"schema":public_schema,"rows":rows},
        "physical_schema":physical_schema}))
}

/// Observe the production optimizer with verified public output names.
#[allow(
    dead_code,
    reason = "Integration tests select individual shared observers."
)]
pub(crate) async fn capture_spark_optimized_observation(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    capture_spark_optimized_observation_impl(session, case, None).await
}

async fn capture_spark_optimized_observation_impl(
    session: &SessionContext,
    case: &Value,
    mut receipt: Option<&mut Value>,
) -> TestResult<Value> {
    if let Some(receipt) = receipt.as_deref_mut() {
        receipt["phase"] = json!("resolve");
    }
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    if let Some(receipt) = receipt.as_deref_mut() {
        receipt["public_names"] = json!(query.column_names);
        receipt["analyzed"] = c06_receipts::schema(query.logical_plan.schema().as_arrow());
        receipt["analyzed_plan"] = json!(format!("{:?}", query.logical_plan));
        receipt["phase"] = json!("optimize");
    }
    let mut state = session.state();
    let extensions = &mut state.config_mut().options_mut().extensions;
    extensions.insert(SparkSqlExpressionSemantics::default());
    extensions.insert(PreserveAggregateStatisticsSchema::default());
    let optimized = state.optimize(&query.logical_plan)?;
    if let Some(receipt) = receipt {
        receipt["optimized"] = c06_receipts::schema(optimized.schema().as_arrow());
        receipt["optimized_plan"] = json!(format!("{optimized:?}"));
    }
    let raw_schema = capture_schema(optimized.schema().fields(), None)?;
    let names_match = query.column_names.len() == query.logical_plan.schema().fields().len()
        && optimized.schema().fields().len() == query.logical_plan.schema().fields().len()
        && optimized
            .schema()
            .fields()
            .iter()
            .zip(query.logical_plan.schema().fields())
            .all(|(actual, resolved)| actual.name() == resolved.name());
    let schema = names_match
        .then(|| capture_schema(optimized.schema().fields(), Some(&query.column_names)))
        .transpose()?;
    Ok(json!({"raw_schema":raw_schema,"schema":schema,"names_match":names_match}))
}

/// Compare every recorded schema stage, ordered values, and the required error condition and phase.
#[allow(
    dead_code,
    reason = "Integration tests select individual shared observers."
)]
pub(crate) async fn compare_exact_spark_query_stages_with_optimized(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    let mut comparison = compare_exact_spark_query_stages(session, case).await?;
    let optimized = capture_spark_optimized_observation(session, case).await;
    add_optimized_comparison(&mut comparison, case, &optimized);
    Ok(comparison)
}

#[allow(dead_code, reason = "Only C06 callers request additive receipts.")]
pub(crate) async fn compare_exact_spark_query_stages_with_optimized_and_receipt(
    session: &SessionContext,
    case: &Value,
) -> TestResult<(Value, Value)> {
    let mut primary = c06_receipts::new("public_physical_collect");
    let mut comparison =
        compare_exact_spark_query_stages_impl(session, case, Some(&mut primary)).await?;
    c06_receipts::finish(&mut primary);
    let mut separate = c06_receipts::new("separately_replanned_optimized");
    let optimized =
        capture_spark_optimized_observation_impl(session, case, Some(&mut separate)).await;
    if let Err(error) = &optimized {
        let phase = separate["phase"].as_str().unwrap_or("resolve").to_owned();
        separate["error"] = c06_receipts::failure(error.as_ref(), &phase);
    }
    c06_receipts::finish(&mut separate);
    add_optimized_comparison(&mut comparison, case, &optimized);
    Ok((
        comparison,
        json!({"primary":primary,"separately_replanned_optimized":separate}),
    ))
}

fn add_optimized_comparison(comparison: &mut Value, case: &Value, optimized: &TestResult<Value>) {
    comparison["optimized_matches"] = json!(match &optimized {
        Ok(observation) =>
            observation["names_match"] == true
                && Some(&observation["schema"]) == case["schema_stages"].get("optimized"),
        Err(error) =>
            case["schema_stages"].get("optimized").is_none()
                && comparison["error_matches"] == true
                && matches!(
                    comparison["native_phase"].as_str(),
                    Some("resolve" | "create_physical_plan")
                )
                && comparison["error"].as_str() == Some(error.to_string().as_str()),
    });
    comparison["optimized_schema"] = optimized
        .as_ref()
        .map(|observation| observation["schema"].clone())
        .unwrap_or(Value::Null);
    comparison["optimized_observation"] = optimized.as_ref().ok().cloned().unwrap_or(Value::Null);
    comparison["optimized_error"] = json!(optimized.as_ref().err().map(ToString::to_string));
}

/// Require the unchanged reference and complete inherited DAR observation.
#[allow(
    dead_code,
    reason = "Integration tests select individual shared observers."
)]
pub(crate) fn matches_inherited_query_observation(
    reference: &Value,
    comparison: &Value,
    control: &Value,
) -> bool {
    if control["reference"] != *reference {
        return false;
    }
    let mut expected = control["observation"].clone();
    expected["batch"] = comparison["batch"].clone();
    comparison == &expected
}

/// Compare exact recorded stages, complete rows, and the required engine error phase.
#[allow(
    dead_code,
    reason = "Integration tests select individual shared observers."
)]
pub(crate) async fn compare_exact_spark_query_stages(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    compare_exact_spark_query_stages_impl(session, case, None).await
}

async fn compare_exact_spark_query_stages_impl(
    session: &SessionContext,
    case: &Value,
    mut receipt: Option<&mut Value>,
) -> TestResult<Value> {
    let mut phase = "resolve";
    let mut public = None;
    let mut before = None;
    let mut executed = None;
    let mut rows_raw = None;
    let actual: TestResult<Vec<Vec<Value>>> = async {
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                session_timezone: case["timezone"].as_str().unwrap_or("UTC").into(),
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        if let Some(receipt) = receipt.as_deref_mut() {
            receipt["public_names"] = json!(query.column_names);
            receipt["analyzed"] = c06_receipts::schema(query.logical_plan.schema().as_arrow());
            receipt["analyzed_plan"] = json!(format!("{:?}", query.logical_plan));
        }
        phase = "public_schema";
        if query.column_names.len() != query.logical_plan.schema().fields().len() {
            return Err("public field count differs from output names".into());
        }
        public = Some(capture_schema(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        )?);
        phase = "create_physical_plan";
        let physical = query.create_physical_plan(session).await?;
        phase = "physical_schema";
        before = Some(capture_schema(physical.schema().fields(), None)?);
        if let Some(receipt) = receipt.as_deref_mut() {
            receipt["before_collect"] = c06_receipts::schema(physical.schema().as_ref());
            receipt["physical_plan"] = json!(format!("{physical:?}"));
        }
        let batches = collect(physical.clone(), session.task_ctx()).await;
        phase = "executed_schema";
        let schema = physical.schema();
        executed = Some(capture_schema(schema.fields(), None)?);
        if let Some(receipt) = receipt.as_deref_mut() {
            receipt["physical"] = c06_receipts::schema(schema.as_ref());
        }
        phase = "collect";
        let batches = batches?;
        phase = "encode";
        let mut rows = Vec::new();
        for batch in &batches {
            validate_batch(schema.as_ref(), batch)?;
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
        if let Some(receipt) = receipt.as_deref_mut() {
            receipt["output"] = c06_receipts::batches(&batches, encode_value)?;
        }
        rows_raw = Some(rows.clone());
        rows.sort_by_key(|row| json!(row).to_string());
        Ok(rows)
    }
    .await;
    if let (Some(receipt), Err(error)) = (receipt, &actual) {
        receipt["error"] = c06_receipts::failure(error.as_ref(), phase);
    }
    let recorded_stage = |stage: &str, alias: &str| -> TestResult<Option<&Value>> {
        let nested = case["schema_stages"].get(stage);
        let flat = case.get(alias);
        if let (Some(nested), Some(flat)) = (nested, flat)
            && nested != flat
        {
            return Err(
                format!("conflicting recorded schema stage {stage}: {nested} != {flat}").into(),
            );
        }
        Ok(nested.or(flat))
    };
    let public_matches = recorded_stage("analyzed", "analyzed_schema")?
        .is_none_or(|expected| public.as_ref() == Some(expected));
    let before_matches =
        recorded_stage("executed_before_collect", "executed_schema_before_collect")?
            .is_none_or(|expected| before.as_ref() == Some(expected));
    let executed_matches = recorded_stage("executed", "executed_schema_after_collect")?
        .is_none_or(|expected| executed.as_ref() == Some(expected));
    let rows_match = if let Some(expected) = case["expected"].get("rows") {
        let mut expected = expected.as_array().ok_or("missing recorded rows")?.clone();
        if case["ordered"] == true {
            expected.sort_by_key(Value::to_string);
        }
        actual
            .as_ref()
            .is_ok_and(|rows| json!(rows) == json!(expected))
    } else {
        true
    };
    let expected_error =
        case["expected"]["condition"].as_str().is_some() && case["expected"].get("rows").is_none();
    let ordered_rows_match = expected_error
        || case["ordered"] != true
        || rows_raw
            .as_ref()
            .is_some_and(|rows| json!(rows) == case["expected"]["rows"]);
    let error_matches = if let Some(condition) = case["expected"]["condition"].as_str() {
        let expected_phase = match case["spark_error_phase"].as_str() {
            Some("sql") => "resolve",
            // DAR creates the optimized logical plan inside create_physical_plan.
            Some("optimized" | "optimized_schema") => "create_physical_plan",
            Some("collect") => "collect",
            other => return Err(format!("unsupported reference error phase: {other:?}").into()),
        };
        phase == expected_phase
            && actual
                .as_ref()
                .err()
                .is_some_and(|error| matches_spark_error_condition(&error.to_string(), condition))
    } else {
        actual.is_ok()
    };
    let matches = public_matches
        && before_matches
        && executed_matches
        && rows_match
        && ordered_rows_match
        && error_matches;
    Ok(json!({
        "public_schema":public, "before_collect":before, "physical_schema":executed,
        "rows":actual.as_ref().ok(), "rows_raw":rows_raw, "native_phase":phase,
        "error":actual.as_ref().err().map(ToString::to_string),
        "public_matches":public_matches, "before_matches":before_matches,
        "physical_matches":executed_matches, "rows_match":rows_match,
        "ordered_rows_match":ordered_rows_match,
        "error_matches":error_matches, "matches":matches,
    }))
}

fn matches_spark_error_condition(error: &str, condition: &str) -> bool {
    error.contains(&format!("[{condition}]"))
        || matches!(
            (condition, error),
            (
                "SCALAR_SUBQUERY_TOO_MANY_ROWS",
                "Execution error: Scalar subquery returned more than one row"
            )
        )
}

#[test]
fn scalar_subquery_condition_mapping_requires_the_exact_execution_error() {
    let error = "Execution error: Scalar subquery returned more than one row";
    assert!(matches_spark_error_condition(
        error,
        "SCALAR_SUBQUERY_TOO_MANY_ROWS"
    ));
    assert!(!matches_spark_error_condition(error, "CAST_INVALID_INPUT"));
    for error in [
        "Planning error: Scalar subquery returned more than one row",
        "Execution error: Scalar subquery returned more than one row: unrelated failure",
        "Execution error: another scalar subquery failure",
    ] {
        assert!(!matches_spark_error_condition(
            error,
            "SCALAR_SUBQUERY_TOO_MANY_ROWS"
        ));
    }
    assert!(matches_spark_error_condition(
        "[CAST_INVALID_INPUT] invalid integer",
        "CAST_INVALID_INPUT"
    ));
}

#[allow(
    dead_code,
    reason = "Integration tests select individual shared result observers."
)]
pub(crate) fn validate_physical_schema(
    resolved: &Fields,
    names: &[String],
    physical: &Fields,
) -> TestResult {
    if resolved.len() != names.len() || physical.len() != resolved.len() {
        return Err("resolved and physical field counts differ from output names".into());
    }
    for ((logical, name), field) in resolved.iter().zip(names).zip(physical) {
        // Only top-level nullability can narrow; nested declarations stay exact.
        if field.name() != name
            || logical.data_type() != field.data_type()
            || logical.metadata() != field.metadata()
            || (field.is_nullable() && !logical.is_nullable())
        {
            return Err(format!(
                "resolved field {logical:?} (public name {name:?}) differs from physical field {field:?}"
            )
            .into());
        }
    }
    Ok(())
}

pub(crate) fn validate_batch(schema: &Schema, batch: &RecordBatch) -> TestResult {
    if batch.schema().as_ref() != schema {
        return Err("returned batch schema differs from physical schema".into());
    }
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if !field.is_nullable() && column.logical_null_count() != 0 {
            return Err(format!(
                "nonnullable physical field {:?} returned NULL",
                field.name()
            )
            .into());
        }
        column.to_data().validate_full()?;
    }
    Ok(())
}

pub(crate) fn capture_schema(fields: &Fields, names: Option<&[String]>) -> TestResult<Value> {
    Ok(json!({"type":"struct","fields":fields.iter().enumerate()
        .map(|(index, field)| Ok(json!({
            "name":names.map_or(field.name(), |names| &names[index]),
            "type":spark_schema_type(field.data_type())?,
            "nullable":field.is_nullable(),"metadata":field.metadata()})))
        .collect::<TestResult<Vec<_>>>()?}))
}

fn spark_schema_type(data_type: &DataType) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Null => json!("void"),
        DataType::Int8 => json!("byte"),
        DataType::Int16 => json!("short"),
        DataType::Int32 => json!("integer"),
        DataType::Int64 => json!("long"),
        DataType::Boolean => json!("boolean"),
        DataType::Interval(IntervalUnit::YearMonth) => json!("interval month"),
        DataType::Float32 => json!("float"),
        DataType::Float64 => json!("double"),
        DataType::Decimal128(precision, scale) => json!(format!("decimal({precision},{scale})")),
        DataType::Date32 => json!("date"),
        DataType::Timestamp(TimeUnit::Microsecond, None) => json!("timestamp_ntz"),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => json!("timestamp"),
        DataType::Utf8 => json!("string"),
        DataType::List(field) => json!({"type":"array",
            "elementType":spark_schema_type(field.data_type())?,"containsNull":field.is_nullable()}),
        DataType::Struct(fields) => json!({"type":"struct", "fields":fields.iter()
            .map(|field| Ok(json!({"name":field.name(), "type":spark_schema_type(field.data_type())?,
                "nullable":field.is_nullable(), "metadata":field.metadata()})))
            .collect::<TestResult<Vec<_>>>()?}),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err("expected MAP entry fields".into());
            };
            let [key, value] = fields.as_ref() else {
                return Err("expected MAP key/value fields".into());
            };
            if entries.is_nullable() || key.is_nullable() {
                return Err("MAP entries and keys must be nonnullable".into());
            }
            json!({"type":"map","keyType":spark_schema_type(key.data_type())?,
                "valueType":spark_schema_type(value.data_type())?,
                "valueContainsNull":value.is_nullable()})
        }
        other => return Err(format!("unexpected Spark output type: {other}").into()),
    })
}

pub(crate) fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Null => Value::Null,
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let value = column.as_primitive::<TimestampMicrosecondType>().value(row);
            let datetime = arrow::temporal_conversions::timestamp_us_to_datetime(value)
                .ok_or("recorded TIMESTAMP_NTZ is outside the datetime encoding range")?;
            let text = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
            let micros = datetime.and_utc().timestamp_subsec_micros();
            json!(if micros == 0 {
                text
            } else {
                format!("{text}.{micros:06}")
            })
        }
        DataType::List(field) => {
            let values = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or("expected list")?
                .value(row);
            if !field.is_nullable() && values.logical_null_count() != 0 {
                return Err("nonnullable list element returned NULL".into());
            }
            json!(
                (0..values.len())
                    .map(|index| encode_value(values.as_ref(), index))
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
        DataType::Struct(fields) => {
            let values = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or("expected struct")?;
            json!(
                values
                    .columns()
                    .iter()
                    .zip(fields)
                    .map(|(column, field)| {
                        let value = encode_value(column.as_ref(), row)?;
                        if value.is_null() && !field.is_nullable() {
                            return Err(format!(
                                "nonnullable struct field {:?} returned NULL",
                                field.name()
                            )
                            .into());
                        }
                        Ok(value)
                    })
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
        DataType::Map(_, _) => {
            let entries = column.as_map().value(row);
            json!(
                (0..entries.len())
                    .map(|index| {
                        let key = encode_value(entries.column(0).as_ref(), index)?;
                        let value = encode_value(entries.column(1).as_ref(), index)?;
                        if key.is_null() || (value.is_null() && !entries.fields()[1].is_nullable())
                        {
                            return Err("nonnullable MAP key/value returned NULL".into());
                        }
                        Ok(json!([key, value]))
                    })
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            json!(
                column
                    .as_primitive::<IntervalYearMonthType>()
                    .value(row)
                    .to_string()
            )
        }
        DataType::Float32 => {
            let value = column.as_primitive::<Float32Type>().value(row);
            json!(if value.is_nan() {
                "nan".into()
            } else {
                format!("{:08x}", value.to_bits())
            })
        }
        DataType::Float64 => {
            let value = column.as_primitive::<Float64Type>().value(row);
            json!(if value.is_nan() {
                "nan".into()
            } else {
                format!("{:016x}", value.to_bits())
            })
        }
        _ => json!(array_value_to_string(column, row)?),
    })
}

#[test]
fn timestamp_ntz_values_use_recorded_datetime_encoding() -> TestResult {
    use arrow::array::TimestampMicrosecondArray;

    assert_eq!(
        spark_schema_type(&DataType::Timestamp(TimeUnit::Microsecond, None))?,
        json!("timestamp_ntz")
    );
    let values =
        TimestampMicrosecondArray::from(vec![Some(0), Some(1), Some(123000), Some(-1), None]);
    for (row, expected) in [
        json!("1970-01-01 00:00:00"),
        json!("1970-01-01 00:00:00.000001"),
        json!("1970-01-01 00:00:00.123000"),
        json!("1969-12-31 23:59:59.999999"),
        Value::Null,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(encode_value(&values, row)?, expected);
    }
    let outside_datetime_range = TimestampMicrosecondArray::from(vec![i64::MAX]);
    assert!(encode_value(&outside_datetime_range, 0).is_err());
    Ok(())
}
