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

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

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

pub(crate) async fn capture_spark_query_result(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    Ok(capture_spark_query_observation(session, case).await?["result"].take())
}

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
    let mut state = session.state();
    let extensions = &mut state.config_mut().options_mut().extensions;
    extensions.insert(SparkSqlExpressionSemantics::default());
    extensions.insert(PreserveAggregateStatisticsSchema::default());
    let optimized = state.optimize(&query.logical_plan)?;
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
        .then(|| {
            capture_schema(optimized.schema().fields(), Some(&query.column_names))
        })
        .transpose()?;
    Ok(json!({"raw_schema":raw_schema,"schema":schema,"names_match":names_match}))
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
        rows_raw = Some(rows.clone());
        rows.sort_by_key(|row| json!(row).to_string());
        Ok(rows)
    }
    .await;
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
    let ordered_rows_match = case["ordered"] != true
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
