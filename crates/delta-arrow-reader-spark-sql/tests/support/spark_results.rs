//! Capture full query rows and schemas for comparison with independent Spark records.

use std::error::Error;

use arrow::array::{Array, AsArray, ListArray, StructArray};
use arrow::datatypes::{DataType, Fields, Float32Type, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
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
    validate_physical_schema(resolved, &query.column_names, schema.fields())?;
    let public_schema = capture_schema(resolved, Some(&query.column_names))?;
    let physical_schema = capture_schema(schema.fields(), None)?;
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
        rows.sort_by_key(|row| json!(row).to_string());
        Ok(rows)
    }
    .await;
    let public_matches = case["schema_stages"]
        .get("analyzed")
        .is_none_or(|expected| public.as_ref() == Some(expected));
    let before_matches = case["schema_stages"]
        .get("executed_before_collect")
        .is_none_or(|expected| before.as_ref() == Some(expected));
    let executed_matches = case["schema_stages"]
        .get("executed")
        .is_none_or(|expected| executed.as_ref() == Some(expected));
    let rows_match = case["expected"]
        .get("rows")
        .is_none_or(|expected| actual.as_ref().is_ok_and(|rows| json!(rows) == *expected));
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
                .is_some_and(|error| error.to_string().contains(&format!("[{condition}]")))
    } else {
        actual.is_ok()
    };
    let matches =
        public_matches && before_matches && executed_matches && rows_match && error_matches;
    Ok(json!({
        "public_schema":public, "before_collect":before, "physical_schema":executed,
        "rows":actual.as_ref().ok(), "native_phase":phase,
        "error":actual.as_ref().err().map(ToString::to_string),
        "public_matches":public_matches, "before_matches":before_matches,
        "physical_matches":executed_matches, "rows_match":rows_match,
        "error_matches":error_matches, "matches":matches,
    }))
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
        DataType::Float32 => json!("float"),
        DataType::Float64 => json!("double"),
        DataType::Decimal128(precision, scale) => json!(format!("decimal({precision},{scale})")),
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
                        if key.is_null()
                            || (value.is_null() && !entries.fields()[1].is_nullable())
                        {
                            return Err("nonnullable MAP key/value returned NULL".into());
                        }
                        Ok(json!([key, value]))
                    })
                    .collect::<TestResult<Vec<_>>>()?
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
