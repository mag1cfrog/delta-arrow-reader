//! Independent Spark observations for the bounded C06 string and binary scope.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int32Array, Int64Array, ListArray, StringArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionState;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use serde_json::{Value, json};

#[allow(
    clippy::duplicate_mod,
    reason = "Independently importable integration helpers share this source in mixed targets."
)]
#[path = "c06_receipts.rs"]
pub(crate) mod c06_receipts;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn decode_hex(value: &str) -> TestResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err("odd binary encoding length".into());
    }
    (0..value.len())
        .step_by(2)
        .map(|i| Ok(u8::from_str_radix(&value[i..i + 2], 16)?))
        .collect()
}

pub(crate) fn register_inputs(
    session: &SessionContext,
    metadata: &Value,
    representation: usize,
    batch_size: usize,
) -> TestResult {
    register_inputs_impl(session, metadata, representation, batch_size, None)
}

#[allow(dead_code, reason = "Only C06 callers request additive receipts.")]
pub(crate) async fn register_inputs_with_receipts(
    session: &SessionContext,
    metadata: &Value,
    representation: usize,
    batch_size: usize,
) -> TestResult<Value> {
    let mut registrations = Vec::new();
    if let Err(error) = register_inputs_impl(
        session,
        metadata,
        representation,
        batch_size,
        Some(&mut registrations),
    ) {
        c06_receipts::emit(
            "provider_setup_failure",
            json!({"representation":representation,
            "batch_size":batch_size,"target_partitions":session.state().config().target_partitions(),
            "input_metadata":metadata}),
            json!({"registrations":registrations,
                "error":c06_receipts::failure(error.as_ref(),"input_registration")}),
        );
        return Err(error);
    }
    c06_receipts::provider_readbacks(session, registrations, encode_value).await
}

fn register_inputs_impl(
    session: &SessionContext,
    metadata: &Value,
    representation: usize,
    batch_size: usize,
    mut receipts: Option<&mut Vec<Value>>,
) -> TestResult {
    let inputs = metadata
        .get("registered_inputs")
        .or_else(|| metadata.get("inputs"))
        .and_then(Value::as_array)
        .ok_or("missing inputs")?;
    for input in inputs {
        let rows = input["rows"].as_array().ok_or("missing rows")?;
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (index, field) in input["schema"]["fields"]
            .as_array()
            .ok_or("missing fields")?
            .iter()
            .enumerate()
        {
            let values: Vec<_> = rows.iter().map(|row| &row[index]).collect();
            let array: ArrayRef = match field["type"].as_str().ok_or("missing type")? {
                "string" => Arc::new(StringArray::from(
                    values.iter().map(|v| v.as_str()).collect::<Vec<_>>(),
                )),
                "integer" => Arc::new(Int32Array::from(
                    values
                        .iter()
                        .map(|v| {
                            if v.is_null() {
                                Ok(None)
                            } else {
                                Ok(Some(v.as_str().ok_or("missing integer")?.parse()?))
                            }
                        })
                        .collect::<TestResult<Vec<_>>>()?,
                )),
                "long" => Arc::new(Int64Array::from(
                    values
                        .iter()
                        .map(|v| {
                            if v.is_null() {
                                Ok(None)
                            } else {
                                Ok(Some(v.as_str().ok_or("missing long")?.parse()?))
                            }
                        })
                        .collect::<TestResult<Vec<_>>>()?,
                )),
                "boolean" => Arc::new(BooleanArray::from(
                    values
                        .iter()
                        .map(|v| {
                            if v.is_null() {
                                Ok(None)
                            } else {
                                Ok(Some(v.as_str().ok_or("missing boolean")?.parse::<bool>()?))
                            }
                        })
                        .collect::<TestResult<Vec<_>>>()?,
                )),
                "binary" => {
                    let decoded = values
                        .iter()
                        .map(|v| v.as_str().map(decode_hex).transpose())
                        .collect::<TestResult<Vec<_>>>()?;
                    Arc::new(BinaryArray::from(
                        decoded.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
                    ))
                }
                other => return Err(format!("unexpected input type: {other}").into()),
            };
            let target = match (array.data_type(), representation) {
                (DataType::Utf8, 1) => DataType::LargeUtf8,
                (DataType::Utf8, 2) => DataType::Utf8View,
                (DataType::Binary, 1) => DataType::LargeBinary,
                (DataType::Binary, 2) => DataType::BinaryView,
                (data_type, _) => data_type.clone(),
            };
            let array = cast(&array, &target)?;
            let field_metadata: HashMap<String, String> =
                serde_json::from_value(field["metadata"].clone())?;
            fields.push(
                Field::new(
                    field["name"].as_str().ok_or("missing name")?,
                    target,
                    field["nullable"].as_bool().ok_or("missing nullability")?,
                )
                .with_metadata(field_metadata),
            );
            columns.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let mut partitions = vec![Vec::new(); 2];
        for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[chunk % 2]
                .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        if let Some(receipts) = receipts.as_deref_mut() {
            let mut receipt =
                c06_receipts::registration(input, &batch, &batch, &partitions, encode_value)?;
            receipt["registration"]["slice"] =
                json!({"offset":0,"length":batch.num_rows(),"storage_rows":batch.num_rows()});
            receipts.push(receipt);
        }
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(schema, partitions)?),
        )?;
    }
    Ok(())
}

fn spark_schema_type(data_type: &DataType) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => json!("string"),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => json!("binary"),
        DataType::Int32 => json!("integer"),
        DataType::Int64 => json!("long"),
        DataType::Float64 => json!("double"),
        DataType::Boolean => json!("boolean"),
        DataType::Null => json!("void"),
        DataType::List(field) => json!({"type":"array",
            "elementType":spark_schema_type(field.data_type())?,
            "containsNull":field.is_nullable()}),
        other => return Err(format!("unexpected output type: {other}").into()),
    })
}

pub(crate) fn capture_schema(fields: &[Arc<Field>], names: Option<&[String]>) -> TestResult<Value> {
    Ok(json!({"type":"struct", "fields": fields.iter().enumerate()
        .map(|(index, field)| Ok(json!({
            "name":names.map_or(field.name(), |names| &names[index]),
            "type":spark_schema_type(field.data_type())?,
            "nullable":field.is_nullable(),"metadata":field.metadata()})))
        .collect::<TestResult<Vec<_>>>()?}))
}

pub(crate) fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column
        .logical_nulls()
        .as_ref()
        .is_some_and(|nulls| nulls.is_null(row))
    {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
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
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => {
            let binary = cast(column, &DataType::Binary)?;
            let binary = binary
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or("expected binary")?;
            json!(
                binary
                    .value(row)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            )
        }
        _ => json!(array_value_to_string(column, row)?),
    })
}

// Read collected batches without planning or evaluating the query again.
#[allow(
    dead_code,
    reason = "Integration tests select individual shared observers."
)]
pub(crate) fn capture_batches(batches: &[RecordBatch]) -> TestResult<Value> {
    let mut rows = Vec::new();
    let mut observed = Vec::new();
    for batch in batches {
        let mut batch_rows = Vec::new();
        for row in 0..batch.num_rows() {
            batch_rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode_value(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
        rows.extend(batch_rows.iter().cloned());
        observed.push(json!({
            "schema":capture_schema(batch.schema().fields(), None)?,
            "arrow_schema":format!("{:?}", batch.schema()),
            "column_types":batch.columns().iter().map(|column| format!("{:?}", column.data_type())).collect::<Vec<_>>(),
            "num_rows":batch.num_rows(),"rows":batch_rows,
        }));
    }
    Ok(json!({"rows":rows,"batches":observed}))
}

// Retain fields from each completed planning stage when a later stage fails.
// Existing callers keep capture_result's Result-based interface below.
pub(crate) async fn capture_observation(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    capture_observation_with_state(session, case, None).await
}

// Fixed-clock controls supply the real query state; ordinary captures retain
// the public SparkSqlQuery planning path.
pub(crate) async fn capture_observation_with_state(
    session: &SessionContext,
    case: &Value,
    state: Option<SessionState>,
) -> TestResult<Value> {
    capture_observation_impl(session, case, state, None).await
}

#[allow(dead_code, reason = "Only C06 callers request additive receipts.")]
pub(crate) async fn capture_observation_with_receipt(
    session: &SessionContext,
    case: &Value,
) -> TestResult<(Value, Value)> {
    let mut receipt = c06_receipts::new("public_physical_collect");
    let actual = capture_observation_impl(session, case, None, Some(&mut receipt)).await?;
    c06_receipts::finish(&mut receipt);
    Ok((actual, receipt))
}

async fn capture_observation_impl(
    session: &SessionContext,
    case: &Value,
    state: Option<SessionState>,
    mut receipt: Option<&mut Value>,
) -> TestResult<Value> {
    let settings = case
        .get("settings")
        .cloned()
        .map(serde_json::from_value::<std::collections::BTreeMap<String, String>>)
        .transpose()?
        .unwrap_or_default();
    let variable_substitution_enabled = settings
        .get("spark.sql.variable.substitute")
        .map(|value| value.parse::<bool>())
        .transpose()?
        .unwrap_or(true);
    let query = match plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            variable_substitution_enabled,
            variable_substitution_values: settings,
            ..Default::default()
        },
    )
    .await
    {
        Ok(query) => query,
        Err(error) => {
            if let Some(receipt) = receipt.as_deref_mut() {
                receipt["error"] = c06_receipts::failure(&error, "resolve");
            }
            return Ok(json!({
                "error":format!("resolve: {error}"),
                "error_stage":"resolve",
                "analyzed_schema":null,
                "physical_schema":null,
                "analyzed_arrow_schema":null,
                "physical_arrow_schema":null,
            }));
        }
    };
    if let Some(receipt) = receipt.as_deref_mut() {
        receipt["public_names"] = json!(query.column_names);
        receipt["analyzed"] = c06_receipts::schema(query.logical_plan.schema().as_arrow());
        receipt["analyzed_plan"] = json!(format!("{:?}", query.logical_plan));
    }
    let analyzed_schema = capture_schema(
        query.logical_plan.schema().fields(),
        Some(&query.column_names),
    )?;
    let mut observation = json!({
        "analyzed_schema":analyzed_schema,
        "analyzed_arrow_schema":format!("{:?}",query.logical_plan.schema()),
        "physical_schema":null,
        "physical_arrow_schema":null,
    });
    if state.is_some() {
        observation["query_column_names"] = json!(query.column_names);
    }
    let physical = match state {
        Some(mut state) => {
            sail_plan::like_errors::set_spark_query_start_time(&mut state);
            DataFrame::new(state, query.logical_plan.clone())
                .create_physical_plan()
                .await
                .and_then(|physical| rename_physical_plan(physical, &query.column_names))
        }
        None => query.create_physical_plan(session).await,
    };
    let physical = match physical {
        Ok(physical) => physical,
        Err(error) => {
            if let Some(receipt) = receipt.as_deref_mut() {
                receipt["error"] = c06_receipts::failure(&error, "physical");
            }
            observation["error"] = json!(format!("physical: {error}"));
            observation["error_stage"] = json!("physical");
            return Ok(observation);
        }
    };
    let schema = physical.schema();
    if let Some(receipt) = receipt.as_deref_mut() {
        receipt["before_collect"] = c06_receipts::schema(schema.as_ref());
        receipt["physical"] = c06_receipts::schema(schema.as_ref());
        receipt["physical_plan"] = json!(format!("{physical:?}"));
    }
    observation["physical_schema"] = capture_schema(schema.fields(), None)?;
    observation["physical_arrow_schema"] = json!(format!("{schema:?}"));
    let collected = collect(Arc::clone(&physical), session.task_ctx()).await;
    if let Some(receipt) = receipt.as_deref_mut() {
        receipt["physical"] = c06_receipts::schema(physical.schema().as_ref());
    }
    let batches = match collected {
        Ok(batches) => batches,
        Err(error) => {
            if let Some(receipt) = receipt.as_deref_mut() {
                receipt["error"] = c06_receipts::failure(&error, "execute");
            }
            observation["error"] = json!(format!("execute: {error}"));
            observation["error_stage"] = json!("execute");
            return Ok(observation);
        }
    };
    let mut rows = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema(), schema);
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for (index, column) in batch.columns().iter().enumerate() {
                let value = encode_value(column.as_ref(), row)?;
                if value.is_null() {
                    assert!(schema.field(index).is_nullable());
                    assert!(query.logical_plan.schema().field(index).is_nullable());
                }
                values.push(value);
            }
            rows.push(values);
        }
    }
    if let Some(receipt) = receipt {
        let mut output = capture_batches(&batches)?;
        let typed = c06_receipts::batches(&batches, encode_value)?;
        for (batch, actual) in output["batches"]
            .as_array_mut()
            .ok_or("missing output batches")?
            .iter_mut()
            .zip(
                typed["batches"]
                    .as_array()
                    .ok_or("missing typed output batches")?,
            )
        {
            batch["storage_schema"] = actual["schema"].clone();
            batch["columns"] = actual["columns"].clone();
        }
        output["order"] = json!("raw_collection_order");
        receipt["output"] = output;
    }
    observation["rows"] = json!(rows);
    Ok(observation)
}

pub(crate) async fn capture_result(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let observation = capture_observation(session, case).await?;
    if let Some(error) = observation["error"].as_str() {
        return Err(error.to_owned().into());
    }
    Ok(observation)
}

#[test]
fn actual_list_capture_retains_child_fields_and_nonnull_values() -> TestResult {
    use arrow::datatypes::Int32Type;
    let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), None, Some(3)]),
        None,
    ]);
    let child = Arc::new(
        Field::new("actual_items", DataType::Int32, true)
            .with_metadata(HashMap::from([("source".into(), "actual_arrow".into())])),
    );
    let values = ListArray::new(
        child,
        values.offsets().clone(),
        values.values().clone(),
        values.nulls().cloned(),
    );
    let fields = vec![Arc::new(Field::new("r", values.data_type().clone(), true))];
    assert_eq!(
        capture_schema(&fields, None)?,
        json!({"type":"struct","fields":[{
        "name":"r","type":{"type":"array","elementType":"integer","containsNull":true},
        "nullable":true,"metadata":{}}]})
    );
    let raw = format!("{fields:?}");
    assert!(raw.contains("actual_items") && raw.contains("actual_arrow"));
    assert_eq!(encode_value(&values, 0)?, json!(["1", null, "3"]));
    assert_eq!(encode_value(&values, 1)?, Value::Null);
    Ok(())
}
