//! Actual objects retained by the C06 receipt paths during their existing execution.
#![allow(dead_code, reason = "Integration targets select individual C06 receipt helpers.")]

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use serde_json::{Value, json};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) fn emit(kind: &str, mut context: Value, receipt: Value) {
    context["receipt"] = receipt;
    eprintln!("c06 receipt {kind}: {context}");
}

pub(crate) fn field(field: &Field) -> Value {
    let children = match field.data_type() {
        DataType::List(child) | DataType::LargeList(child)
        | DataType::ListView(child) | DataType::LargeListView(child)
        | DataType::FixedSizeList(child, _) | DataType::Map(child, _) => {
            vec![self::field(child)]
        }
        DataType::Struct(children) => children.iter().map(|child| self::field(child)).collect(),
        DataType::Union(children, _) => children.iter().map(|(id, child)| {
            json!({"type_id":id,"field":self::field(child)})
        }).collect(),
        DataType::RunEndEncoded(runs, values) => vec![self::field(runs), self::field(values)],
        _ => Vec::new(),
    };
    json!({"name":field.name(),"type":format!("{:?}",field.data_type()),
        "nullable":field.is_nullable(),"metadata":field.metadata(),"children":children,
        "raw_field":format!("{field:?}")})
}

pub(crate) fn fields(fields: &[Arc<Field>]) -> Value {
    json!(fields.iter().map(|value| field(value)).collect::<Vec<_>>())
}

pub(crate) fn schema(schema: &Schema) -> Value {
    json!({"fields":fields(schema.fields()),"metadata":schema.metadata(),
        "raw_schema":format!("{schema:?}")})
}

pub(crate) fn new(handle: &str) -> Value {
    json!({"handle":handle,"public_names":null,"analyzed":null,"before_collect":null,
        "physical":null,"output":null,"error":null,
        "native_condition":{"status":"unavailable","reason":"No structured native condition API on this error path."},
        "native_sqlstate":{"status":"unavailable","reason":"No native SQLSTATE API on this error path."},
        "native_parameters":{"status":"unavailable","reason":"No native parameter API on this error path."}})
}

pub(crate) fn failure(error: &(dyn std::error::Error + 'static), stage: &str) -> Value {
    let mut chain = Vec::new();
    let mut current = Some(error);
    while let Some(cause) = current {
        chain.push(json!({"display":cause.to_string(),"debug":format!("{cause:?}")}));
        current = cause.source();
    }
    json!({"stage":stage,"display":error.to_string(),"debug":format!("{error:?}"),"chain":chain})
}

fn availability(actual: &Value, keys: &[&str]) -> Value {
    let mut availability = serde_json::Map::new();
    for &key in keys {
        availability.insert(key.into(), json!(match actual.get(key) {
            None => "absent", Some(Value::Null) => "null", Some(_) => "available",
        }));
    }
    Value::Object(availability)
}

pub(crate) fn finish(receipt: &mut Value) {
    receipt["availability"] = availability(receipt,
        &["public_names", "analyzed", "optimized", "before_collect", "physical", "output", "error"]);
}

pub(crate) fn batches(
    batches: &[RecordBatch],
    encode: impl Fn(&dyn Array, usize) -> TestResult<Value>,
) -> TestResult<Value> {
    let mut rows = Vec::new();
    let mut observed = Vec::new();
    for batch in batches {
        let batch_rows = (0..batch.num_rows()).map(|row| {
            batch.columns().iter().map(|column| encode(column.as_ref(), row))
                .collect::<TestResult<Vec<_>>>()
        }).collect::<TestResult<Vec<_>>>()?;
        rows.extend(batch_rows.iter().cloned());
        observed.push(json!({"schema":schema(batch.schema().as_ref()),
            "column_types":batch.columns().iter().map(|column| format!("{:?}",column.data_type())).collect::<Vec<_>>(),
            "columns":batch.columns().iter().map(|column| {
                let data=column.to_data();
                let ieee_bits = match column.data_type() {
                    DataType::Float32 => Some((0..column.len()).map(|row| {
                        if column.is_null(row) { Value::Null } else {
                            json!(format!("{:08x}",column.as_primitive::<Float32Type>().value(row).to_bits()))
                        }
                    }).collect::<Vec<_>>()),
                    DataType::Float64 => Some((0..column.len()).map(|row| {
                        if column.is_null(row) { Value::Null } else {
                            json!(format!("{:016x}",column.as_primitive::<Float64Type>().value(row).to_bits()))
                        }
                    }).collect::<Vec<_>>()),
                    _ => None,
                };
                json!({"len":column.len(),"null_count":column.null_count(),"offset":data.offset(),
                    "buffer_lengths":data.buffers().iter().map(|buffer|buffer.len()).collect::<Vec<_>>(),
                    "ieee_bits":ieee_bits})
            }).collect::<Vec<_>>(),"num_rows":batch.num_rows(),"rows":batch_rows}));
    }
    Ok(json!({"rows":rows,"batches":observed,"order":"raw_collection_order"}))
}

pub(crate) fn registration(
    input: &Value,
    storage: &RecordBatch,
    visible: &RecordBatch,
    partitions: &[Vec<RecordBatch>],
    encode: impl Fn(&dyn Array, usize) -> TestResult<Value> + Copy,
) -> TestResult<Value> {
    Ok(json!({"name":input["name"],"input_definition":input,
        "registration":{"storage":batches(std::slice::from_ref(storage),encode)?,
            "visible":batches(std::slice::from_ref(visible),encode)?,
            "partitions":partitions.iter().map(|partition|batches(partition,encode)).collect::<TestResult<Vec<_>>>()?}}))
}

fn provider_failure(
    completed: &mut Vec<Value>,
    mut receipt: Value,
    error: &(dyn std::error::Error + 'static),
    stage: &str,
) -> Value {
    receipt["provider"]["error"] = failure(error, stage);
    receipt["provider"]["availability"] = availability(&receipt["provider"],
        &["schema", "readback_sql", "readback", "error"]);
    completed.push(receipt);
    json!({"providers":completed,"error":failure(error, stage)})
}

pub(crate) async fn provider_readbacks(
    session: &SessionContext,
    registrations: Vec<Value>,
    encode: impl Fn(&dyn Array, usize) -> TestResult<Value> + Copy,
) -> TestResult<Value> {
    let mut receipts = Vec::new();
    for mut receipt in registrations {
        receipt["provider"] = json!({"schema":null,"readback_sql":null,"readback":null,"error":null});
        let mut stage = "provider_lookup";
        let result: TestResult<()> = async {
            let name=receipt["name"].as_str().ok_or("missing provider name")?;
            let provider=session.table_provider(name).await?;
            let actual_schema=provider.schema();
            let quote=|value: &str| format!("\"{}\"",value.replace('"',"\"\""));
            let sql=format!("SELECT * FROM {} ORDER BY {}",quote(name),quote(actual_schema.field(0).name()));
            receipt["provider"]=json!({"schema":schema(actual_schema.as_ref()),
                "readback_sql":sql,"readback_order":"sorted_by_first_actual_provider_field",
                "readback":null,"error":null});
            stage = "readback_sql";
            let frame=session.sql(&sql).await?;
            stage = "readback_collect";
            let readback=frame.collect().await?;
            stage = "readback_encode";
            receipt["provider"]["readback"]=batches(&readback,encode)?;
            Ok(())
        }.await;
        if let Err(error) = result {
            let retained = provider_failure(&mut receipts, receipt, error.as_ref(), stage);
            emit("provider_failure",json!({"batch_size":session.state().config().batch_size(),
                "target_partitions":session.state().config().target_partitions()}),retained);
            return Err(error);
        }
        receipts.push(receipt);
    }
    Ok(json!(receipts))
}

#[test]
fn receipts_retain_nested_actual_fields_batches_and_error_sources() -> TestResult<()> {
    use arrow::array::Int32Array;
    use std::collections::HashMap;
    let child=Arc::new(Field::new("actual_child",DataType::Int32,false)
        .with_metadata(HashMap::from([("source".into(),"actual".into())])));
    let parent=Field::new("items",DataType::List(child),true);
    let observed=field(&parent);
    assert_eq!(observed["children"][0]["name"],"actual_child");
    assert_eq!(observed["children"][0]["metadata"]["source"],"actual");
    let batch=RecordBatch::try_from_iter(vec![("n",Arc::new(Int32Array::from(vec![2,1])) as Arc<dyn Array>)])?;
    let output=batches(&[batch],|column,row|Ok(json!(arrow::util::display::array_value_to_string(column,row)?)))?;
    assert_eq!(output["rows"],json!([["2"],["1"]]));
    #[derive(Debug)]
    struct Outer(std::io::Error);
    impl std::fmt::Display for Outer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("outer")
        }
    }
    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(&self.0) }
    }
    let error=Outer(std::io::Error::other("inner"));
    let observed=failure(&error,"collect");
    assert_eq!(observed["chain"][0]["display"],"outer");
    assert_eq!(observed["chain"][1]["display"],"inner");
    assert_eq!(observed["chain"].as_array().map(Vec::len),Some(2));
    assert!(observed["debug"].as_str().is_some_and(|text|text.contains("inner")));
    let completed=json!({"name":"completed","provider":{"readback":output}});
    let failed=json!({"name":"failed","provider":{"schema":null,"readback_sql":null,"readback":null,"error":null}});
    let mut providers=vec![completed.clone()];
    let retained=provider_failure(&mut providers,failed,&error,"provider_lookup");
    assert_eq!(retained["providers"][0],completed);
    assert_eq!(retained["providers"][1]["name"],"failed");
    assert_eq!(retained["providers"][1]["provider"]["error"]["stage"],"provider_lookup");
    assert_eq!(retained["providers"][1]["provider"]["error"]["chain"][1]["display"],"inner");
    assert_eq!(retained["providers"][1]["provider"]["availability"]["readback"],"null");
    assert_eq!(retained["error"]["display"],error.to_string());
    assert_eq!(availability(&json!({"output_batches":null}),&["output_batches", "missing"]),
        json!({"output_batches":"null","missing":"absent"}));
    let mut receipt=new("test");
    receipt["output"]=output;
    finish(&mut receipt);
    assert_eq!(receipt["availability"]["output"],"available");
    assert_eq!(receipt["availability"]["analyzed"],"null");
    assert_eq!(receipt["availability"]["optimized"],"absent");
    Ok(())
}
