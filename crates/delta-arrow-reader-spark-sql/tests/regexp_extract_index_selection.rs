//! Native-only index-selection controls with complete callback and field observations.
use std::sync::{Arc, LazyLock, Mutex, atomic::{AtomicUsize, Ordering}};

use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static CALLS: AtomicUsize = AtomicUsize::new(0);
static INVOCATIONS: Mutex<Vec<Value>> = Mutex::new(Vec::new());

#[allow(dead_code)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::register_inputs;

#[derive(Debug, PartialEq, Eq, Hash)]
struct IndexSelectionCallback { folding: bool }

impl ScalarUDFImpl for IndexSelectionCallback {
    fn name(&self) -> &str { "index_callback" }
    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }
    fn supports_constant_folding(&self) -> bool { self.folding }
    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> { Ok(args[0].clone()) }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::common::Result<FieldRef> { Ok(Arc::clone(&args.arg_fields[0])) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> {
        let argument = &args.args[0];
        let values = match argument {
            ColumnarValue::Scalar(value) => json!({"kind":"scalar","value":format!("{value:?}")}),
            ColumnarValue::Array(array) => {
                let values = (0..array.len()).map(|row| {
                    if array.is_null(row) { Ok(Value::Null) }
                    else { array_value_to_string(array.as_ref(), row).map(Value::String) }
                }).collect::<Result<Vec<_>, _>>()?;
                json!({"kind":"array","values":values})
            }
        };
        INVOCATIONS.lock().map_err(|_| datafusion::common::DataFusionError::Internal("callback mutex poisoned".into()))?
            .push(json!({"number_rows":args.number_rows,"argument_type":format!("{:?}",argument.data_type()),
                "arg_fields":format!("{:?}",args.arg_fields),"argument":values}));
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(argument.clone())
    }
}

fn fields(values: &[Arc<Field>]) -> Value {
    json!(values.iter().map(|field| json!({"name":field.name(),"type":format!("{:?}",field.data_type()),
        "nullable":field.is_nullable(),"metadata":field.metadata()})).collect::<Vec<_>>())
}
fn failure(error: &(dyn std::error::Error + 'static)) -> Value {
    let mut chain = Vec::new();
    let mut current = Some(error);
    while let Some(error) = current { chain.push(error.to_string()); current = error.source(); }
    json!({"error":error.to_string(),"error_chain":chain})
}

#[tokio::test]
async fn regexp_extract_index_selection_controls() -> TestResult {
    let source = json!({"native_test":"regexp_extract_index_selection_controls","spark_oracle_credit":false});
    let label = "permanent-selection-controls";
    eprintln!("c06 index selection header: {}", json!({"run_label":label,"source_manifest":source,
        "native_crate_cwd":env!("CARGO_MANIFEST_DIR"),"native_process_cwd":std::env::current_dir()?.display().to_string(),
        "cases":32,"registered_inputs_from_manifest":true,"compatibility_evaluated":false,"spark_oracle_credit":false}));
    let inputs: Value = serde_json::from_str(include_str!("fixtures/regexp-extract-index-selection-inputs.json"))?;
    let profiles: Value = serde_json::from_str(include_str!("fixtures/regexp-extract-index-selection-native.json"))?;
    let controls = profiles["controls"].as_array().ok_or("missing native controls")?;
    let mut differences = Vec::new();
    let cases = inputs["cases"].as_array().ok_or("missing callback inputs")?;
    assert_eq!(cases.len(), 32);
    let mut observed = 0;
    for case in cases {
                    let ansi_mode = case["ansi"].as_bool().ok_or("missing ANSI")?;
                    let folding = case["folding"].as_bool().ok_or("missing folding flag")?;
                    let kind = case["kind"].as_str().ok_or("missing index kind")?;
                    let surface = case["surface"].as_str().ok_or("missing surface")?;
                    let sql = case["sql"].as_str().ok_or("missing SQL")?.to_owned();
                    let session = SessionContext::new_with_state(configure_spark_sql(
                        SessionStateBuilder::new().with_default_features().with_config(
                            SessionConfig::new().with_batch_size(32).with_target_partitions(2))).build());
                    register_inputs(&session, &inputs, 0, 32)?;
                    session.register_udf(ScalarUDF::from(IndexSelectionCallback { folding }));
                    INVOCATIONS.lock().map_err(|_| "callback mutex poisoned")?.clear();
                    CALLS.store(0, Ordering::SeqCst);
                    let mut actual = json!({});
                    let physical = if surface == "standalone_native" {
                        match session.sql(&sql).await {
                            Ok(frame) => {
                                actual["after_resolve"] = json!(CALLS.load(Ordering::SeqCst));
                                actual["analyzed_arrow_schema"] = json!(format!("{:?}", frame.schema()));
                                actual["analyzed_fields"] = fields(frame.schema().fields());
                                frame.create_physical_plan().await.map_err(|error| ("physical", failure(&error)))
                            }
                            Err(error) => Err(("resolve", failure(&error))),
                        }
                    } else {
                        match plan_query(&session, &sql, &SparkSqlOptions { ansi_mode, ..Default::default() }).await {
                            Ok(query) => {
                                actual["after_resolve"] = json!(CALLS.load(Ordering::SeqCst));
                                actual["analyzed_arrow_schema"] = json!(format!("{:?}", query.logical_plan.schema()));
                                actual["analyzed_fields"] = fields(query.logical_plan.schema().fields());
                                actual["column_names"] = json!(query.column_names);
                                query.create_physical_plan(&session).await.map_err(|error| ("physical", failure(&error)))
                            }
                            Err(error) => Err(("resolve", failure(&error))),
                        }
                    };
                    match physical {
                        Err((stage, error)) => {
                            actual["error_stage"] = json!(stage);
                            actual["error"] = error["error"].clone();
                            actual["error_chain"] = error["error_chain"].clone();
                            let field = if stage == "resolve" { "after_resolve" } else { "after_physical" };
                            actual[field] = json!(CALLS.load(Ordering::SeqCst));
                        }
                        Ok(physical) => {
                            actual["after_physical"] = json!(CALLS.load(Ordering::SeqCst));
                            actual["physical_arrow_schema"] = json!(format!("{:?}",physical.schema()));
                            actual["physical_fields"] = fields(physical.schema().fields());
                            match datafusion::physical_plan::collect(physical,session.task_ctx()).await {
                                Err(error) => {
                                    let error = failure(&error);actual["error_stage"] = json!("execute"); actual["error"] = error["error"].clone(); actual["error_chain"] = error["error_chain"].clone();
                                }
                                Ok(batches) => {
                                    let mut rows = Vec::new();
                                    for batch in batches {
                                        for row in 0..batch.num_rows() {
                                            rows.push((0..batch.num_columns()).map(|column| {
                                                let array = batch.column(column);
                                                if array.is_null(row) { Ok(Value::Null) }
                                                else { array_value_to_string(array.as_ref(), row).map(Value::String) }
                                            }).collect::<Result<Vec<_>, _>>()?);
                                        }
                                    }
                                    actual["rows"] = json!(rows);
                                }
                            }
                            actual["after_execute"] = json!(CALLS.load(Ordering::SeqCst));
                        }
                    }
                    actual["invocations"] = json!(INVOCATIONS.lock().map_err(|_| "callback mutex poisoned")?.clone());
                    observed += 1;
                    let contract = controls.iter().find(|value| value["kind"] == case["kind"] && value["folding"] == case["folding"]
                        && value["surface"] == case["surface"] && value["ansi"] == case["ansi"] && value["reviewed"] == true);
                    if !contract.is_some_and(|value| value["actual"] == actual) {
                        differences.push(format!("{kind} folding={folding} surface={surface} ANSI={ansi_mode}: {actual}"));
                    }
                    eprintln!("c06 index selection observation: {}", json!({"run_label":label,"kind":kind,
                        "folding":folding,"surface":surface,"ansi":ansi_mode,"sql":sql,"actual":actual,
                        "compatibility_evaluated":false,"spark_oracle_credit":false}));
    }
    assert_eq!(observed,32);
    eprintln!("c06 index selection summary: {}",json!({"run_label":label,"observations":observed,
        "collection_complete":true,"compatibility_evaluated":false,"spark_oracle_credit":false}));
    assert!(differences.is_empty(), "unreviewed or changed native controls: {}", differences.join("\n"));
    Ok(())
}
