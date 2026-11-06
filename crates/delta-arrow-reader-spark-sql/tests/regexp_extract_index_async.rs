//! Native-only async source controls with no Apache Spark oracle credit.
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
#[path = "support/regexp_extract_index_native.rs"]
mod index_native;
use index_native::{control_key, native_controls, native_matches};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static SYNC_CALLS: AtomicUsize = AtomicUsize::new(0);
static ASYNC_CALLS: AtomicUsize = AtomicUsize::new(0);
static INVOCATIONS: Mutex<Vec<Value>> = Mutex::new(Vec::new());

#[derive(Debug, PartialEq, Eq, Hash)]
struct AsyncIdentity(Signature);
impl ScalarUDFImpl for AsyncIdentity {
    fn name(&self) -> &str {
        "index_async_identity"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> {
        SYNC_CALLS.fetch_add(1, Ordering::SeqCst);
        datafusion::common::exec_err!("async identity requires asynchronous invocation")
    }
}
impl AsyncScalarUDFImpl for AsyncIdentity {
    fn invoke_async_with_args<'life0, 'async_trait>(
        &'life0 self,
        args: ScalarFunctionArgs,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = datafusion::common::Result<ColumnarValue>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let argument = &args.args[0];
            let values = match argument {
                ColumnarValue::Scalar(value) => {
                    json!({"kind":"scalar","value":format!("{value:?}")})
                }
                ColumnarValue::Array(array) => {
                    json!({"kind":"array","values":(0..array.len()).map(|row| {
                    if array.is_null(row) { Ok(Value::Null) } else { array_value_to_string(array.as_ref(), row).map(Value::String) }
                }).collect::<Result<Vec<_>, _>>()?})
                }
            };
            INVOCATIONS.lock().map_err(|_| datafusion::common::DataFusionError::Internal("async callback mutex poisoned".into()))?
                .push(json!({"number_rows":args.number_rows,"arg_fields":format!("{:?}",args.arg_fields),"argument":values}));
            ASYNC_CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(argument.clone())
        })
    }
}
fn fields(values: &[Arc<Field>]) -> Value {
    json!(
        values
            .iter()
            .map(
                |field| json!({"name":field.name(),"type":format!("{:?}",field.data_type()),
        "nullable":field.is_nullable(),"metadata":field.metadata()})
            )
            .collect::<Vec<_>>()
    )
}
fn failure(error: &(dyn std::error::Error + 'static)) -> Value {
    let mut chain = Vec::new();
    let mut current = Some(error);
    while let Some(error) = current {
        chain.push(error.to_string());
        current = error.source();
    }
    json!({"error":error.to_string(),"error_chain":chain})
}
fn counts() -> Value {
    json!({"sync":SYNC_CALLS.load(Ordering::SeqCst),"async":ASYNC_CALLS.load(Ordering::SeqCst)})
}

#[tokio::test]
async fn regexp_extract_preserves_native_async_source_controls() -> TestResult {
    let source = json!({"native_test":"regexp_extract_preserves_native_async_source_controls","spark_oracle_credit":false});
    let inputs: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-extract-index-async-inputs.json"
    ))?;
    let profiles: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-extract-index-current-async-native.json"
    ))?;
    let controls = profiles["controls"]
        .as_array()
        .ok_or("missing native controls")?;
    let mut differences = Vec::new();
    let label = "permanent-async-controls";
    eprintln!(
        "c06 index async header: {}",
        json!({"run_label":label,"source_manifest":source,
        "native_crate_cwd":env!("CARGO_MANIFEST_DIR"),"native_process_cwd":std::env::current_dir()?.display().to_string(),
        "cases":6,"registered_inputs":inputs["registered_inputs"],"spark_oracle_credit":false,"compatibility_evaluated":false})
    );
    let cases = inputs["cases"].as_array().ok_or("missing async inputs")?;
    assert_eq!(cases.len(), 6);
    let keys = &["kind", "ansi"];
    let mut controls = native_controls(cases, controls, keys)?;
    for case in cases {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(32)
                            .with_target_partitions(1),
                    ),
            )
            .build(),
        );
        session.register_udf(
            AsyncScalarUDF::new(Arc::new(AsyncIdentity(Signature::exact(
                vec![DataType::Utf8],
                Volatility::Volatile,
            ))))
            .into_scalar_udf(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("p", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(StringArray::from(vec!["(a)", "(a)"])),
            ],
        )?;
        session.register_table(
            "rp788_async",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
        )?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?.to_owned();
        SYNC_CALLS.store(0, Ordering::SeqCst);
        ASYNC_CALLS.store(0, Ordering::SeqCst);
        INVOCATIONS
            .lock()
            .map_err(|_| "async callback mutex poisoned")?
            .clear();
        let mut actual = json!({});
        let physical = match plan_query(
            &session,
            &sql,
            &SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                ..Default::default()
            },
        )
        .await
        {
            Ok(query) => {
                actual["after_resolve"] = counts();
                actual["column_names"] = json!(query.column_names);
                actual["analyzed_fields"] = fields(query.logical_plan.schema().fields());
                actual["analyzed_arrow_schema"] =
                    json!(format!("{:?}", query.logical_plan.schema()));
                query
                    .create_physical_plan(&session)
                    .await
                    .map_err(|error| ("physical", failure(&error)))
            }
            Err(error) => Err(("resolve", failure(&error))),
        };
        match physical {
            Err((stage, error)) => {
                actual["error_stage"] = json!(stage);
                actual["error"] = error["error"].clone();
                actual["error_chain"] = error["error_chain"].clone();
                actual[if stage == "resolve" {
                    "after_resolve"
                } else {
                    "after_physical"
                }] = counts();
            }
            Ok(physical) => {
                actual["after_physical"] = counts();
                actual["physical_fields"] = fields(physical.schema().fields());
                actual["physical_arrow_schema"] = json!(format!("{:?}", physical.schema()));
                match datafusion::physical_plan::collect(physical, session.task_ctx()).await {
                    Ok(batches) => {
                        let mut rows = Vec::new();
                        for batch in batches {
                            for row in 0..batch.num_rows() {
                                rows.push(
                                    (0..batch.num_columns())
                                        .map(|column| {
                                            let values = batch.column(column);
                                            if values.is_null(row) {
                                                Ok(Value::Null)
                                            } else {
                                                array_value_to_string(values.as_ref(), row)
                                                    .map(Value::String)
                                            }
                                        })
                                        .collect::<Result<Vec<_>, _>>()?,
                                );
                            }
                        }
                        actual["rows"] = json!(rows);
                    }
                    Err(error) => {
                        let error = failure(&error);
                        actual["error_stage"] = json!("execute");
                        actual["error"] = error["error"].clone();
                        actual["error_chain"] = error["error_chain"].clone();
                    }
                }
                actual["after_execute"] = counts();
            }
        }
        actual["invocations"] = json!(
            INVOCATIONS
                .lock()
                .map_err(|_| "async callback mutex poisoned")?
                .clone()
        );
        let contract = controls
            .remove(&control_key(case, keys)?)
            .ok_or("missing native control")?;
        if !native_matches(&contract, case, &actual) {
            differences.push(format!("{} ANSI={}: {actual}", case["kind"], case["ansi"]));
        }
        eprintln!(
            "c06 index async observation: {}",
            json!({"run_label":label,"kind":case["kind"],"ansi":case["ansi"],"sql":sql,"actual":actual,"spark_oracle_credit":false,"compatibility_evaluated":false})
        );
    }
    eprintln!(
        "c06 index async summary: {}",
        json!({"run_label":label,"observations":6,"collection_complete":true,"compatibility_evaluated":false,"spark_oracle_credit":false})
    );
    assert!(controls.is_empty(), "unused native controls");
    assert!(
        differences.is_empty(),
        "unreviewed or changed native controls: {}",
        differences.join("\n")
    );
    Ok(())
}
