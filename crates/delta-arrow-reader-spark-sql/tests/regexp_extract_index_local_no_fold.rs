//! Native-only local no-fold permissions and preservation controls. No Spark oracle credit.
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static PHASE: AtomicUsize = AtomicUsize::new(0);
static INDEX_CALLS: AtomicUsize = AtomicUsize::new(0);
static PRODUCER_CALLS: AtomicUsize = AtomicUsize::new(0);
static INVOCATIONS: Mutex<Vec<Value>> = Mutex::new(Vec::new());

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

#[derive(Debug, PartialEq, Eq, Hash)]
struct IndexSelectionCallback {
    folding: bool,
    producer: bool,
}

impl ScalarUDFImpl for IndexSelectionCallback {
    fn name(&self) -> &str {
        if self.producer {
            "producer_callback"
        } else {
            "index_callback"
        }
    }
    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }
    fn supports_constant_folding(&self) -> bool {
        self.folding
    }
    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(args[0].clone())
    }
    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let argument = &args.args[0];
        let values = match argument {
            ColumnarValue::Scalar(value) => json!({"kind":"scalar","value":format!("{value:?}")}),
            ColumnarValue::Array(array) => {
                let values = (0..array.len())
                    .map(|row| {
                        if array.is_null(row) {
                            Ok(Value::Null)
                        } else {
                            array_value_to_string(array.as_ref(), row).map(Value::String)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                json!({"kind":"array","values":values})
            }
        };
        let phase = match PHASE.load(Ordering::SeqCst) {
            0 => "resolve",
            1 => "physical",
            2 => "execute",
            _ => "unknown",
        };
        INVOCATIONS.lock().map_err(|_| datafusion::common::DataFusionError::Internal("callback mutex poisoned".into()))?
            .push(json!({"callback":self.name(),"phase":phase,"number_rows":args.number_rows,
                "argument_type":format!("{:?}",argument.data_type()),"argument":values,
                "arg_fields":fields(&args.arg_fields),"raw_arg_fields":format!("{:?}",args.arg_fields),
                "return_field":fields(&[Arc::clone(&args.return_field)])}));
        if self.producer {
            PRODUCER_CALLS.fetch_add(1, Ordering::SeqCst);
        } else {
            INDEX_CALLS.fetch_add(1, Ordering::SeqCst);
        }
        Ok(argument.clone())
    }
}

fn counts() -> Value {
    json!({"index":INDEX_CALLS.load(Ordering::SeqCst),"producer":PRODUCER_CALLS.load(Ordering::SeqCst)})
}
fn failure(error: &(dyn std::error::Error + 'static)) -> Value {
    let mut chain = Vec::new();
    let mut current = Some(error);
    while let Some(error) = current {
        chain.push(error.to_string());
        current = error.source();
    }
    json!({"error":error.to_string(),"error_debug":format!("{error:?}"),"error_chain":chain})
}
fn retain_error(actual: &mut Value, stage: &str, error: Value) {
    actual["error_stage"] = json!(stage);
    for key in ["error", "error_debug", "error_chain"] {
        actual[key] = error[key].clone();
    }
}

// Native field declarations and execution vectors preserve the measured control inventory.
fn native_field(name: &str, nullable: bool) -> Value {
    json!({"name":name,"type":"Utf8","nullable":nullable,"metadata":{}})
}

fn local_no_fold_contract(case: &Value, actual: &Value) -> Option<&'static str> {
    let id = case["id"].as_str()?;
    let ansi = case["ansi"].as_bool()?;
    let mixed = matches!(
        id,
        "mixed_nofold_then_required" | "mixed_required_then_nofold"
    );
    let producer = matches!(
        id,
        "producer_limit_zero" | "producer_live" | "producer_filter_false"
    );
    let native = matches!(id, "standalone_limit_zero" | "standalone_live");
    let live = matches!(id, "index_live" | "producer_live" | "standalone_live");
    if !matches!(
        id,
        "index_limit_zero"
            | "index_live"
            | "index_null_source_limit_zero"
            | "index_null_pattern_limit_zero"
            | "index_filter_false"
            | "mixed_nofold_then_required"
            | "mixed_required_then_nofold"
            | "producer_limit_zero"
            | "producer_live"
            | "producer_filter_false"
            | "standalone_limit_zero"
            | "standalone_live"
    ) {
        return None;
    }
    let zero = json!({"index":0,"producer":0});
    let analyzed = if mixed {
        json!([
            native_field("#6", id == "mixed_nofold_then_required"),
            native_field("#7", id == "mixed_required_then_nofold")
        ])
    } else {
        json!([native_field(
            if native {
                "r"
            } else if producer {
                "#9"
            } else {
                "#6"
            },
            true
        )])
    };
    let names = if id == "mixed_nofold_then_required" {
        json!(["deferred", "required"])
    } else if mixed {
        json!(["required", "deferred"])
    } else {
        json!(["r"])
    };
    if actual["after_resolve"] != zero
        || actual["after_physical"] != zero
        || actual["analyzed_fields"] != analyzed
        || actual["analyzed_metadata"] != json!({})
        || actual["column_names"] != names
        || !actual["analyzed_arrow_schema"].is_string()
        || !actual["analyzed_logical_schema"].is_string()
    {
        return None;
    }
    for key in [
        "analyzed_fields",
        "analyzed_metadata",
        "analyzed_arrow_schema",
        "analyzed_logical_schema",
        "column_names",
    ] {
        if actual["availability"][key] != "available" {
            return None;
        }
    }
    if mixed && ansi && actual.get("error").is_some() {
        let cause = "Arrow error: Cast error: Can't cast value 4294967296 to type Int32";
        let error = format!(
            "preserve_spark_expression_errors\ncaused by\n[CAST_OVERFLOW]\ncaused by\n{cause}"
        );
        if actual["error_stage"] != "physical"
            || actual["error"] != error
            || actual["error_chain"]
                != json!([
                    error,
                    format!("[CAST_OVERFLOW]\ncaused by\n{cause}"),
                    cause,
                    "Cast error: Can't cast value 4294967296 to type Int32"
                ])
            || actual.get("physical_fields").is_some()
            || actual.get("physical_arrow_schema").is_some()
            || actual.get("physical_metadata").is_some()
            || actual.get("rows").is_some()
            || actual.get("batches").is_some()
            || actual["error_debug"]
                != "Context(\"preserve_spark_expression_errors\", Context(\"[CAST_OVERFLOW]\", ArrowError(CastError(\"Can't cast value 4294967296 to type Int32\"), Some(\"\"))))"
            || actual.get("after_execute").is_some()
            || actual["invocations"] != json!([])
        {
            return None;
        }
        for key in [
            "physical_fields",
            "physical_metadata",
            "physical_arrow_schema",
            "rows",
            "batches",
        ] {
            if actual["availability"][key] != "absent" {
                return None;
            }
        }
        return Some("native_required_sibling_error");
    }
    let rows = if live {
        json!([[if native { "1" } else { "a" }]])
    } else {
        json!([])
    };
    let physical_fields = if id == "mixed_nofold_then_required" {
        json!([
            native_field("deferred", true),
            native_field("required", false)
        ])
    } else if mixed {
        json!([
            native_field("required", false),
            native_field("deferred", true)
        ])
    } else {
        json!([native_field("r", true)])
    };
    if (mixed && ansi)
        || actual.get("error").is_some()
        || actual.get("error_stage").is_some()
        || actual["physical_fields"] != physical_fields
        || actual["physical_metadata"] != json!({})
        || !actual["physical_arrow_schema"].is_string()
        || actual["rows"] != rows
    {
        return None;
    }
    let mut row_count = 0;
    for batch in actual["batches"].as_array()? {
        if batch["fields"] != physical_fields
            || batch["metadata"] != json!({})
            || !batch["raw_arrow_schema"].is_string()
        {
            return None;
        }
        row_count += batch["rows"].as_u64()?;
    }
    if row_count != (if live { 1 } else { 0 }) {
        return None;
    }
    for key in [
        "physical_fields",
        "physical_metadata",
        "physical_arrow_schema",
        "rows",
        "batches",
    ] {
        if actual["availability"][key] != "available" {
            return None;
        }
    }
    let counts = if !live {
        zero
    } else if producer {
        json!({"index":0,"producer":1})
    } else {
        json!({"index":1,"producer":0})
    };
    if actual["after_execute"] != counts {
        return None;
    }
    let expected_invocations = if !live {
        json!([])
    } else {
        let arg_name = if native || producer {
            "column1"
        } else {
            "column3"
        };
        let return_name = if native {
            "i"
        } else if producer {
            "#3"
        } else {
            "#5"
        };
        json!([{"callback":if producer {"producer_callback"} else {"index_callback"},"phase":"execute",
            "number_rows":1,"argument_type":"Utf8","argument":{"kind":"array","values":[if producer {"a"} else {"1"}]},
            "arg_fields":[native_field(arg_name,native)],"return_field":[native_field(return_name,native)],
            "raw_arg_fields":if native {"[Field { name: \"column1\", data_type: Utf8, nullable: true }]"}
                else if producer {"[Field { name: \"column1\", data_type: Utf8 }]"}
                else {"[Field { name: \"column3\", data_type: Utf8 }]"}}])
    };
    if actual["invocations"] != expected_invocations {
        return None;
    }
    Some("native_planning_permissions_and_preservation")
}

#[tokio::test]
async fn regexp_extract_index_local_no_fold_permissions() -> TestResult {
    let source = json!({"native_test":"regexp_extract_index_local_no_fold_permissions","spark_oracle_credit":false});
    let inputs: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-extract-index-local-no-fold-inputs.json"
    ))?;
    let label = "permanent-local-no-fold-controls";
    let cases = inputs["cases"].as_array().ok_or("missing no-fold inputs")?;
    assert_eq!(cases.len(), 24);
    assert_eq!(inputs["spark_oracle_credit"], false);
    eprintln!(
        "c06 local no fold header: {}",
        json!({"run_label":label,"source_manifest":source,
        "native_crate_cwd":env!("CARGO_MANIFEST_DIR"),"native_process_cwd":std::env::current_dir()?.display().to_string(),
        "cases":cases.len(),"registered_callbacks":inputs["callbacks"],"settings":inputs["settings"],
        "compatibility_evaluated":false,"spark_oracle_credit":false})
    );
    let mut differences = Vec::new();
    let mut identities = std::collections::HashSet::new();
    for case in cases {
        assert!(identities.insert((case["id"].as_str(), case["ansi"].as_bool())));
        let ansi_mode = case["ansi"].as_bool().ok_or("missing ANSI")?;
        let folding = case["folding"].as_bool().ok_or("missing folding flag")?;
        assert!(!folding);
        let sql = case["sql"].as_str().ok_or("missing SQL")?.to_owned();
        let surface = case["surface"].as_str().ok_or("missing surface")?;
        assert_eq!(case["ansi_option_applied"], surface != "standalone_native");
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
        for producer in [false, true] {
            session.register_udf(ScalarUDF::from(IndexSelectionCallback {
                folding,
                producer,
            }));
        }
        INVOCATIONS
            .lock()
            .map_err(|_| "callback mutex poisoned")?
            .clear();
        INDEX_CALLS.store(0, Ordering::SeqCst);
        PRODUCER_CALLS.store(0, Ordering::SeqCst);
        PHASE.store(0, Ordering::SeqCst);
        let mut actual = json!({});
        let physical = if surface == "standalone_native" {
            match session.sql(&sql).await {
                Ok(frame) => {
                    actual["after_resolve"] = counts();
                    actual["analyzed_fields"] = fields(frame.schema().fields());
                    actual["analyzed_metadata"] = json!(frame.schema().metadata());
                    actual["analyzed_arrow_schema"] =
                        json!(format!("{:?}", frame.schema().as_arrow()));
                    actual["analyzed_logical_schema"] = json!(format!("{:?}", frame.schema()));
                    actual["column_names"] = json!(
                        frame
                            .schema()
                            .fields()
                            .iter()
                            .map(|field| field.name())
                            .collect::<Vec<_>>()
                    );
                    PHASE.store(1, Ordering::SeqCst);
                    frame
                        .create_physical_plan()
                        .await
                        .map_err(|error| ("physical", failure(&error)))
                }
                Err(error) => Err(("resolve", failure(&error))),
            }
        } else {
            match plan_query(
                &session,
                &sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..Default::default()
                },
            )
            .await
            {
                Ok(query) => {
                    actual["after_resolve"] = counts();
                    actual["analyzed_fields"] = fields(query.logical_plan.schema().fields());
                    actual["analyzed_metadata"] = json!(query.logical_plan.schema().metadata());
                    actual["analyzed_arrow_schema"] =
                        json!(format!("{:?}", query.logical_plan.schema().as_arrow()));
                    actual["analyzed_logical_schema"] =
                        json!(format!("{:?}", query.logical_plan.schema()));
                    actual["analyzed_logical_plan"] = json!(format!("{:?}", query.logical_plan));
                    actual["column_names"] = json!(query.column_names);
                    PHASE.store(1, Ordering::SeqCst);
                    query
                        .create_physical_plan(&session)
                        .await
                        .map_err(|error| ("physical", failure(&error)))
                }
                Err(error) => Err(("resolve", failure(&error))),
            }
        };
        match physical {
            Err((stage, error)) => {
                retain_error(&mut actual, stage, error);
                actual[if stage == "resolve" {
                    "after_resolve"
                } else {
                    "after_physical"
                }] = counts();
            }
            Ok(physical) => {
                actual["after_physical"] = counts();
                actual["physical_fields"] = fields(physical.schema().fields());
                actual["physical_metadata"] = json!(physical.schema().metadata());
                actual["physical_arrow_schema"] = json!(format!("{:?}", physical.schema()));
                PHASE.store(2, Ordering::SeqCst);
                match datafusion::physical_plan::collect(physical, session.task_ctx()).await {
                    Err(error) => retain_error(&mut actual, "execute", failure(&error)),
                    Ok(batches) => {
                        let mut rows = Vec::new();
                        let mut batch_evidence = Vec::new();
                        for batch in batches {
                            batch_evidence.push(json!({"rows":batch.num_rows(),"fields":fields(batch.schema().fields()),
                                "metadata":batch.schema().metadata(),"raw_arrow_schema":format!("{:?}",batch.schema())}));
                            for row in 0..batch.num_rows() {
                                rows.push(
                                    (0..batch.num_columns())
                                        .map(|column| {
                                            let array = batch.column(column);
                                            if array.is_null(row) {
                                                Ok(Value::Null)
                                            } else {
                                                array_value_to_string(array.as_ref(), row)
                                                    .map(Value::String)
                                            }
                                        })
                                        .collect::<Result<Vec<_>, _>>()?,
                                );
                            }
                        }
                        actual["rows"] = json!(rows);
                        actual["batches"] = json!(batch_evidence);
                    }
                }
                actual["after_execute"] = counts();
            }
        }
        actual["invocations"] = json!(
            INVOCATIONS
                .lock()
                .map_err(|_| "callback mutex poisoned")?
                .clone()
        );
        let mut availability = json!({});
        for key in [
            "analyzed_fields",
            "analyzed_metadata",
            "analyzed_arrow_schema",
            "analyzed_logical_schema",
            "physical_fields",
            "physical_metadata",
            "physical_arrow_schema",
            "column_names",
            "rows",
            "batches",
        ] {
            availability[key] = json!(if actual.get(key).is_some() {
                "available"
            } else {
                "absent"
            });
        }
        actual["availability"] = availability;
        let contract = local_no_fold_contract(case, &actual);
        if contract.is_none() {
            differences.push(format!("{} ANSI={ansi_mode}: {actual}", case["id"]));
        }
        eprintln!(
            "c06 local no fold observation: {}",
            json!({"run_label":label,"id":case["id"],
            "folding":folding,"surface":surface,"ansi":ansi_mode,"ansi_option_applied":case["ansi_option_applied"],"sql":sql,"actual":actual,
            "native_contract":contract,"full_compatibility_pass":false,
            "compatibility_evaluated":false,"spark_oracle_credit":false})
        );
    }
    eprintln!(
        "c06 local no fold summary: {}",
        json!({"run_label":label,"observations":cases.len(),
        "collection_complete":true,"compatibility_evaluated":false,"spark_oracle_credit":false})
    );
    assert!(
        differences.is_empty(),
        "native local permission/preservation differences: {}",
        differences.join("\n")
    );
    Ok(())
}
