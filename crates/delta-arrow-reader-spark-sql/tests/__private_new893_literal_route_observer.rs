//! Temporary native extension observations for #893; no Apache Spark UDF oracle credit.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock, Mutex, atomic::{AtomicUsize, Ordering}};

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, Result as DfResult, exec_err};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[expect(dead_code, reason = "Only the maintained batch codec is reused by this private observer.")]
#[path = "support/string_binary.rs"]
mod string_binary;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static PHASE: AtomicUsize = AtomicUsize::new(0);
static EVENTS: Mutex<Vec<Value>> = Mutex::new(Vec::new());
const NAMES: [&str; 6] = [
    "retain_constant", "index_callback", "mixed_counted_producer",
    "allowed_child", "eligible_failure", "position_callback",
];

fn fields(fields: &[FieldRef]) -> Value {
    json!(fields.iter().map(|field| json!({
        "name":field.name(), "type":format!("{:?}",field.data_type()),
        "nullable":field.is_nullable(), "metadata":field.metadata(),
        "raw_field":format!("{field:?}")
    })).collect::<Vec<_>>())
}

fn event(name: &str, method: &str, detail: Value) -> DfResult<()> {
    let phase = ["resolve", "physical", "execute"][PHASE.load(Ordering::SeqCst)];
    EVENTS.lock().map_err(|_| datafusion::common::DataFusionError::Internal(
        "893 callback mutex poisoned".into()
    ))?.push(json!({"callback":name,"method":method,"phase":phase,"detail":detail}));
    Ok(())
}

fn counts() -> TestResult<Value> {
    let events = EVENTS.lock().map_err(|_| "893 callback mutex poisoned")?;
    let mut counts = json!({});
    for name in NAMES {
        let mut count = json!({});
        for method in ["invoke", "return_field", "return_type"] {
            count[method] = json!(events.iter().filter(|event|
                event["callback"] == name && event["method"] == method
            ).count());
        }
        count["invoke_by_phase"] = json!({});
        for phase in ["resolve", "physical", "execute"] {
            count["invoke_by_phase"][phase] = json!(events.iter().filter(|event|
                event["callback"] == name && event["method"] == "invoke" && event["phase"] == phase
            ).count());
        }
        counts[name] = count;
    }
    Ok(counts)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Callback {
    name: &'static str,
    folding: bool,
    fail: bool,
}

impl ScalarUDFImpl for Callback {
    fn name(&self) -> &str { self.name }

    fn signature(&self) -> &Signature {
        static ANY: LazyLock<Signature> = LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        static FLOAT: LazyLock<Signature> = LazyLock::new(||
            Signature::exact(vec![DataType::Float64], Volatility::Immutable));
        static INT: LazyLock<Signature> = LazyLock::new(||
            Signature::exact(vec![DataType::Int32], Volatility::Immutable));
        match self.name {
            "retain_constant" => &FLOAT,
            "mixed_counted_producer" => &INT,
            _ => &ANY,
        }
    }

    fn supports_constant_folding(&self) -> bool { self.folding }

    fn propagates_constant_folding_errors(&self) -> bool { self.name == "eligible_failure" }

    fn return_type(&self, args: &[DataType]) -> DfResult<DataType> {
        event(self.name, "return_type", json!({"types":format!("{args:?}")}))?;
        Ok(args[0].clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
        event(self.name, "return_field", json!({"arg_fields":fields(args.arg_fields),
            "scalar_arguments":format!("{:?}",args.scalar_arguments)}))?;
        Ok(Arc::clone(&args.arg_fields[0]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        event(self.name, "invoke", json!({
            "number_rows":args.number_rows, "args":format!("{:?}",args.args),
            "arg_fields":fields(&args.arg_fields),
            "return_field":fields(&[Arc::clone(&args.return_field)])
        }))?;
        if self.fail {
            exec_err!("893_callback_invoked:{}", self.name)
        } else {
            Ok(args.args[0].clone())
        }
    }
}

fn error(error: &(dyn std::error::Error + 'static)) -> Value {
    let mut chain = Vec::new();
    let mut cause = Some(error);
    while let Some(value) = cause {
        chain.push(value.to_string());
        cause = value.source();
    }
    json!({"display":error.to_string(),"debug":format!("{error:?}"),"chain":chain})
}

fn analyzed(schema: &DFSchema, column_names: Value) -> Value {
    json!({"fields":fields(schema.fields()),"metadata":schema.metadata(),
        "arrow_schema":format!("{:?}",schema.as_arrow()),
        "logical_schema":format!("{schema:?}"),"column_names":column_names})
}

fn session(spark: bool, fail: bool, position_folding: bool) -> TestResult<SessionContext> {
    let builder = SessionStateBuilder::new().with_default_features().with_config(
        SessionConfig::new().with_batch_size(32).with_target_partitions(1)
    );
    let session = SessionContext::new_with_state(
        if spark { configure_spark_sql(builder) } else { builder }.build()
    );
    for name in NAMES {
        let folding = matches!(name, "allowed_child" | "eligible_failure")
            || (name == "position_callback" && position_folding);
        let function = ScalarUDF::from(Callback {
            name, folding, fail: name == "eligible_failure" || (fail && !folding),
        });
        session.register_udf(if name == "retain_constant" {
            function.with_aliases(["retained_alias"])
        } else { function });
    }
    let schema = Arc::new(Schema::new_with_metadata(vec![
        Field::new("v", DataType::Float64, false)
            .with_metadata([("893_input".into(),"required".into())].into()),
        Field::new("n", DataType::Float64, true)
            .with_metadata([("893_input".into(),"nullable".into())].into()),
    ], [("893_provider".into(),"one_partition_one_batch".into())].into()));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Float64Array::from(vec![2.0, 4.0])),
        Arc::new(Float64Array::from(vec![Some(2.0), None])),
    ])?;
    session.register_table("probe_input", Arc::new(MemTable::try_new(
        Arc::clone(&schema), vec![vec![batch.clone()]]
    )?))?;
    session.register_table("probe_empty", Arc::new(MemTable::try_new(
        schema, vec![vec![batch.slice(0,0)]]
    )?))?;
    Ok(session)
}

async fn observe(session: &SessionContext, sql: &str, spark: bool, ansi: bool) -> TestResult<Value> {
    let mut actual = json!({
        "resolve":{"status":"pending"},
        "physical":{"status":"unavailable","reason":"resolve_not_completed"},
        "execute":{"status":"unavailable","reason":"physical_not_completed"}
    });
    PHASE.store(0, Ordering::SeqCst);
    let physical: Result<Arc<dyn ExecutionPlan>, (&str, Value)> = if spark {
        match plan_query(session, sql, &SparkSqlOptions {
            ansi_mode:ansi, session_timezone:"UTC".into(), ..Default::default()
        }).await {
            Ok(query) => {
                actual["resolve"] = json!({"status":"ok","counts":counts()?,
                    "schema":analyzed(query.logical_plan.schema(),json!(query.column_names)),
                    "logical_plan":format!("{:?}",query.logical_plan)});
                PHASE.store(1, Ordering::SeqCst);
                query.create_physical_plan(session).await.map_err(|e| ("physical",error(&e)))
            }
            Err(e) => Err(("resolve",error(&e))),
        }
    } else {
        match session.sql(sql).await {
            Ok(frame) => {
                actual["resolve"] = json!({"status":"ok","counts":counts()?,
                    "schema":analyzed(frame.schema(),json!(frame.schema().fields().iter()
                        .map(|field| field.name()).collect::<Vec<_>>())),
                    "logical_plan":format!("{:?}",frame.logical_plan())});
                PHASE.store(1, Ordering::SeqCst);
                frame.create_physical_plan().await.map_err(|e| ("physical",error(&e)))
            }
            Err(e) => Err(("resolve",error(&e))),
        }
    };
    match physical {
        Err((phase,failure)) => {
            actual[phase] = json!({"status":"error","counts":counts()?,"error":failure});
            actual["error_phase"] = json!(phase);
        }
        Ok(physical) => {
            actual["physical"] = json!({"status":"ok","counts":counts()?,"schema":{
                "fields":fields(physical.schema().fields()),
                "metadata":physical.schema().metadata(),
                "arrow_schema":format!("{:?}",physical.schema())
            }});
            PHASE.store(2, Ordering::SeqCst);
            actual["execute"] = match collect(physical, session.task_ctx()).await {
                Ok(batches) => {
                    let result = string_binary::capture_batches(&batches)?;
                    json!({"status":"ok","counts":counts()?,"result":result,
                        "batch_fields":batches.iter().map(|batch| json!({
                            "fields":fields(batch.schema().fields()),
                            "metadata":batch.schema().metadata(),
                            "arrow_schema":format!("{:?}",batch.schema())
                        })).collect::<Vec<_>>()})
                }
                Err(e) => {
                    actual["error_phase"] = json!("execute");
                    json!({"status":"error","counts":counts()?,"error":error(&e)})
                }
            };
        }
    }
    actual["events"] = json!(EVENTS.lock().map_err(|_| "893 callback mutex poisoned")?.clone());
    Ok(actual)
}

#[tokio::test]
async fn private_893_phase_observations() -> TestResult {
    let run_label = std::env::var("DAR_893_RUN_LABEL")?;
    let product_head = std::env::var("DAR_893_PRODUCT_HEAD")?;
    let private_head = std::env::var("DAR_893_PRIVATE_HEAD")?;
    eprintln!("private_893_header={}",json!({
        "run_label":run_label,"product_head":product_head,"private_head":private_head,
        "manifest_dir":env!("CARGO_MANIFEST_DIR"),"cwd":std::env::current_dir()?,
        "batch_size":32,"target_partitions":1,
        "native_session_options":format!("{:?}",session(false,false,false)?.state().config_options()),
        "spark_session_options":format!("{:?}",session(true,false,false)?.state().config_options()),
        "spark_options_true":format!("{:?}",SparkSqlOptions::default()),
        "spark_options_false":format!("{:?}",SparkSqlOptions { ansi_mode:false,..Default::default() }),
        "callback_declaration":"clone argument Field; retain_constant exact Float64; mixed_counted_producer exact Int32; others any(1)",
        "alias":"retained_alias forwards retain_constant permission and trace",
        "spark_oracle_credit":false,"compatibility_evaluated":false
    }));
    let mut cases: Vec<(String,String,bool)> = [
        ("division", "SELECT retain_constant(1/1) AS r"),
        ("addition", "SELECT retain_constant(1+1) AS r"),
        ("function_alias", "SELECT retained_alias(1/1) AS r"),
        ("projection_alias", "SELECT r FROM (SELECT retain_constant(1/1) AS r) AS u"),
        ("arithmetic_parent", "SELECT retain_constant(1/1)+1 AS r"),
        ("cast_parent", "SELECT CAST(retain_constant(1/1) AS STRING) AS r"),
        ("array_parent", "SELECT ARRAY(retain_constant(1/1)) AS r"),
        ("allowed_child", "SELECT retain_constant(allowed_child(1+1)/1) AS r"),
        ("failing_eligible_child", "SELECT retain_constant(eligible_failure(1)/1) AS r"),
        ("required_cast_child", "SELECT retain_constant(CAST('bad' AS DOUBLE)/1) AS r"),
        ("dead_case", "SELECT CASE WHEN false THEN retain_constant(1/0) ELSE CAST(7 AS DOUBLE) END AS r"),
        ("null_argument", "SELECT retain_constant(CAST(NULL AS DOUBLE)/1) AS r"),
        ("null_arithmetic_parent", "SELECT CAST(NULL AS DOUBLE)+retain_constant(1/0) AS r"),
        ("concat_null_first", "SELECT CONCAT(CAST(NULL AS STRING),CAST(retain_constant(1/1) AS STRING)) AS r"),
        ("concat_null_last", "SELECT CONCAT(CAST(retain_constant(1/0) AS STRING),CAST(NULL AS STRING)) AS r"),
        ("limit_zero", "SELECT retain_constant(1/1) AS r LIMIT 0"),
        ("filter_false", "SELECT retain_constant(1/1) AS r WHERE false"),
        ("provider_required", "SELECT retain_constant(v/1.0) AS r FROM probe_input"),
        ("provider_nullable", "SELECT retain_constant(n/1.0) AS r FROM probe_input"),
        ("provider_empty", "SELECT retain_constant(v/1.0) AS r FROM probe_empty"),
        ("mixed_numeric_array", "SELECT (1 DIV 0)+ARRAY(mixed_counted_producer(7))[1] AS r"),
        ("mixed_array_numeric", "SELECT ARRAY(mixed_counted_producer(7))[1]+(1 DIV 0) AS r"),
        ("array_only", "SELECT ARRAY(mixed_counted_producer(7))[1] AS r"),
        ("required_first", "SELECT (1 DIV 0)+retain_constant(1/1) AS r"),
        ("required_last", "SELECT retain_constant(1/1)+(1 DIV 0) AS r"),
    ].into_iter().map(|(id,sql)| (id.to_owned(),sql.to_owned(),false)).collect();
    for (route,expression) in [
        ("extract", "regexp_extract(s,p,CAST(index_callback(i) AS BIGINT))"),
        ("cast_only", "CAST(index_callback(i) AS BIGINT)"),
    ] {
        for parent in [false,true] {
            for empty in [false,true] {
                let columns = if parent { "r,(SELECT 1) AS q" } else { "r" };
                let limit = if empty { " LIMIT 0" } else { "" };
                cases.push((format!("{route}_scalar_parent_{parent}_limit_zero_{empty}"),
                    format!("SELECT {columns} FROM (SELECT {expression} AS r FROM VALUES('a','(a)','1') AS t(s,p,i)) AS u{limit}"), false));
            }
        }
    }
    for (kind,argument) in [
        ("int","2"), ("bigint","CAST(2 AS BIGINT)"),
        ("string","'2'"), ("double","CAST(1.9 AS DOUBLE)"),
    ] {
        for folding in [false,true] {
            cases.push((format!("regexp_position_{kind}_folding_{folding}"),
                format!("SELECT regexp_replace('a1a','a','X',position_callback({argument})) AS r"),folding));
        }
    }
    cases.push(("standalone_position_int".into(),"SELECT position_callback(2) AS r".into(),false));
// Insert into a new private copy before `let mut keys`; keep the frozen observer unchanged.
cases.extend([
    (
        "literal_route_round_scale".to_owned(),
        "SELECT round(CAST(1.25 AS DOUBLE),index_callback(1)) AS r".to_owned(),
        false,
    ),
    (
        "literal_route_json_schema".to_owned(),
        "SELECT j.a AS r FROM (SELECT from_json('{\"a\":1}',position_callback('a INT')) AS j) AS q".to_owned(),
        false,
    ),
    (
        "literal_route_rows_bound".to_owned(),
        "SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN index_callback(1) PRECEDING AND CURRENT ROW) AS r FROM probe_input".to_owned(),
        false,
    ),
    (
        "literal_route_identifier".to_owned(),
        "SELECT IDENTIFIER(position_callback('v')) AS r FROM probe_input".to_owned(),
        false,
    ),
]);

    let mut keys = HashSet::new();
    let mut observations = 0;
    for (id,sql,position_folding) in &cases {
        for ansi in [true,false] {
            for fail in [false,true] {
                for spark in [true,false] {
                    let surface = if spark { "spark" } else { "native_unconfigured" };
                    let key = format!("{id}/ansi={ansi}/error_if_invoked={fail}/surface={surface}");
                    assert!(keys.insert(key.clone()), "duplicate observer identity: {key}");
                    let session = session(spark,fail,*position_folding)?;
                    EVENTS.lock().map_err(|_| "893 callback mutex poisoned")?.clear();
                    let actual = observe(&session,sql,spark,ansi).await?;
                    eprintln!("private_893_observation={}",json!({
                        "key":key,"id":id,"sql":sql,"ansi":ansi,"ansi_option_applied":spark,
                        "surface":surface,"error_if_invoked":fail,"position_folding":position_folding,
                        "actual":actual,"spark_oracle_credit":false,"compatibility_evaluated":false
                    }));
                    observations += 1;
                }
            }
        }
    }
    assert_eq!(observations,cases.len()*8);
    eprintln!("private_893_summary={}",json!({
        "case_definitions":cases.len(),"observations":observations,"unique_keys":keys.len(),
        "contract_pass_evaluated":false,"spark_oracle_credit":false
    }));
    Ok(())
}
