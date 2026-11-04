//! Shared computed projections retain Spark row-error priority and callback reuse.
//! Retained input sorts stay below their row-error consumers.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlan, ScalarFunctionArgs, Volatility, create_udf,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use futures_util::FutureExt;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use sail_plan::like_errors::set_spark_like_query_clock;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const SPARK_REFERENCE_SHA256: &str =
    "bef5d407f51ae133ba0ba7537bf0cec326a058201c4a7782e46ec3fe198423c5";
const MANIFEST_SHA256: &str = "5c680b99a395fd2b3b5be1a6ad0e0ea337d3536a4922db0b88d05bb2f58bccd6";
const CAPTURE_SHA256: &str = "a7a17804e6a585b6203771cc53dbeb4ba0483171f4b42bf3bc41c40d5528ee3c";
const VERIFIER_SHA256: &str = "90b085cd04c91b35d55d480a404e768954f4d8fa25a88135c20034f544256841";
const FLAGS: &[&str] = &[
    "matches",
    "optimized_matches",
    "public_matches",
    "before_matches",
    "physical_matches",
    "rows_match",
    "ordered_rows_match",
    "error_matches",
    "cast_input_matches",
];
const STAGES: &[&str] = &[
    "public",
    "resolved",
    "analyzed",
    "optimized",
    "physical",
    "executed_before_collect",
    "executed",
];

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|message| (*message).into())
        })
        .unwrap_or_else(|| "non-string panic payload".into())
}

fn source_sha256(source: &str) -> TestResult<String> {
    let digest = datafusion::functions::crypto::sha256().invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            source.into(),
        )))],
        arg_fields: vec![Arc::new(Field::new("source", DataType::Utf8, false))],
        number_rows: 1,
        return_field: Arc::new(Field::new("digest", DataType::Binary, false)),
        config_options: Default::default(),
    })?;
    let ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) = digest else {
        return Err("SHA256 did not return a binary scalar".into());
    };
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn session(batch_size: usize) -> SessionContext {
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

fn register_one_partition(
    session: &SessionContext,
    name: &str,
    batch: RecordBatch,
    batch_size: usize,
) -> TestResult {
    let chunks = (0..batch.num_rows())
        .step_by(batch_size)
        .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
        .collect::<Vec<_>>();
    session.register_table(
        name,
        Arc::new(MemTable::try_new(batch.schema(), vec![chunks])?),
    )?;
    Ok(())
}

fn column_uses(expr: &Arc<dyn PhysicalExpr>, path: Vec<usize>, uses: &mut Vec<Value>) {
    if let Some(column) = expr.downcast_ref::<Column>() {
        uses.push(json!({"index":column.index(),"name":column.name(),"expression_path":path}));
    }
    for (index, child) in expr.children().into_iter().enumerate() {
        let mut child_path = path.clone();
        child_path.push(index);
        column_uses(child, child_path, uses);
    }
}

fn projection_bindings(plan: &Arc<dyn ExecutionPlan>, path: Vec<usize>) -> TestResult<Vec<Value>> {
    let mut observations = Vec::new();
    if let Some(parent) = plan.downcast_ref::<ProjectionExec>() {
        let input = parent.input();
        let mut uses = Vec::new();
        for (output, expression) in parent.expr().iter().enumerate() {
            column_uses(&expression.expr, vec![output], &mut uses);
        }
        let mut by_index = BTreeMap::<usize, Vec<Value>>::new();
        for usage in &uses {
            let index = usage["index"].as_u64().ok_or("missing physical index")? as usize;
            assert!(
                index < input.schema().fields().len(),
                "physical column bounds"
            );
            assert_eq!(
                usage["name"],
                input.schema().field(index).name().as_str(),
                "bind columns to the actual input field, including resolver aliases"
            );
            by_index.entry(index).or_default().push(usage.clone());
        }
        let boundary_children = input.children();
        let producer_input = if input.name() == "InputSortBoundaryExec" {
            let [producer] = boundary_children.as_slice() else {
                return Err("an input-sort boundary must have exactly one producer".into());
            };
            assert_eq!(
                input.schema(),
                producer.schema(),
                "sort preserves every bound output index"
            );
            *producer
        } else {
            input
        };
        let mut shared = Vec::new();
        if let Some(producer) = producer_input.downcast_ref::<ProjectionExec>() {
            assert_eq!(producer_input.schema(), parent.input().schema());
            for (index, consumers) in &by_index {
                let expression = producer
                    .expr()
                    .get(*index)
                    .ok_or("missing producer field")?;
                let computed = expression.expr.downcast_ref::<Column>().is_none()
                    && expression.expr.downcast_ref::<Literal>().is_none();
                if computed && consumers.len() > 1 {
                    shared.push(json!({"producer_output_index":index,
                        "producer_expression":expression.expr.to_string(),
                        "producer_prefix_transport":producer.preserves_row_error_prefix(),
                        "producer_return_field":format!("{:?}", expression.expr.return_field(producer.input().schema().as_ref())?),
                        "consumers":consumers}));
                }
            }
        }
        observations.push(json!({"plan_path":path,"input_operator":input.name(),
            "prefix_transport":parent.preserves_row_error_prefix(),
            "parent_expressions":parent.expr().iter().map(|expression| expression.expr.to_string()).collect::<Vec<_>>(),
            "input_schema":spark_results::capture_schema(input.schema().fields(), None)?,
            "parent_schema":spark_results::capture_schema(parent.schema().fields(), None)?,
            "column_uses":uses,"shared_computed_producers":shared,
            "binding_basis":"actual column index through the schema-preserving input-sort boundary when present, not common-expression field names"}));
    }
    for (index, child) in plan.children().into_iter().enumerate() {
        let mut child_path = path.clone();
        child_path.push(index);
        observations.extend(projection_bindings(child, child_path)?);
    }
    Ok(observations)
}

fn bound_schema_observation(
    fields: &Fields,
    internal_names: &[String],
    public_names: &[String],
) -> TestResult<Value> {
    if fields.len() != internal_names.len() || fields.len() != public_names.len() {
        return Err("schema stage field count differs from its output bindings".into());
    }
    let mut bindings = Vec::with_capacity(fields.len());
    for (index, ((field, internal), public)) in
        fields.iter().zip(internal_names).zip(public_names).enumerate()
    {
        if field.name() != internal {
            return Err(format!("schema stage output {index} binds {:?}, expected {internal:?}", field.name()).into());
        }
        bindings.push(json!({"index":index,"internal_name":internal,"public_name":public}));
    }
    Ok(json!({
        "raw_schema":spark_results::capture_schema(fields,None)?,
        "arrow_fields":format!("{fields:?}"),
        "schema":spark_results::capture_schema(fields,Some(public_names))?,
        "output_bindings":bindings,
    }))
}

#[test]
fn stage_schema_capture_keeps_actual_declarations_and_checks_output_names() -> TestResult {
    let internal = vec!["#4".to_owned()];
    let public = vec!["r".to_owned()];
    let base: Fields = vec![Field::new("#4", DataType::Int32, false)].into();
    let expected = json!({"type":"struct","fields":[
        {"name":"r","type":"integer","nullable":false,"metadata":{}}
    ]});
    let captured = bound_schema_observation(&base,&internal,&public)?;
    assert_eq!(captured["schema"],expected);
    assert_eq!(captured["raw_schema"]["fields"][0]["name"],"#4");
    assert_eq!(captured["output_bindings"],json!([
        {"index":0,"internal_name":"#4","public_name":"r"}
    ]));
    for field in [
        Field::new("#4",DataType::Int64,false),
        Field::new("#4",DataType::Int32,true),
        Field::new("#4",DataType::Int32,false)
            .with_metadata([("stage".into(),"changed".into())].into()),
    ] {
        let changed: Fields = vec![field].into();
        assert_ne!(bound_schema_observation(&changed,&internal,&public)?["schema"],expected,
            "a differing stage type, nullability, or metadata must not be replaced by resolved fields");
    }
    let renamed: Fields = vec![Field::new("#5",DataType::Int32,false)].into();
    assert!(bound_schema_observation(&renamed,&internal,&public).is_err());
    assert!(bound_schema_observation(&base,&internal,&[]).is_err());
    Ok(())
}

async fn plan_observation(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            session_timezone: "UTC".into(),
            ..Default::default()
        },
    )
    .await?;
    let internal_names = query.logical_plan.schema().fields().iter()
        .map(|field| field.name().clone()).collect::<Vec<_>>();
    let resolved_schema = bound_schema_observation(
        query.logical_plan.schema().fields(), &internal_names, &query.column_names,
    )?;
    let mut state = session.state();
    set_spark_like_query_clock(&mut state);
    let extensions = &mut state.config_mut().options_mut().extensions;
    extensions.insert(SparkSqlExpressionSemantics::default());
    extensions.insert(PreserveAggregateStatisticsSchema::default());
    // Observe the configured analyzer separately. Production state.optimize must
    // still receive the original resolved plan, since it runs its own analyzer.
    let analyzed = state.analyzer().execute_and_check(
        query.logical_plan.clone(), state.config_options(), |_, _| {},
    )?;
    let analyzed_schema = bound_schema_observation(
        analyzed.schema().fields(), &internal_names, &query.column_names,
    )?;
    let optimized = state.optimize(&query.logical_plan)?;
    let optimized_schema = bound_schema_observation(
        optimized.schema().fields(), &internal_names, &query.column_names,
    )?;
    let planned = state.query_planner().create_physical_plan(&optimized, &state).await?;
    let planned_schema = bound_schema_observation(
        planned.schema().fields(), &internal_names, &query.column_names,
    )?;
    let physical = rename_physical_plan(planned, &query.column_names)?;
    let execution_schema = bound_schema_observation(
        physical.schema().fields(), &query.column_names, &query.column_names,
    )?;
    let bindings = projection_bindings(&physical, vec![])?;
    let shared = bindings
        .iter()
        .map(|binding| {
            binding["shared_computed_producers"]
                .as_array()
                .map_or(0, Vec::len)
        })
        .sum::<usize>();
    Ok(
        json!({"logical_resolved":query.logical_plan.display_indent_schema().to_string(),
        "logical_analyzed":analyzed.display_indent_schema().to_string(),
        "logical_optimized":optimized.display_indent_schema().to_string(),
        "physical":displayable(physical.as_ref()).indent(true).to_string(),
        "physical_partitions":physical.output_partitioning().partition_count(),
        "public_schema":spark_results::capture_schema(query.logical_plan.schema().fields(), Some(&query.column_names))?,
        "physical_schema":spark_results::capture_schema(physical.schema().fields(), None)?,
        "schema_stages":{
            "public":resolved_schema,"resolved":resolved_schema,
            "analyzed":analyzed_schema,"optimized":optimized_schema,
            "physical":planned_schema,"executed_before_collect":execution_schema,
        },
        "schema_stage_sources":{
            "public":"Sail resolved output with its preserved public names",
            "resolved":"plan_query logical_plan before session analysis",
            "analyzed":"configured analyzer.execute_and_check result",
            "optimized":"SessionState::optimize on the original resolved plan, including production analysis",
            "physical":"completed query-planner physical plan before public output rename",
            "executed_before_collect":"same completed physical boundary after production output rename; no separate initial/AQE stage is exposed",
        },
        "optimizer_rules":state.optimizer().rules.iter().map(|rule| rule.name()).collect::<Vec<_>>(),
        "prefix_producers":bindings.iter().filter(|binding| binding["prefix_transport"] == true).count(),
        "input_sort_boundaries":bindings.iter().filter(|binding| binding["input_operator"] == "InputSortBoundaryExec").count(),
        "projection_bindings":bindings,"shared_computed_projection_edges":shared,
        "shared_projection_status":if shared > 0 { "observed" } else { "unverified_absent_in_actual_plan" },
        "cse_origin":"inspect SQL and optimized logical plan; explicit query projections are not automatically attributed to CSE",
        "execution":"plan-only independent planning pass; replay uses the shared strict observer"}),
    )
}

async fn input_observation(session: &SessionContext, sql: &str) -> TestResult<Value> {
    let frame = session.sql(sql).await?;
    let physical = frame.create_physical_plan().await?;
    let schema = physical.schema();
    let partitions = physical.output_partitioning().partition_count();
    let plan = displayable(physical.as_ref()).indent(true).to_string();
    let mut rows = Vec::new();
    let mut batches = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        spark_results::validate_batch(schema.as_ref(), &batch)?;
        batches.push(batch.num_rows());
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| spark_results::encode_value(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    Ok(
        json!({"schema":spark_results::capture_schema(schema.fields(),None)?,
        "partitions":partitions,"physical_plan":plan,"batch_rows":batches,"rows_raw":rows}),
    )
}

fn cast_input_matches(case: &Value, observation: &Value) -> bool {
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

#[tokio::test]
async fn shared_projections_and_retained_input_sort_match_spark() -> TestResult {
    let raw = include_str!("fixtures/slice-shared-projection-errors-920/spark-reference.jsonl");
    let manifest_source = include_str!("fixtures/slice-shared-projection-errors-920/manifest.json");
    assert_eq!(source_sha256(manifest_source)?, MANIFEST_SHA256);
    assert_eq!(
        source_sha256(include_str!(
            "fixtures/slice-shared-projection-errors-920/capture_spark.py"
        ))?,
        CAPTURE_SHA256
    );
    assert_eq!(
        source_sha256(include_str!(
            "fixtures/slice-shared-projection-errors-920/verify_capture.py"
        ))?,
        VERIFIER_SHA256
    );
    assert_eq!(
        source_sha256(raw)?,
        SPARK_REFERENCE_SHA256,
        "frozen Spark reference bytes"
    );
    let manifest: Value = serde_json::from_str(manifest_source)?;
    let records = raw
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 7);
    let header = &records[0];
    for key in [
        "group",
        "spark_version",
        "spark_revision",
        "source_commit",
        "timezone",
        "inputs",
        "spark_config",
    ] {
        assert_eq!(header[key], manifest[key], "{key}");
    }
    assert_eq!(header["capture_sha256"], CAPTURE_SHA256);
    assert_eq!(header["manifest_sha256"], MANIFEST_SHA256);
    assert_eq!(header["provider_input_roundtrip_checked"], true);
    let identities = records[1..]
        .iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    assert_eq!(json!(identities), manifest["identities"]);
    let provider = &header["inputs"][0];
    assert_eq!(provider["name"], "slice_priority");
    assert_eq!(
        provider["rows"],
        json!([
            ["0", "10", "0", "1"],
            ["1", "array_bad", "start_bad", "length_bad"],
            ["2", "10", "1", "-1"],
            ["3", "array_bad", "start_bad", "length_bad"]
        ])
    );
    let mut differences = Vec::new();
    let mut completions = std::collections::BTreeSet::new();
    for batch in [1, 32] {
        let session = session(batch);
        register_one_partition(
            &session,
            "slice_priority",
            spark_inputs::decode_recorded_batch(provider)?,
            batch,
        )?;
        let provider_input = input_observation(
            &session,
            "SELECT k, array_text, start_text, length_text FROM slice_priority",
        )
        .await?;
        assert_eq!(provider_input["partitions"], 1);
        assert_eq!(provider_input["schema"], provider["schema"]);
        assert_eq!(provider_input["rows_raw"], provider["rows"]);
        eprintln!(
            "SLICE_920_PLAN_PROVIDER {}",
            json!({"batch":batch,"observation":provider_input})
        );
        for case in &records[1..] {
            let definition = manifest["scenarios"]
                .as_array()
                .ok_or("missing definitions")?
                .iter()
                .find(|definition| definition["id"] == case["id"])
                .ok_or("missing definition")?;
            for (key, value) in definition.as_object().ok_or("missing definition object")? {
                assert_eq!(&case[key], value, "{key}");
            }
            let input = input_observation(
                &session,
                case["input_sql"].as_str().ok_or("missing input SQL")?,
            )
            .await?;
            let expected_input = header["scenario_input_roundtrips"]
                .as_array()
                .ok_or("missing input roundtrips")?
                .iter()
                .find(|item| item["id"] == case["id"])
                .ok_or("missing roundtrip")?;
            let input_matches = input["partitions"] == 1
                && input["schema"] == expected_input["schema"]
                && input["rows_raw"] == expected_input["rows_in_partition_order"];
            let plans = match std::panic::AssertUnwindSafe(plan_observation(&session, case))
                .catch_unwind()
                .await
            {
                Ok(result) => result,
                Err(payload) => Err(panic_message(payload).into()),
            };
            let mut observation = match std::panic::AssertUnwindSafe(
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case),
            )
            .catch_unwind()
            .await
            {
                Ok(Ok(observation)) => observation,
                Ok(Err(error)) => json!({"observer_error":error.to_string()}),
                Err(payload) => json!({"panic":panic_message(payload)}),
            };
            observation["id"] = case["id"].clone();
            observation["ansi"] = case["ansi"].clone();
            observation["batch"] = json!(batch);
            observation["sql"] = case["sql"].clone();
            observation["input_observation"] = input;
            observation["input_matches"] = json!(input_matches);
            observation["cast_input_matches"] = json!(cast_input_matches(case, &observation));
            observation["plan_observation"] = plans.as_ref().ok().cloned().unwrap_or(Value::Null);
            observation["plan_observer_error"] =
                json!(plans.as_ref().err().map(ToString::to_string));
            let required = case["schema_stages"]
                .as_object()
                .ok_or("missing reference stages")?;
            for stage in STAGES.iter().filter(|stage| required.contains_key(**stage)) {
                let evidence = plans.as_ref().ok()
                    .map(|plan| plan["schema_stages"][stage].clone()).unwrap_or(Value::Null);
                let actual = evidence["schema"].clone();
                observation["schema_stage_evidence"][stage] = evidence;
                observation["schema_stage_sources"][stage] = plans.as_ref().ok()
                    .map(|plan| plan["schema_stage_sources"][stage].clone()).unwrap_or(Value::Null);
                observation["schema_stage_observations"][stage] = actual.clone();
                observation["schema_stage_matches"][stage] = json!(actual == required[*stage]);
            }
            let strict = FLAGS.iter().all(|flag| observation[flag] == true)
                && input_matches
                && plans.is_ok()
                && observation["schema_stage_matches"]
                    .as_object()
                    .is_some_and(|stages| {
                        stages.len() == required.len() && stages.values().all(|value| value == true)
                    })
                && observation.get("observer_error").is_none()
                && observation.get("panic").is_none();
            observation["strict_matches"] = json!(strict);
            eprintln!("SLICE_920_PLAN_OBSERVATION {observation}");
            let plan = plans.as_ref().map_err(|error| error.to_string())?;
            assert_eq!(
                plan["shared_computed_projection_edges"], 1,
                "one actual shared producer"
            );
            let sorted = case["id"] == "logical_sort_below_outer_slice";
            assert_eq!(
                plan["input_sort_boundaries"],
                if sorted { 1 } else { 0 },
                "only the original input sort becomes an execution boundary"
            );
            assert_eq!(
                plan["prefix_producers"],
                if sorted { 0 } else { 1 },
                "admission follows real lineage and retains the original logical sort boundary"
            );
            if !strict {
                differences.push(observation);
            }
            assert!(completions.insert((
                case["id"].as_str().ok_or("missing ID")?.to_owned(),
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch
            )));
        }
    }
    assert_eq!(completions.len(), 12);
    eprintln!(
        "SLICE_920_PLAN_COMPLETION {}",
        json!({"identities":completions,"comparisons":12,"strict_matches":12-differences.len(),"known_919_differences":0})
    );
    assert!(differences.is_empty(), "{}", json!(differences));
    Ok(())
}

#[tokio::test]
async fn valid_immutable_callback_records_actual_sharing_and_exact_values() -> TestResult {
    for batch_size in [1, 32] {
        let session = session(batch_size);
        let calls = Arc::new(Mutex::new(Vec::<Vec<(i32, i32)>>::new()));
        let callback_calls = Arc::clone(&calls);
        session.register_udf(create_udf(
            "record_shared_value",
            vec![DataType::Int32, DataType::Int32],
            DataType::Int32,
            Volatility::Immutable,
            Arc::new(move |args| {
                let values = args[0].to_array(1)?;
                let keys = args[1].to_array(1)?;
                let values = values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| DataFusionError::Internal("expected Int32 values".into()))?;
                let keys = keys
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| DataFusionError::Internal("expected Int32 keys".into()))?;
                if values.null_count() != 0 || keys.null_count() != 0 || values.len() != keys.len()
                {
                    return Err(DataFusionError::Internal("invalid callback input".into()));
                }
                callback_calls
                    .lock()
                    .map_err(|_| DataFusionError::Internal("callback trace lock poisoned".into()))?
                    .push(
                        keys.values()
                            .iter()
                            .copied()
                            .zip(values.values().iter().copied())
                            .collect(),
                    );
                Ok(args[0].clone())
            }),
        ));
        let batch = RecordBatch::try_from_iter([
            (
                "k",
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            (
                "v",
                Arc::new(Int32Array::from(vec![10, 11, 12, 13])) as ArrayRef,
            ),
        ])?;
        register_one_partition(&session, "slice_valid_priority", batch, batch_size)?;
        let sql = "SELECT SLICE(ARRAY(record_shared_value(v, k)), 1, 1) AS r, ARRAY(record_shared_value(v, k)) AS repeated_array, k FROM slice_valid_priority ORDER BY k";
        let case = json!({"sql":sql,"ansi":true});
        let plans = plan_observation(&session, &case).await?;
        assert!(
            calls
                .lock()
                .map_err(|_| "callback trace lock poisoned")?
                .is_empty(),
            "planning must not invoke callback"
        );
        let query = plan_query(
            &session,
            sql,
            &SparkSqlOptions {
                ansi_mode: true,
                ..Default::default()
            },
        )
        .await?;
        let physical = query.create_physical_plan(&session).await?;
        let executed_plan = displayable(physical.as_ref()).indent(true).to_string();
        let declared_schema = physical.schema();
        let executed_bindings = projection_bindings(&physical, vec![])?;
        assert!(
            calls
                .lock()
                .map_err(|_| "callback trace lock poisoned")?
                .is_empty(),
            "all planning stays callback-free"
        );
        let mut rows = Vec::new();
        for batch in collect(physical, session.task_ctx()).await? {
            spark_results::validate_batch(declared_schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| spark_results::encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                );
            }
        }
        assert_eq!(
            json!(rows),
            json!([
                [["10"], ["10"], "0"],
                [["11"], ["11"], "1"],
                [["12"], ["12"], "2"],
                [["13"], ["13"], "3"]
            ])
        );
        let trace = calls
            .lock()
            .map_err(|_| "callback trace lock poisoned")?
            .clone();
        let expected_calls = if batch_size == 1 {
            vec![vec![(0, 10)], vec![(1, 11)], vec![(2, 12)], vec![(3, 13)]]
        } else {
            vec![vec![(0, 10), (1, 11), (2, 12), (3, 13)]]
        };
        assert_eq!(
            trace, expected_calls,
            "one evaluation per actual shared producer batch"
        );
        assert_eq!(plans["shared_computed_projection_edges"], 1);
        assert_eq!(plans["prefix_producers"], 1);
        assert_eq!(
            executed_bindings
                .iter()
                .filter(|binding| binding["prefix_transport"] == true)
                .count(),
            1
        );
        assert_eq!(
            spark_results::capture_schema(declared_schema.fields(), None)?,
            json!({
                "type":"struct", "fields":[
                    {"name":"r","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":false,"metadata":{}},
                    {"name":"repeated_array","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":false,"metadata":{}},
                    {"name":"k","type":"integer","nullable":false,"metadata":{}}
                ]
            })
        );
        eprintln!(
            "SLICE_920_IMMUTABLE_SHARING_CONTROL {}",
            json!({"batch":batch_size,
            "sql":sql,"rows":rows,"schema":spark_results::capture_schema(declared_schema.fields(),None)?,
            "calls":trace,"call_count":trace.len(),"plan_probe":plans,"executed_physical":executed_plan,
            "executed_projection_bindings":executed_bindings,
            "scope":"actual shared producer, exact immutable callback count/key batches, complete fields and rows"})
        );
    }
    Ok(())
}

#[tokio::test]
async fn captured_sort_boundary_allows_column_filter_and_limit_pruning() -> TestResult {
    let session = session(32);
    let batch = RecordBatch::try_from_iter([
        ("k", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
        (
            "array_text",
            Arc::new(StringArray::from(vec!["10", "11"])) as ArrayRef,
        ),
        (
            "unused",
            Arc::new(StringArray::from(vec!["unused wide value".repeat(1024); 2])) as ArrayRef,
        ),
    ])?;
    session.register_batch("wide_input", batch)?;
    let query = plan_query(
        &session,
        "SELECT SLICE(shared, 1, 1) AS r, shared AS repeated_array, k FROM \
         (SELECT k, ARRAY(CAST(array_text AS INT)) AS shared, unused FROM wide_input \
          WHERE k >= 0 ORDER BY k) named_input LIMIT 1",
        &SparkSqlOptions::default(),
    )
    .await?;
    let mut state = session.state();
    state
        .config_mut()
        .options_mut()
        .extensions
        .insert(SparkSqlExpressionSemantics::default());
    let optimized = state.optimize(&query.logical_plan)?;
    let mut boundaries = 0;
    let mut scans = 0;
    let mut bounded_sort = false;
    optimized.apply_with_subqueries(|plan| {
        if let LogicalPlan::Extension(extension) = plan
            && extension.node.name() == "InputSortRowErrorBoundary"
        {
            boundaries += 1;
            assert!(
                extension
                    .node
                    .schema()
                    .fields()
                    .iter()
                    .all(|field| field.name() != "unused")
            );
            assert_eq!(extension.node.schema(), extension.node.inputs()[0].schema());
        }
        if let LogicalPlan::TableScan(scan) = plan {
            scans += 1;
            assert_eq!(
                scan.projection,
                Some(vec![0, 1]),
                "unused wide column is pruned through the carrier"
            );
        }
        if let LogicalPlan::Sort(sort) = plan {
            bounded_sort |= sort.fetch == Some(1);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert_eq!(boundaries, 1);
    assert_eq!(scans, 1);
    assert!(
        bounded_sort,
        "limit still reaches the retained sort through provenance"
    );
    let physical = query.create_physical_plan(&session).await?;
    assert!(
        projection_bindings(&physical, vec![])?
            .iter()
            .all(|binding| binding["prefix_transport"] == false)
    );
    let result = collect(physical, session.task_ctx()).await?;
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let native = session
        .sql(
            "SELECT make_array(CAST(array_text AS INT)) AS a, \
        make_array(CAST(array_text AS INT)) AS b FROM wide_input",
        )
        .await?
        .create_physical_plan()
        .await?;
    let native_bindings = projection_bindings(&native, vec![])?;
    assert_eq!(
        native_bindings
            .iter()
            .map(|binding| {
                binding["shared_computed_producers"]
                    .as_array()
                    .map_or(0, Vec::len)
            })
            .sum::<usize>(),
        1,
        "the native control retains one actual shared computed producer"
    );
    assert!(
        native_bindings
            .iter()
            .all(|binding| binding["prefix_transport"] == false),
        "ordinary native shared projections do not opt in"
    );
    Ok(())
}
