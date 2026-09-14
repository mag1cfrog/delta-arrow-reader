use arrow::{ipc::writer::StreamWriter, util::display::array_value_to_string};
use datafusion::{
    execution::SessionStateBuilder,
    physical_plan::{ExecutionPlan, displayable, execute_stream},
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::{
    DeltaTableBuilder,
    datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics},
};
use futures_util::StreamExt;
use sail_catalog::{
    manager::{CatalogManager, CatalogManagerOptions},
    provider::CatalogProvider,
};
use sail_catalog_memory::MemoryCatalogProvider;
use sail_common::spec;
use sail_common_datafusion::{
    catalog::display::DefaultCatalogDisplay, rename::physical_plan::rename_physical_plan,
    session::plan::PlanService,
};
use sail_plan::{
    catalog::SparkCatalogObjectDisplay,
    config::PlanConfig,
    formatter::SparkPlanFormatter,
    resolver::{PlanResolver, plan::NamedPlan},
};
use serde_json::{Value, json};
use std::{collections::HashMap, error::Error, fs, path::Path, sync::Arc};

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn session() -> ProbeResult<SessionContext> {
    let mut state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(2))
        .with_default_features()
        .build();
    // Sail still consults this for unresolved UDF lookup. Tables use the native registry.
    let catalog = CatalogManager::try_new(CatalogManagerOptions {
        catalogs: HashMap::from([(
            "sail".to_string(),
            Arc::new(MemoryCatalogProvider::new(
                "sail".to_string(),
                vec![Arc::from("default")].try_into()?,
                None,
            )) as Arc<dyn CatalogProvider>,
        )]),
        default_catalog: "sail".into(),
        default_database: vec!["default".into()],
        global_temporary_database: vec!["global_temp".into()],
    })?;
    state.config_mut().set_extension(Arc::new(catalog));
    state.config_mut().set_extension(Arc::new(PlanService::new(
        Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
        Box::new(SparkPlanFormatter),
    )));
    let ctx = SessionContext::new_with_state(state);
    // Native DF also registers range; Spark's INT literals need Sail's implementation.
    let range = sail_plan::function::get_built_in_table_function("range")?;
    ctx.register_udtf("range", range.function().clone());
    Ok(ctx)
}

async fn resolve(ctx: &SessionContext, sql: &str, settings: &Value) -> ProbeResult<NamedPlan> {
    let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
    if !matches!(&spec, spec::Plan::Query(_)) {
        return Err("extraction probe accepts queries only".into());
    }
    let mut config = PlanConfig::new()?;
    config.ansi_mode = settings["spark.sql.ansi.enabled"] == "true";
    config.case_sensitive = settings["spark.sql.caseSensitive"] == "true";
    config.session_timezone = settings["spark.sql.session.timeZone"]
        .as_str()
        .ok_or("missing session timezone")?
        .into();
    Ok(PlanResolver::new(ctx, Arc::new(config))
        .resolve_named_plan(spec)
        .await?)
}

async fn write_stream(
    ctx: &SessionContext,
    physical: Arc<dyn ExecutionPlan>,
    path: &Path,
) -> ProbeResult<Value> {
    let plan_text = displayable(physical.as_ref()).indent(true).to_string();
    let metrics = collect_scan_metrics(physical.as_ref());
    let rows_before_execution: u64 = metrics
        .iter()
        .map(|m| m.snapshot().reader_metrics.scheduler_rows_emitted)
        .sum();
    assert_eq!(rows_before_execution, 0, "planning performed row reads");
    let schema = physical.schema();
    let arrow_types: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("{:?}", f.data_type()))
        .collect();
    let mut writer = StreamWriter::try_new(fs::File::create(path)?, schema.as_ref())?;
    let mut stream = execute_stream(physical, ctx.task_ctx())?;
    let mut batch_count = 0;
    while let Some(batch) = stream.next().await {
        writer.write(&batch?)?;
        batch_count += 1;
    }
    writer.finish()?;
    Ok(json!({"status":"ok", "arrow_types":arrow_types,
        "physical_plan":plan_text, "delta_scans":metrics.len(),
        "stream_batches":batch_count, "rows_before_execution":rows_before_execution,
        "files_planned":metrics.iter().map(|m| m.snapshot().reader_metrics.files_planned).sum::<u64>(),
        "files_excluded":metrics.iter().map(|m| m.snapshot().reader_metrics.add_actions_excluded_during_planning.unwrap_or(0)).sum::<u64>(),
        "scan_rows":metrics.iter().map(|m| m.snapshot().reader_metrics.scheduler_rows_emitted).sum::<u64>()}))
}

async fn execute(ctx: &SessionContext, named: NamedPlan, path: &Path) -> ProbeResult<Value> {
    let NamedPlan { plan, fields } = named;
    let logical = plan.display_indent().to_string();
    let frame = ctx.execute_logical_plan(plan).await?;
    let physical = frame.create_physical_plan().await?;
    let physical = if let Some(fields) = fields {
        rename_physical_plan(physical, &fields)?
    } else {
        physical
    };
    let mut result = write_stream(ctx, physical, path).await?;
    result["logical_plan"] = json!(logical);
    Ok(result)
}

async fn run() -> ProbeResult<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let output_arg = std::env::args()
        .nth(1)
        .ok_or("usage: delta-reader-sail-extraction-probe RUN_DIRECTORY")?;
    let output = Path::new(&output_arg);
    let inputs: Value = serde_json::from_str(&fs::read_to_string(root.join("inputs.json"))?)?;
    let cases: Vec<Value> = fs::read_to_string(root.join("queries.jsonl"))?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let ctx = session()?;
    let tables = inputs["tables"].as_object().ok_or("missing tables")?;
    for name in tables.keys() {
        let path = output.join("delta").join(name);
        let table = DeltaTableBuilder::new(path.to_str().ok_or("invalid path")?)
            .load_table()
            .await?;
        ctx.register_table(
            name.as_str(),
            Arc::new(DeltaTableProvider::try_new(table, ScanOptions::default())?),
        )?;
        let physical = ctx
            .table(name.as_str())
            .await?
            .create_physical_plan()
            .await?;
        write_stream(&ctx, physical, &output.join(format!("input-{name}.arrow"))).await?;
    }
    let native = ctx
        .sql("SELECT SUM(x) AS total FROM t")
        .await?
        .collect()
        .await?;
    assert_eq!(array_value_to_string(native[0].column(0).as_ref(), 0)?, "6");
    let mut results = Vec::new();
    for case in cases {
        let id = case["id"].as_str().ok_or("missing case ID")?;
        let mut settings = inputs["settings"].clone();
        if let Some(overrides) = case["settings"].as_object() {
            settings
                .as_object_mut()
                .ok_or("invalid settings")?
                .extend(overrides.clone());
        }
        let actual = match resolve(&ctx, case["sql"].as_str().ok_or("missing SQL")?, &settings)
            .await
        {
            Err(error) => {
                json!({"status":"planning_error", "condition":null, "error":error.to_string()})
            }
            Ok(named) if case["family"] == "excluded" => {
                // Do not execute a forbidden operation if a future resolver accidentally accepts it.
                json!({"status":"policy_failure", "error":"excluded SQL reached a logical plan",
                    "logical_plan":named.plan.display_indent().to_string()})
            }
            Ok(named) => match execute(&ctx, named, &output.join(format!("{id}.arrow"))).await {
                Ok(actual) => actual,
                Err(error) => {
                    json!({"status":"execution_error", "condition":null, "error":error.to_string()})
                }
            },
        };
        println!("{id}: {}", actual["status"]);
        results.push(json!({"id":id, "settings":settings, "actual":actual}));
        fs::write(
            output.join("rust-observations.json"),
            serde_json::to_string_pretty(&results)?,
        )?;
    }
    // Rejections must leave the registered table and its data usable.
    assert!(!ctx.table_exist("unwanted")?);
    let native = ctx
        .sql("SELECT SUM(x) AS total FROM t")
        .await?
        .collect()
        .await?;
    assert_eq!(array_value_to_string(native[0].column(0).as_ref(), 0)?, "6");
    Ok(())
}

fn main() -> ProbeResult<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(run())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, ListArray, TimestampMicrosecondArray};
    use sail_plan::error::PlanError;

    #[tokio::test]
    async fn native_sequence_and_timezone_paths_still_execute() -> ProbeResult<()> {
        let ctx = session()?;
        let settings = json!({"spark.sql.ansi.enabled":"true", "spark.sql.caseSensitive":"false", "spark.sql.session.timeZone":"UTC"});
        let named = resolve(&ctx, "SELECT SEQUENCE(1, 3), CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST('2024-01-01 08:00:00' AS TIMESTAMP_NTZ))", &settings).await?;
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[1, 2, 3]
        );
        let timestamp = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(timestamp.value(0), 1_704_067_200_000_000);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_python_entrypoints_before_payload_or_input_resolution() -> ProbeResult<()> {
        use spec::{CommonInlineUserDefinedFunction, QueryNode, QueryPlan};
        let ctx = session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let function = CommonInlineUserDefinedFunction {
            function_name: "python_fn".into(),
            deterministic: true,
            is_distinct: false,
            arguments: vec![],
            function: spec::FunctionDefinition::PythonUdf {
                output_type: spec::DataType::Int32,
                eval_type: spec::PySparkUdfType::Batched,
                command: vec![0xff],
                python_version: "not-a-python-version".into(),
                additional_includes: vec![],
            },
        };
        // If a removed path starts resolving its input, this missing table makes
        // the test fail with a different error before any row reads are possible.
        let missing = match sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement("SELECT * FROM missing_table")?,
        )? {
            spec::Plan::Query(query) => Box::new(query),
            _ => unreachable!(),
        };
        let expression = spec::Expr::CommonInlineUserDefinedFunction(function.clone());
        let nodes = vec![
            QueryNode::Project {
                input: None,
                expressions: vec![expression.clone()],
            },
            QueryNode::Project {
                input: None,
                expressions: vec![spec::Expr::Window {
                    window_function: Box::new(expression),
                    window: spec::Window::Unnamed {
                        cluster_by: vec![],
                        partition_by: vec![],
                        order_by: vec![],
                        frame: None,
                    },
                }],
            },
            QueryNode::MapPartitions {
                input: missing.clone(),
                function: function.clone(),
                is_barrier: false,
            },
            QueryNode::GroupMap(spec::GroupMap {
                input: missing.clone(),
                grouping_expressions: vec![],
                function: function.clone(),
                sorting_expressions: vec![],
                initial_input: None,
                initial_grouping_expressions: vec![],
                is_map_groups_with_state: None,
                output_mode: None,
                timeout_conf: None,
                state_schema: None,
                transform_with_state_info: None,
            }),
            QueryNode::CoGroupMap(spec::CoGroupMap {
                input: missing.clone(),
                input_grouping_expressions: vec![],
                other: missing.clone(),
                other_grouping_expressions: vec![],
                function: function.clone(),
                input_sorting_expressions: vec![],
                other_sorting_expressions: vec![],
            }),
            QueryNode::ApplyInPandasWithState(spec::ApplyInPandasWithState {
                input: missing,
                grouping_expressions: vec![],
                function,
                output_schema: spec::Schema {
                    fields: Default::default(),
                },
                state_schema: spec::Schema {
                    fields: Default::default(),
                },
                output_mode: "append".into(),
                timeout_conf: "NoTimeout".into(),
            }),
            QueryNode::CommonInlineUserDefinedTableFunction(
                spec::CommonInlineUserDefinedTableFunction {
                    function_name: "python_table_fn".into(),
                    deterministic: true,
                    arguments: vec![],
                    function: spec::TableFunctionDefinition::PythonUdtf {
                        return_type: None,
                        eval_type: spec::PySparkUdfType::Table,
                        command: vec![0xff],
                        python_version: "not-a-python-version".into(),
                    },
                },
            ),
        ];
        for node in nodes {
            let error = resolver
                .resolve_named_plan(spec::Plan::Query(QueryPlan::new(node)))
                .await
                .unwrap_err();
            assert!(matches!(error, PlanError::NotSupported(_)), "{error:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_named_arguments_instead_of_discarding_their_names() -> ProbeResult<()> {
        let ctx = session()?;
        let settings = json!({"spark.sql.ansi.enabled":"true", "spark.sql.caseSensitive":"false", "spark.sql.session.timeZone":"UTC"});
        for sql in ["SELECT ABS(value => -1)", "SELECT * FROM range(end => 3)"] {
            let error = resolve(&ctx, sql, &settings).await.unwrap_err();
            assert!(
                matches!(
                    error.downcast_ref::<PlanError>(),
                    Some(PlanError::NotSupported(_))
                ),
                "{sql}: {error}"
            );
        }
        Ok(())
    }
}
