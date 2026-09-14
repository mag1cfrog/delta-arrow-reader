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
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_plan::{
    config::PlanConfig,
    resolver::{PlanResolver, plan::NamedPlan},
};
use serde_json::{Value, json};
use std::{error::Error, fs, path::Path, sync::Arc};

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn session() -> ProbeResult<SessionContext> {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(2))
        .with_default_features()
        .build();
    Ok(SessionContext::new_with_state(state))
}

async fn resolve(ctx: &SessionContext, sql: &str, settings: &Value) -> ProbeResult<NamedPlan> {
    let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
    let mut config = PlanConfig::default();
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
    let physical = rename_physical_plan(physical, &fields)?;
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
    use sail_common::spec;
    use sail_plan::error::PlanError;

    #[tokio::test]
    async fn hex_sql_uses_the_retained_implementations() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        let settings = json!({"spark.sql.ansi.enabled":"true", "spark.sql.caseSensitive":"false", "spark.sql.session.timeZone":"UTC"});
        let named = resolve(
            &ctx,
            "SELECT hex(255), hex('Spark'), hex(unhex('f')), unhex('bad!') IS NULL",
            &settings,
        )
        .await?;
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
            .columns()
            .iter()
            .map(|column| array_value_to_string(column.as_ref(), 0))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(values, ["FF", "537061726B", "0F", "true"]);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_dataframe_expressions_while_sql_fields_remain() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        let settings = json!({"spark.sql.ansi.enabled":"true", "spark.sql.caseSensitive":"false", "spark.sql.session.timeZone":"UTC"});
        for (sql, expected) in [
            (
                "SELECT a, r.b FROM VALUES (1,2) AS l(a,b) JOIN VALUES (1,9) AS r(a,b) USING (a)",
                vec!["1", "9"],
            ),
            (
                "SELECT l.a, r.b FROM VALUES (1,2) AS l(a,b) JOIN VALUES (1,9) AS r(a,b) ON l.a=r.a",
                vec!["1", "9"],
            ),
            (
                "SELECT s.*, s.a, arr[0], m['key'] FROM (SELECT named_struct('a',7,'b',9) AS s, array(3,4) AS arr, map('key',5) AS m)",
                vec!["7", "9", "7", "3", "5"],
            ),
        ] {
            let named = resolve(&ctx, sql, &settings).await?;
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
                .columns()
                .iter()
                .map(|column| array_value_to_string(column.as_ref(), 0))
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(values, expected, "{sql}");
        }
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        let missing = spec::Expr::UnresolvedAttribute {
            name: spec::ObjectName::bare("missing"),
            plan_id: None,
            is_metadata_column: false,
        };
        let expressions = vec![
            spec::Expr::UnresolvedAttribute {
                name: spec::ObjectName::bare("missing"),
                plan_id: Some(42),
                is_metadata_column: false,
            },
            spec::Expr::UnresolvedStar {
                target: None,
                plan_id: Some(42),
                wildcard_options: Default::default(),
            },
            spec::Expr::UnresolvedRegex {
                col_name: ".*".into(),
                plan_id: None,
            },
            spec::Expr::UnresolvedRegex {
                col_name: "[invalid".into(),
                plan_id: Some(42),
            },
            spec::Expr::UpdateFields {
                struct_expression: Box::new(missing.clone()),
                field_name: spec::ObjectName::bare("a"),
                value_expression: None,
            },
            spec::Expr::UpdateFields {
                struct_expression: Box::new(missing.clone()),
                field_name: spec::ObjectName::bare("a"),
                value_expression: Some(Box::new(missing)),
            },
        ];
        for expr in expressions {
            let query = spec::QueryPlan::new(spec::QueryNode::Project {
                input: None,
                expressions: vec![expr],
            });
            let error = resolver.resolve_named_plan(query).await.unwrap_err();
            assert!(matches!(error, PlanError::NotSupported(_)), "{error}");
        }
        let mut query = spec::QueryPlan::new(spec::QueryNode::Empty {
            produce_one_row: true,
        });
        query.plan_id = Some(42);
        let error = resolver.resolve_named_plan(query).await.unwrap_err();
        assert!(matches!(error, PlanError::NotSupported(_)), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn rejects_connect_scopes_while_sql_subqueries_remain() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        let settings = json!({"spark.sql.ansi.enabled":"true", "spark.sql.caseSensitive":"false", "spark.sql.session.timeZone":"UTC"});
        for (sql, expected) in [
            (
                "WITH source AS (SELECT * FROM VALUES (1,2),(3,4) AS v(a,b)) SELECT
                (SELECT MAX(a) FROM source)",
                vec!["3"],
            ),
            (
                "WITH source AS (SELECT * FROM VALUES (1,2),(3,4) AS v(a,b)) SELECT MAX(a)
                FROM source WHERE EXISTS(SELECT * FROM source WHERE a=3)",
                vec!["3"],
            ),
            (
                "WITH source AS (SELECT * FROM VALUES (1,2),(3,4) AS v(a,b)) SELECT a,b
                FROM source WHERE (a,b) IN (SELECT a,b FROM source WHERE a=1)",
                vec!["1", "2"],
            ),
            (
                "WITH source AS (SELECT * FROM VALUES (1,2),(3,4) AS v(a,b)) SELECT a,b
                FROM source WHERE (a,b) NOT IN (SELECT a,b FROM source WHERE a=1)",
                vec!["3", "4"],
            ),
            ("SELECT IDENTIFIER('a') FROM VALUES (7) AS v(a)", vec!["7"]),
        ] {
            let named = resolve(&ctx, sql, &settings).await?;
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
                .columns()
                .iter()
                .map(|column| array_value_to_string(column.as_ref(), 0))
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(values, expected, "{sql}");
        }
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        let missing = sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement("SELECT * FROM missing_table")?,
        )?;
        let mut nodes = vec![
            spec::QueryNode::WithParameters {
                input: Box::new(missing.clone()),
                positional_arguments: vec![],
                named_arguments: vec![],
            },
            spec::QueryNode::WithRelations {
                root: Box::new(missing.clone()),
                references: vec![],
            },
            spec::QueryNode::SubqueryAlias {
                input: Box::new(missing),
                alias: "alias".into(),
                qualifier: vec![],
            },
        ];
        for subquery_type in [
            spec::SubqueryType::In,
            spec::SubqueryType::Scalar,
            spec::SubqueryType::Exists,
        ] {
            for negated in [false, true] {
                nodes.push(spec::QueryNode::Project {
                    input: None,
                    expressions: vec![spec::Expr::Subquery {
                        plan_id: 42,
                        subquery_type: subquery_type.clone(),
                        in_subquery_values: vec![],
                        negated,
                    }],
                });
            }
        }
        for node in nodes {
            let error = resolver
                .resolve_named_plan(spec::QueryPlan::new(node))
                .await
                .unwrap_err();
            assert!(matches!(error, PlanError::NotSupported(_)), "{error}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn variant_sql_survives_storage_removal() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        let settings = json!({"spark.sql.ansi.enabled":"true", "spark.sql.caseSensitive":"false", "spark.sql.session.timeZone":"UTC"});
        let named = resolve(
            &ctx,
            r#"SELECT
            variant_get(parse_json('{"a":7}'), '$.a', 'int'),
            is_variant_null(parse_json('null')),
            variant_to_json(to_variant_object(named_struct('a', 7))),
            try_parse_json('{broken') IS NULL,
            CAST(parse_json('{"a":[1,null]}') AS STRING)"#,
            &settings,
        )
        .await?;
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
            .columns()
            .iter()
            .map(|column| array_value_to_string(column.as_ref(), 0))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            values,
            ["7", "true", r#"{"a":7}"#, "true", r#"{"a":[1,null]}"#]
        );
        Ok(())
    }

    #[tokio::test]
    async fn rejects_dataframe_transforms_while_sql_paths_remain() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        let input = serde_json::to_value(spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                name: spec::ObjectName::bare("missing_table"),
                temporal: None,
                sample: None,
                options: vec![],
            })),
            is_streaming: false,
        }))?;
        for (name, mut fields) in [
            ("toDf", json!({"columnNames": []})),
            ("toSchema", json!({"schema": {"fields": []}})),
            ("withColumnsRenamed", json!({"renameColumnsMap": []})),
            ("drop", json!({"columns": [], "columnNames": []})),
            ("withColumns", json!({"aliases": []})),
            ("tail", json!({"limit": {"literal": "null"}})),
            ("hint", json!({"name": "COALESCE", "parameters": []})),
            ("repartition", json!({"numPartitions": 0, "shuffle": false})),
            (
                "repartitionByExpression",
                json!({"partitionExpressions": [], "numPartitions": null}),
            ),
            (
                "sample",
                json!({"lowerBound": 0.0, "upperBound": 1.0, "withReplacement": false, "seed": null, "deterministicOrder": false}),
            ),
            ("collectMetrics", json!({"name": "unused", "metrics": []})),
            (
                "parse",
                json!({"format": "csv", "schema": null, "options": []}),
            ),
            (
                "pivot",
                json!({"grouping": null, "aggregate": [], "columns": [], "values": []}),
            ),
        ] {
            fields["input"] = input.clone();
            let node = serde_json::from_value(json!({(name): fields}))?;
            let error = resolver
                .resolve_named_plan(spec::QueryPlan::new(node))
                .await
                .unwrap_err();
            assert!(
                matches!(error, PlanError::NotSupported(ref message)
                if message.contains("DataFrame")),
                "{name}: {error}"
            );
        }
        let range = spec::QueryNode::Range(spec::Range {
            start: None,
            end: 3,
            step: 1,
            num_partitions: None,
        });
        assert!(matches!(
            resolver
                .resolve_named_plan(spec::QueryPlan::new(range))
                .await
                .unwrap_err(),
            PlanError::NotSupported(_)
        ));

        let settings =
            json!({"spark.sql.ansi.enabled": "true", "spark.sql.session.timeZone": "UTC"});
        for (sql, expected) in [
            (
                "WITH q(a,b) AS (SELECT 1,2) SELECT a AS b FROM q ORDER BY b LIMIT 1",
                vec![vec!["1"]],
            ),
            (
                "SELECT * FROM (SELECT * FROM VALUES (1,10), (2,20) AS t(k,v)) PIVOT (SUM(v) FOR (k) IN (1 AS one, 2 AS two))",
                vec![vec!["10", "20"]],
            ),
            (
                "SELECT * FROM (SELECT * FROM range(3)) TABLESAMPLE (100 PERCENT) ORDER BY id",
                vec![vec!["0"], vec!["1"], vec!["2"]],
            ),
            (
                "SELECT * FROM (SELECT * FROM range(3)) TABLESAMPLE (0 PERCENT)",
                vec![],
            ),
        ] {
            let named = resolve(&ctx, sql, &settings).await?;
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let rows = batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows()).map(|row| {
                        batch
                            .columns()
                            .iter()
                            .map(|array| array_value_to_string(array.as_ref(), row))
                            .collect::<Result<Vec<_>, _>>()
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(rows, expected, "{sql}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_inline_arrow_payloads_before_decoding() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        for data in [None, Some(vec![]), Some(vec![0xff, 0x00, 0x01])] {
            let plan = spec::QueryPlan::new(spec::QueryNode::LocalRelation { data, schema: None });
            let error = resolver.resolve_named_plan(plan).await.unwrap_err();
            assert!(
                matches!(error, PlanError::NotSupported(ref message)
                if message.contains("inline Arrow input")),
                "{error}"
            );
        }
        Ok(())
    }

    #[test]
    fn rejects_commands_during_analysis() -> ProbeResult<()> {
        use sail_sql_analyzer::error::SqlError;
        for sql in [
            "CREATE DATABASE unwanted",
            "CREATE TABLE unwanted (x INT)",
            "CREATE TABLE unwanted AS SELECT * FROM missing_table",
            "CREATE VIEW unwanted AS SELECT * FROM missing_table",
            "ALTER TABLE missing_table ADD COLUMNS (x INT)",
            "DROP TABLE missing_table",
            "INSERT INTO missing_table VALUES (1)",
            "INSERT OVERWRITE DIRECTORY '/nonexistent/output' USING parquet SELECT 1",
            "UPDATE missing_table SET x = 1",
            "DELETE FROM missing_table",
            "MERGE INTO missing_table t USING missing_source s ON t.x = s.x WHEN MATCHED THEN DELETE",
            "LOAD DATA INPATH '/nonexistent/input' INTO TABLE missing_table",
            "CACHE TABLE missing_table",
            "UNCACHE TABLE missing_table",
            "CLEAR CACHE",
            "REFRESH TABLE missing_table",
            "ANALYZE TABLE missing_table COMPUTE STATISTICS",
            "SHOW TABLES",
            "DESCRIBE TABLE missing_table",
            "EXPLAIN SELECT * FROM missing_table",
            "USE DATABASE unwanted",
            "SET spark.sql.ansi.enabled = false",
            "COMMENT ON TABLE missing_table IS 'unused'",
        ] {
            let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
            let error = sail_sql_analyzer::statement::from_ast_statement(ast).unwrap_err();
            assert!(
                matches!(error, SqlError::NotSupported(ref message)
                if message == "extraction probe accepts queries only"),
                "{sql}: {error}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_dataframe_statistics_before_resolving_inputs() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        let input = serde_json::to_value(spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                name: spec::ObjectName::bare("missing_table"),
                temporal: None,
                sample: None,
                options: vec![],
            })),
            is_streaming: false,
        }))?;
        for (name, mut fields) in [
            ("fillNa", json!({"columns": [], "values": []})),
            ("dropNa", json!({"columns": [], "minNonNulls": null})),
            ("replace", json!({"columns": [], "replacements": []})),
            ("statSummary", json!({"statistics": []})),
            ("statDescribe", json!({"columns": []})),
            (
                "statCrosstab",
                json!({"leftColumn": "x", "rightColumn": "y"}),
            ),
            ("statCov", json!({"leftColumn": "x", "rightColumn": "y"})),
            (
                "statCorr",
                json!({"leftColumn": "x", "rightColumn": "y", "method": "pearson"}),
            ),
            (
                "statApproxQuantile",
                json!({"columns": [], "probabilities": [], "relativeError": 0.0}),
            ),
            ("statFreqItems", json!({"columns": [], "support": null})),
            (
                "statSampleBy",
                json!({"column": {"literal": "null"}, "fractions": [], "seed": null}),
            ),
        ] {
            fields["input"] = input.clone();
            let node = serde_json::from_value(json!({(name): fields}))?;
            let error = resolver
                .resolve_named_plan(spec::QueryPlan::new(node))
                .await
                .unwrap_err();
            assert!(
                matches!(error, PlanError::NotSupported(ref message)
                if message.contains("DataFrame NA/statistics")),
                "{name}: {error}"
            );
        }
        let settings =
            json!({"spark.sql.ansi.enabled": "true", "spark.sql.session.timeZone": "UTC"});
        let named = resolve(&ctx, "SELECT COUNT(x), AVG(x), COVAR_SAMP(x,y), CORR(x,y), SUM(COALESCE(x,0)) FROM VALUES (1,2), (2,4), (NULL,6) AS t(x,y)", &settings).await?;
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        let row = batches[0]
            .columns()
            .iter()
            .map(|array| array_value_to_string(array.as_ref(), 0))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(row, ["2", "1.5", "1.0", "1.0", "3"]);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_streaming_and_checkpoints_before_resolving_inputs() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        ctx.register_table("registered", ctx.sql("SELECT 1 AS x").await?.into_view())?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        let named = spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: spec::ObjectName::bare("registered"),
            temporal: None,
            sample: None,
            options: vec![],
        }));
        let batch = spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: named.clone(),
            is_streaming: false,
        });
        for read_type in [
            named,
            spec::ReadType::Udtf(Box::new(spec::ReadUdtf {
                name: spec::ObjectName::bare("range"),
                arguments: vec![],
                named_arguments: vec![],
                options: vec![],
            })),
            spec::ReadType::DynamicTable(Box::new(spec::ReadDynamicTable {
                name: spec::Expr::Literal(spec::Literal::Null),
                sample: None,
                options: vec![],
            })),
            spec::ReadType::DataSource(Box::new(spec::ReadDataSource {
                format: None,
                schema: None,
                options: vec![],
                paths: vec![],
                predicates: vec![],
            })),
        ] {
            let plan = spec::QueryPlan::new(spec::QueryNode::Read {
                read_type,
                is_streaming: true,
            });
            let error = resolver.resolve_named_plan(plan).await.unwrap_err();
            assert!(
                matches!(error, PlanError::NotSupported(ref message)
                if message.contains("streaming reads")),
                "{error}"
            );
        }
        for node in [
            spec::QueryNode::WithWatermark(spec::WithWatermark {
                input: Box::new(batch.clone()),
                event_time: "missing_column".into(),
                delay_threshold: "invalid".into(),
            }),
            spec::QueryNode::CachedRemoteRelation {
                relation_id: "missing_checkpoint".into(),
            },
        ] {
            let error = resolver
                .resolve_named_plan(spec::QueryPlan::new(node))
                .await
                .unwrap_err();
            assert!(matches!(error, PlanError::NotSupported(_)), "{error}");
        }
        let named = resolver.resolve_named_plan(batch).await?;
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(
            array_value_to_string(batches[0].column(0).as_ref(), 0)?,
            "1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rejects_external_sources_before_format_or_predicate_resolution() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        for format in [
            Some("parquet"),
            Some("delta"),
            Some("iceberg"),
            Some("unknown"),
            None,
        ] {
            for predicates in [vec![], vec![spec::Expr::Literal(spec::Literal::Null)]] {
                let source = spec::ReadDataSource {
                    format: format.map(str::to_string),
                    schema: None,
                    options: vec![("path".into(), "/nonexistent/spark-source".into())],
                    paths: vec!["/nonexistent/spark-source".into()],
                    predicates,
                };
                let plan = spec::QueryPlan::new(spec::QueryNode::Read {
                    read_type: spec::ReadType::DataSource(Box::new(source)),
                    is_streaming: false,
                });
                let error = resolver.resolve_named_plan(plan).await.unwrap_err();
                assert!(
                    matches!(error, PlanError::NotSupported(ref message)
                    if message.contains("external data sources")),
                    "{format:?}: {error}"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_writes_and_snapshot_modifiers_before_table_lookup() -> ProbeResult<()> {
        let ctx = SessionContext::new();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        for sql in [
            "CREATE TABLE unwanted (x INT)",
            "INSERT INTO missing_table VALUES (1)",
            "UPDATE missing_table SET x = 1",
            "DELETE FROM missing_table WHERE x = 1",
            "MERGE INTO missing_table t USING missing_source s ON t.x = s.x WHEN MATCHED THEN UPDATE SET x = s.x",
            "SELECT * FROM missing_table VERSION AS OF 0",
            "SELECT * FROM missing_table TIMESTAMP AS OF '2024-01-01'",
        ] {
            let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
            let error = match sail_sql_analyzer::statement::from_ast_statement(ast) {
                Ok(plan) => resolver.resolve_named_plan(plan).await.unwrap_err(),
                Err(error) => PlanError::from(error),
            };
            assert!(
                matches!(error, PlanError::NotSupported(_)),
                "{sql}: {error}"
            );
        }
        assert!(!ctx.table_exist("unwanted")?);
        Ok(())
    }

    #[tokio::test]
    async fn native_session_catalogs_and_views_need_no_sail_extensions() -> ProbeResult<()> {
        let settings = json!({"spark.sql.ansi.enabled":"true", "spark.sql.caseSensitive":"false", "spark.sql.session.timeZone":"UTC"});
        for (catalog, schema) in [("datafusion", "public"), ("warehouse", "schema.with`quote")] {
            let ctx = SessionContext::new_with_config(
                SessionConfig::new().with_default_catalog_and_schema(catalog, schema),
            );
            let view = ctx.sql("SELECT CAST(42 AS INT) AS x").await?.into_view();
            ctx.register_table("v", view)?;
            let named = resolve(&ctx,
                "SELECT x, current_catalog(), current_database(), current_schema(), typeof(x) FROM v",
                &settings).await?;
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let row = (0..5)
                .map(|i| array_value_to_string(batches[0].column(i).as_ref(), 0))
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(row, ["42", catalog, schema, schema, "int"]);
            assert!(
                resolve(&ctx, "SELECT * FROM missing_table", &settings)
                    .await
                    .is_err()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn native_functions_preserve_spark_precedence_and_formatting() -> ProbeResult<()> {
        use arrow::datatypes::DataType;
        use datafusion::logical_expr::{Volatility, create_udf};

        let ctx = SessionContext::new();
        for name in ["probe_native", "abs", "from_avro"] {
            ctx.register_udf(create_udf(
                name,
                vec![DataType::Int32],
                DataType::Int32,
                Volatility::Immutable,
                Arc::new(|args| Ok(args[0].clone())),
            ));
        }
        let range = sail_plan::function::get_built_in_table_function("range")?;
        ctx.register_udtf("probe_range", range.function().clone());
        let settings = json!({"spark.sql.ansi.enabled":"true", "spark.sql.caseSensitive":"false", "spark.sql.session.timeZone":"UTC"});
        let named = resolve(
            &ctx,
            "SELECT probe_native(-7), ABS(-7), 1, CAST(1 AS BIGINT), typeof(1)",
            &settings,
        )
        .await?;
        assert_eq!(
            named.fields,
            [
                "probe_native((- 7))",
                "ABS((- 7))",
                "1",
                "CAST(1 AS BIGINT)",
                "typeof(1)"
            ]
        );
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        let row = (0..5)
            .map(|i| array_value_to_string(batches[0].column(i).as_ref(), 0))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(row, ["-7", "7", "1", "1", "int"]);
        assert_eq!(batches[0].schema().field(2).data_type(), &DataType::Int32);
        assert_eq!(batches[0].schema().field(3).data_type(), &DataType::Int64);
        for name in ["range", "probe_range"] {
            let named = resolve(
                &ctx,
                &format!("SELECT * FROM {name}(3) ORDER BY id"),
                &settings,
            )
            .await?;
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let values = batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows())
                        .map(|row| array_value_to_string(batch.column(0).as_ref(), row))
                })
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(values, ["0", "1", "2"]);
        }
        for sql in [
            "SELECT missing_function(1)",
            "SELECT from_avro(1)",
            "SELECT probe_native(DISTINCT 1)",
            "SELECT probe_native(1) FILTER (WHERE TRUE)",
            "SELECT probe_native(1) IGNORE NULLS",
            "SELECT * FROM missing_table_function(1)",
        ] {
            assert!(resolve(&ctx, sql, &settings).await.is_err(), "{sql}");
        }
        // Spark resolution must not change the host's native registry.
        let native = ctx
            .sql("SELECT abs(CAST(-7 AS INT))")
            .await?
            .collect()
            .await?;
        assert_eq!(
            array_value_to_string(native[0].column(0).as_ref(), 0)?,
            "-7"
        );
        Ok(())
    }

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
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
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
        let missing = Box::new(sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement("SELECT * FROM missing_table")?,
        )?);
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
                .resolve_named_plan(QueryPlan::new(node))
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
