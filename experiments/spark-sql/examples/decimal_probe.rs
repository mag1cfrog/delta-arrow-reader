use std::{error::Error, fs, sync::Arc};

use arrow::{array::Array, util::display::array_value_to_string};
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use sail_plan::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

async fn observe(case: &Value) -> Result<Value> {
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(
                SessionConfig::new()
                    .with_batch_size(1)
                    .with_target_partitions(2),
            )
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
    let mut config = PlanConfig::default();
    config.ansi_mode = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
    config.session_timezone = "UTC".into();
    let named = match PlanResolver::new(&ctx, Arc::new(config))
        .resolve_named_plan(spec)
        .await
    {
        Ok(named) => named,
        Err(e) => return Ok(json!({"status":"planning_error","error":e.to_string()})),
    };
    let logical_plan = named.plan.display_indent().to_string();
    let batches = async { ctx.execute_logical_plan(named.plan).await?.collect().await }.await;
    let batches = match batches {
        Ok(batches) => batches,
        Err(e) => {
            return Ok(
                json!({"status":"execution_error","error":e.to_string(),"logical_plan":logical_plan}),
            );
        }
    };
    let schema = batches.first().ok_or("missing output batch")?.schema();
    let types = schema
        .fields()
        .iter()
        .map(|f| format!("{:?}", f.data_type()))
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for batch in batches {
        for i in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| {
                        if c.data_type() == &arrow::datatypes::DataType::Null || c.is_null(i) {
                            Ok(Value::Null)
                        } else {
                            array_value_to_string(c.as_ref(), i).map(Value::String)
                        }
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(json!({"status":"ok","types":types,"rows":rows,"logical_plan":logical_plan}))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let cases = fs::read_to_string(args.get(1).ok_or("expected JSONL cases path")?)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut results = Vec::new();
    for case in &cases {
        let mut case = case.clone();
        for ansi in [true, false] {
            case["ansi"] = json!(ansi);
            let actual = observe(&case).await?;
            results.push(json!({"id":format!("{}_{}",case["id"].as_str().ok_or("missing ID")?,ansi),"actual":actual}));
        }
    }
    fs::write(
        args.get(2).ok_or("expected output path")?,
        serde_json::to_vec_pretty(&json!({"cases":cases,"results":results}))?,
    )?;
    Ok(())
}
