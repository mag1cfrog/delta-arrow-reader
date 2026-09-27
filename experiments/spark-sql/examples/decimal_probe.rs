use std::{error::Error, fs, sync::Arc};

use arrow::{
    array::Array, datatypes::Schema, ipc::writer::StreamWriter,
    util::display::array_value_to_string,
};
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use sail_plan::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn schema_ipc(schema: &Schema) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    StreamWriter::try_new(&mut bytes, schema)?.finish()?;
    Ok(bytes)
}

async fn observe(case: &Value, include_physical_plan: bool) -> Result<Value> {
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(
                SessionConfig::new()
                    .with_batch_size(usize::try_from(case["batch_size"].as_u64().unwrap_or(1))?)
                    .with_target_partitions(2),
            )
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    let mut config = PlanConfig::default();
    config.ansi_mode = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
    config.session_timezone = "UTC".into();
    let planned: Result<_> = async {
        let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        Ok(PlanResolver::new(&ctx, Arc::new(config))
            .resolve_named_plan(spec)
            .await?)
    }
    .await;
    let named = match planned {
        Ok(named) => named,
        Err(e) => return Ok(json!({"status":"planning_error","error":e.to_string()})),
    };
    let logical_plan = named.plan.display_indent().to_string();
    // Preserve Arrow's complete schema separately from the public output names.
    let output_names = named.fields;
    let logical_schema_ipc = schema_ipc(named.plan.schema().as_arrow())?;
    let logical_nullable = named
        .plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.is_nullable())
        .collect::<Vec<_>>();
    let mut physical_plan = None;
    let mut physical_schema_ipc = None;
    let executed: datafusion::common::Result<_> = async {
        let frame = ctx.execute_logical_plan(named.plan).await?;
        let physical = frame.create_physical_plan().await?;
        if include_physical_plan {
            physical_plan = Some(
                datafusion::physical_plan::displayable(physical.as_ref())
                    .indent(true)
                    .to_string(),
            );
        }
        let schema = physical.schema();
        physical_schema_ipc = Some(
            schema_ipc(&schema).map_err(|e| datafusion::common::DataFusionError::External(e))?,
        );
        let batches = datafusion::physical_plan::collect(physical, ctx.task_ctx()).await?;
        Ok((schema, batches))
    }
    .await;
    let (schema, batches) = match executed {
        Ok(result) => result,
        Err(e) => {
            let mut actual = json!({"status":"execution_error","error":e.to_string(),"logical_plan":logical_plan,
                "output_names":output_names,"logical_schema_ipc":logical_schema_ipc,
                "physical_schema_ipc":physical_schema_ipc});
            if let Some(plan) = physical_plan {
                actual["physical_plan"] = json!(plan);
            }
            return Ok(actual);
        }
    };
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
    let mut actual = json!({"status":"ok","types":types,"rows":rows,"logical_plan":logical_plan,
        "output_names":output_names,"logical_schema_ipc":logical_schema_ipc,"physical_schema_ipc":physical_schema_ipc,
        "logical_nullable":logical_nullable,"physical_nullable":schema.fields().iter().map(|f| f.is_nullable()).collect::<Vec<_>>()});
    if let Some(plan) = physical_plan {
        actual["physical_plan"] = json!(plan);
    }
    Ok(actual)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let include_physical_plan = match args.get(3).map(String::as_str) {
        None => false,
        Some("--physical-plans") => true,
        Some(_) => return Err("expected --physical-plans or no extra argument".into()),
    };
    let cases = fs::read_to_string(args.get(1).ok_or("expected JSONL cases path")?)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut results = Vec::new();
    for case in &cases {
        let mut case = case.clone();
        for ansi in [true, false] {
            case["ansi"] = json!(ansi);
            let actual = observe(&case, include_physical_plan).await?;
            results.push(json!({"id":format!("{}_{}",case["id"].as_str().ok_or("missing ID")?,ansi),"actual":actual}));
        }
    }
    fs::write(
        args.get(2).ok_or("expected output path")?,
        serde_json::to_vec_pretty(&json!({"cases":cases,"results":results}))?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        datatypes::{DataType, Field},
        ipc::reader::StreamReader,
    };
    use std::{collections::HashMap, io::Cursor};

    #[tokio::test]
    async fn captures_complete_schemas_without_changing_output_names() -> Result<()> {
        let field = Field::new("internal", DataType::Decimal128(38, 18), true)
            .with_metadata(HashMap::from([("origin".into(), "fixture".into())]));
        let schema = Schema::new_with_metadata(
            vec![field],
            HashMap::from([("schema_origin".into(), "fixture".into())]),
        );
        let reader = StreamReader::try_new(Cursor::new(schema_ipc(&schema)?), None)?;
        assert_eq!(reader.schema().as_ref(), &schema);
        for sql in [
            "SELECT CAST(1 AS DECIMAL(10,2)) AS same, CAST(NULL AS INT) AS same",
            "SELECT CAST(id AS DECIMAL(10,2)) AS r FROM range(0)",
            "SELECT CAST('bad' AS INT) AS r FROM range(1)",
        ] {
            let actual = observe(&json!({"sql":sql,"ansi":true}), true).await?;
            assert!(actual["logical_schema_ipc"].is_array(), "{actual}");
            if actual["status"] == "ok" {
                assert!(actual["physical_schema_ipc"].is_array(), "{actual}");
                if sql.contains("AS same") {
                    assert_eq!(actual["output_names"], json!(["same", "same"]));
                } else {
                    assert_eq!(actual["rows"], json!([]));
                }
            } else {
                assert_eq!(actual["status"], "execution_error");
                assert!(actual["error"].as_str().unwrap().contains("bad"));
            }
        }
        Ok(())
    }
}
