//! Scratch-only registry and query probe over the CLI's native session.

use std::{error::Error, process::ExitCode, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, Int64Array, Int64Builder, MapBuilder, StringBuilder},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    execution::{runtime_env::RuntimeEnvBuilder, session_state::SessionStateBuilder},
    prelude::SessionContext,
};
use serde_json::{Value, json};

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) fn run() -> Option<ExitCode> {
    let mut args = std::env::args_os().skip(1);
    let action = args.next()?;
    if action != "--proof-native-registry" && action != "--proof-native-sql" {
        return None;
    }
    // Retain the Spark entry point in this scratch binary without calling it.
    std::hint::black_box(
        delta_arrow_reader_spark_sql::configure_spark_sql
            as fn(SessionStateBuilder) -> SessionStateBuilder,
    );
    std::panic::set_hook(Box::new(|info| {
        eprintln!(
            "{}",
            json!({
                "status": "panic",
                "message": info.payload_as_str(),
                "location": info.location().map(|loc| json!({
                    "file": loc.file(),
                    "line": loc.line(),
                    "column": loc.column(),
                })),
            })
        );
    }));
    let result = if action == "--proof-native-registry" {
        session().map(|ctx| registry(&ctx))
    } else {
        args.next()
            .and_then(|sql| sql.into_string().ok())
            .ok_or_else(|| "proof requires one SQL statement".into())
            .and_then(|sql| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(probe(&sql))
            })
    };
    match result {
        Ok(result) => {
            println!("{result}");
            Some(ExitCode::SUCCESS)
        }
        Err(error) => {
            println!(
                "{}",
                json!({"status": "error", "message": error.to_string()})
            );
            Some(ExitCode::FAILURE)
        }
    }
}

fn session() -> ProbeResult<SessionContext> {
    Ok(crate::query::native_session(
        RuntimeEnvBuilder::new().build_arc()?,
    ))
}

fn registry(context: &SessionContext) -> Value {
    let state = context.state();
    let mut result = Vec::new();
    let mut record = |group: &str, name: &str, canonical: &str, order: Option<usize>| {
        result.push(json!({
            "category": group,
            "name": name,
            "canonical_name": canonical,
            "order": order,
        }));
    };
    for (name, function) in state.scalar_functions() {
        record("scalar", name, function.name(), None);
    }
    for (name, function) in state.higher_order_functions() {
        record("higher_order", name, function.name(), None);
    }
    for (name, function) in state.aggregate_functions() {
        record("aggregate", name, function.name(), None);
    }
    for (name, function) in state.window_functions() {
        record("window", name, function.name(), None);
    }
    for (name, function) in state.table_functions() {
        record("table_function", name, function.name(), None);
    }
    for name in state.table_factories().keys() {
        record("table_provider_factory", name, name, None);
    }
    for (index, planner) in state.expr_planners().iter().enumerate() {
        let name = format!("{planner:?}");
        record("expression_planner", &name, &name, Some(index));
    }
    for (index, planner) in state.relation_planners().iter().enumerate() {
        let name = format!("{planner:?}");
        record("relation_planner", &name, &name, Some(index));
    }
    let mut builder = SessionStateBuilder::new_from_existing(state);
    for factory in builder.file_formats().iter().flatten() {
        let name = factory.get_ext();
        record("file_format", &name, &name, None);
    }
    if let Some(planner) = builder.type_planner() {
        let name = format!("{planner:?}");
        record("type_planner", &name, &name, None);
    }
    json!(result)
}

async fn probe(sql: &str) -> ProbeResult<Value> {
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    map.append(false)?;
    map.keys().append_value("a");
    map.values().append_value(4);
    map.keys().append_value("b");
    map.values().append_null();
    map.append(true)?;
    map.append(true)?;
    map.keys().append_value("c");
    map.values().append_value(9);
    map.append(true)?;
    let map = map.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("m", map.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![0, 1, 2, 3])), Arc::new(map)],
    )?;
    let context = session()?;
    context.register_table(
        "bench",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    let batches = context.sql(sql).await?.collect().await?;
    Ok(json!({
        "status": "rows",
        "row_count": batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
    }))
}
