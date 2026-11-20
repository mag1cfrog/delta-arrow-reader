//! The #1391 native map_entries statement in the stock root workspace.

use std::{error::Error, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, Int64Array, Int64Builder, MapBuilder, StringBuilder},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    prelude::SessionContext,
};

#[tokio::test]
async fn native_map_entries() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    map.append(false)?;
    map.keys().append_value("a");
    map.values().append_value(4);
    map.keys().append_value("b");
    map.values().append_null();
    map.append(true)?;
    let map = map.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("m", map.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![0, 1])), Arc::new(map)],
    )?;
    let context = SessionContext::new();
    context.register_table(
        "bench",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    let query = context
        .sql("SELECT id,map_entries(m) AS value FROM bench")
        .await?;
    println!("{:?}", query.collect().await?);
    Ok(())
}
