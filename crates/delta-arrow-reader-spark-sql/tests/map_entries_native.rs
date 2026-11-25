//! Native MAP_ENTRIES accepts Arrow map field names and Spark keeps its executed type.

#[path = "support/spark_results.rs"]
mod spark_results;

use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array, Int64Builder, MapBuilder, MapFieldNames, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::MemTable,
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde::Deserialize;
use serde_json::{Value, json};
use spark_results::{TestResult, capture_executed_query_result, capture_native_query_observation};

#[derive(Deserialize)]
struct MapEntry(String, Option<i64>);

#[derive(Deserialize)]
struct InputRow(i64, Option<Vec<MapEntry>>);

#[derive(Deserialize)]
struct Observation {
    ansi: bool,
    executed_schema: Value,
    rows: Value,
}

#[derive(Deserialize)]
struct Reference {
    spark_version: String,
    spark_build: String,
    sql: String,
    inputs: Vec<InputRow>,
    observations: [Observation; 2],
}

fn reference() -> TestResult<Reference> {
    let reference: Reference = serde_json::from_str(include_str!(
        "fixtures/map-entries-native/spark-reference.json"
    ))?;
    assert_eq!(reference.spark_version, "4.2.0");
    assert_eq!(
        reference.spark_build,
        "4.2.0 32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert!(!reference.observations[0].ansi && reference.observations[1].ansi);
    Ok(reference)
}

fn spark_map_fields() -> MapFieldNames {
    MapFieldNames {
        key: "key".into(),
        value: "value".into(),
        ..MapFieldNames::default()
    }
}

fn register_maps(
    session: &SessionContext,
    inputs: &[InputRow],
    fields: Option<MapFieldNames>,
) -> TestResult {
    let mut map = MapBuilder::new(fields, StringBuilder::new(), Int64Builder::new());
    for InputRow(_, entries) in inputs {
        if let Some(entries) = entries {
            for MapEntry(key, value) in entries {
                map.keys().append_value(key);
                map.values().append_option(*value);
            }
        }
        map.append(entries.is_some())?;
    }
    let map = map.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("m", map.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(inputs.iter().map(|row| row.0))),
            Arc::new(map),
        ],
    )?;
    session.register_table(
        "bench",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    Ok(())
}

#[tokio::test]
async fn native_map_entries_accepts_both_entry_field_layouts() -> TestResult {
    let reference = reference()?;
    let expected = &reference.observations[0];
    for fields in [None, Some(spark_map_fields())] {
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        register_maps(&session, &reference.inputs, fields)?;
        let actual = capture_native_query_observation(&session, &reference.sql).await?;
        assert!(actual["error"].is_null(), "{actual}");
        assert_eq!(actual["rows"], expected.rows);
        assert_eq!(actual["physical_schema"], expected.executed_schema);
    }
    Ok(())
}

#[tokio::test]
async fn spark_map_entries_matches_values_and_types_in_both_ansi_modes() -> TestResult {
    let reference = reference()?;
    let state = configure_spark_sql(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(1)),
    )
    .build();
    let session = SessionContext::new_with_state(state);
    register_maps(&session, &reference.inputs, Some(spark_map_fields()))?;
    for observed in &reference.observations {
        let case = json!({"sql": reference.sql, "ansi": observed.ansi, "ordered": true});
        let actual = capture_executed_query_result(&session, &case).await?;
        assert_eq!(actual["rows"], observed.rows, "ANSI={}", observed.ansi);
        assert_eq!(
            actual["schema"], observed.executed_schema,
            "ANSI={}",
            observed.ansi
        );
    }
    Ok(())
}
