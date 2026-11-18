//! Spark metadata values and types over registered provider schemas.

use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    catalog::{MemoryCatalogProvider, MemorySchemaProvider, Session},
    common::TableReference,
    datasource::{MemTable, TableProvider, TableType},
    execution::SessionStateBuilder,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::{TestResult, capture_executed_query_result, remove_nullability_and_metadata};

#[derive(Debug)]
struct ProviderKind {
    input: Arc<dyn TableProvider>,
    kind: TableType,
}

#[async_trait]
impl TableProvider for ProviderKind {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }
    fn table_type(&self) -> TableType {
        self.kind
    }
    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.input.scan(session, projection, filters, limit).await
    }
}

fn request() -> TestResult<Value> {
    Ok(serde_json::from_str(include_str!(
        "fixtures/read-only-metadata/requests.json"
    ))?)
}

fn context(catalog: &str, schema: &str, create_default: bool) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(1)
                        .with_default_catalog_and_schema(catalog, schema)
                        .with_create_default_catalog_and_schema(create_default),
                ),
        )
        .build(),
    )
}

fn register_sources(request: &Value) -> TestResult<SessionContext> {
    let metadata = &request["corpora"][0]["metadata"];
    let catalog_name = metadata["catalog"].as_str().ok_or("missing catalog")?;
    let default_schema = metadata["default_schema"]
        .as_str()
        .ok_or("missing default schema")?;
    let session = context(catalog_name, default_schema, true);
    let catalog = session.catalog(catalog_name).ok_or("missing catalog")?;
    for name in metadata["schemas"].as_array().ok_or("missing schemas")? {
        let name = name.as_str().ok_or("invalid schema")?;
        if catalog.schema(name).is_none() {
            catalog.register_schema(name, Arc::new(MemorySchemaProvider::new()))?;
        }
    }
    let input = spark_inputs::decode_recorded_batch(&metadata["inputs"][0])?;
    let rich: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(input.schema(), vec![vec![input]])?);
    let basic_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let basic: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
        basic_schema.clone(),
        vec![vec![RecordBatch::new_empty(basic_schema)]],
    )?);
    for table in metadata["tables"].as_array().ok_or("missing tables")? {
        let schema = table["schema"].as_str().ok_or("missing table schema")?;
        let name = table["name"].as_str().ok_or("missing table name")?;
        let kind = match table["kind"].as_str() {
            Some("base") => TableType::Base,
            Some("view") => TableType::View,
            Some("temporary") => TableType::Temporary,
            _ => return Err("invalid table kind".into()),
        };
        session.register_table(
            TableReference::full(catalog_name, schema, name),
            Arc::new(ProviderKind {
                input: if table["input"] == "rich_source" {
                    rich.clone()
                } else {
                    basic.clone()
                },
                kind,
            }),
        )?;
    }
    Ok(session)
}

#[tokio::test]
async fn describe_show_and_explain_match_spark_values_and_executed_types() -> TestResult {
    let request = request()?;
    let session = register_sources(&request)?;
    let records = include_str!("fixtures/read-only-metadata/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    let cases = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    assert_eq!(records.len() - 1, cases.len());
    assert_eq!(
        records.len() - 1,
        request["expected_record_count"]
            .as_u64()
            .ok_or("missing count")? as usize
    );
    for (record, requested) in records[1..].iter().zip(cases) {
        assert_eq!(record["id"], requested["id"]);
        assert_eq!(record["sql"], requested["sql"]);
        assert_eq!(record["ansi"], requested["ansi"]);
        assert_eq!(record["ordered"], requested["ordered"]);
        assert_eq!(record["status"], "ok");
        let actual = capture_executed_query_result(&session, record).await?;
        let mut actual_schema = actual["schema"].clone();
        let mut expected = record["schema_stages"]["executed"].clone();
        remove_nullability_and_metadata(&mut actual_schema);
        remove_nullability_and_metadata(&mut expected);
        assert_eq!(actual_schema, expected, "{}", record["id"]);
        if record["explain"] == true {
            let rows = actual["rows"].as_array().ok_or("missing rows")?;
            assert_eq!(rows.len(), 1);
            let text = rows[0][0].as_str().ok_or("missing plan text")?;
            assert!(text.contains("Logical Plan:") && text.contains("Physical Plan:"));
        } else {
            assert_eq!(
                actual["rows"], record["expected"]["rows"],
                "{}",
                record["id"]
            );
        }
        println!(
            "READ_ONLY_METADATA case={} ansi={}",
            record["id"], record["ansi"]
        );
    }
    Ok(())
}

#[tokio::test]
async fn empty_metadata_results_keep_their_schema_and_execute() -> TestResult {
    let session = context("empty_catalog", "empty_schema", false);
    session.register_catalog("empty_catalog", Arc::new(MemoryCatalogProvider::new()));
    for ansi in [false, true] {
        let actual = capture_executed_query_result(
            &session,
            &json!({"sql":"SHOW SCHEMAS","ansi":ansi,"ordered":true}),
        )
        .await?;
        assert_eq!(actual["rows"], json!([]));
        assert_eq!(actual["schema"]["fields"][0]["type"], "string");
    }
    session
        .catalog("empty_catalog")
        .ok_or("missing catalog")?
        .register_schema("empty_schema", Arc::new(MemorySchemaProvider::new()))?;
    let empty_schema = Arc::new(Schema::empty());
    session.register_table(
        "blank",
        Arc::new(MemTable::try_new(
            empty_schema.clone(),
            vec![vec![RecordBatch::new_empty(empty_schema)]],
        )?),
    )?;
    for ansi in [false, true] {
        for sql in [
            "DESCRIBE blank",
            "SHOW COLUMNS FROM blank",
            "SHOW TABLES LIKE 'missing*'",
            "SHOW TABLES LIKE '['",
            "SHOW SCHEMAS LIKE '['",
        ] {
            let actual = capture_executed_query_result(
                &session,
                &json!({"sql":sql,"ansi":ansi,"ordered":true}),
            )
            .await?;
            assert_eq!(actual["rows"], json!([]), "{sql}");
            assert!(
                !actual["schema"]["fields"]
                    .as_array()
                    .ok_or("missing fields")?
                    .is_empty()
            );
        }
    }
    let empty = context("catalog", "empty", true);
    for ansi in [false, true] {
        for sql in [
            "SHOW TABLES",
            "SHOW TABLES LIKE 'missing*'",
            "SHOW TABLES LIKE '['",
        ] {
            let actual = capture_executed_query_result(
                &empty,
                &json!({"sql":sql,"ansi":ansi,"ordered":true}),
            )
            .await?;
            assert_eq!(actual["rows"], json!([]));
            assert_eq!(
                actual["schema"]["fields"]
                    .as_array()
                    .ok_or("missing fields")?
                    .len(),
                3
            );
        }
    }
    Ok(())
}
