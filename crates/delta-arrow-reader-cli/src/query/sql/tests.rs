use super::*;

use arrow::{array::Int32Array, compute::concat_batches};
use delta_arrow_reader::{DeltaTableBuilder, WarmupMode};
use futures_util::TryStreamExt;

#[tokio::test]
async fn joins_use_distinct_snapshots_and_respect_deletion_vectors()
-> Result<(), Box<dyn std::error::Error>> {
    let corpus = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus"
    );
    let mut tables = Vec::new();
    for name in ["partitioned", "deletion_vectors"] {
        let table = DeltaTableBuilder::new(format!("{corpus}/{name}/table"))
            .with_warmup(WarmupMode::None)
            .load_table()
            .await?;
        tables.push((name.to_owned(), table));
    }
    let query = ReadOnlySqlQuery::parse(
        "SELECT p.id AS partitioned_id, d.id AS retained_id
         FROM partitioned AS p LEFT JOIN deletion_vectors AS d ON p.id = d.id
         WHERE p.id <= 12 ORDER BY p.id",
    )?;
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new().build_arc()?;
    let stream = query.execute_stream(tables, runtime).await?;
    let schema = stream.schema();
    let batches = stream.try_collect::<Vec<_>>().await?;
    let actual = concat_batches(&schema, &batches)?;
    assert_eq!(schema.field(0).name(), "partitioned_id");
    assert_eq!(schema.field(1).name(), "retained_id");
    assert_eq!(actual.num_columns(), 2);
    assert_eq!(
        actual.column(0).as_ref(),
        &Int32Array::from_iter_values(1..=12)
    );
    // The independent Spark fixture deleted IDs 2, 5, and 12 from the right table.
    assert_eq!(
        actual.column(1).as_ref(),
        &Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            None,
        ])
    );
    Ok(())
}

#[test]
fn admission_checks_nested_queries_and_one_read_only_statement() {
    for sql in [
        "SELECT count(*) FROM t",
        "WITH selected AS (SELECT id FROM t) SELECT id FROM selected ORDER BY id",
        "SELECT '; COPY t TO secret' AS text FROM t LIMIT 0",
        "SELECT id FROM t UNION ALL SELECT id FROM t",
    ] {
        assert!(ReadOnlySqlQuery::parse(sql).is_ok(), "{sql}");
    }
    for sql in [
        "",
        "-- only a comment",
        "SELECT 1; SELECT 2",
        "SELECT FROM",
        "CREATE TABLE secret (id INT)",
        "INSERT INTO t VALUES (1)",
        "DELETE FROM t",
        "UPDATE t SET id = 2",
        "COPY t TO 'secret'",
        "SET target_partitions = 1",
        "EXPLAIN ANALYZE SELECT * FROM t",
        "SELECT * INTO secret FROM t",
        "SELECT * FROM t FOR UPDATE",
        "WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT * FROM x) SELECT * FROM x",
        "SELECT * FROM (WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x) nested",
        "SELECT * FROM read_csv('secret.csv')",
        "SELECT * FROM read_parquet('https://secret/file.parquet')",
    ] {
        assert!(ReadOnlySqlQuery::parse(sql).is_err(), "{sql}");
    }
    let nested = format!("SELECT {}1{}", "(".repeat(200), ")".repeat(200));
    assert!(ReadOnlySqlQuery::parse(&nested).is_err());
}
