//! Qualified USING keys retain Spark's hidden-key marker and source metadata.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::Value;

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

#[tokio::test]
async fn using_key_metadata_matches_spark() -> TestResult {
    let mut records = include_str!("fixtures/using-key-metadata-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 156);
    for (fixture, count) in [
        (
            include_str!("fixtures/using-key-metadata-followup-spark.jsonl"),
            32,
        ),
        (
            include_str!("fixtures/using-key-metadata-boundaries-spark.jsonl"),
            48,
        ),
    ] {
        let followup = fixture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(followup.len() - 1, count);
        assert_eq!(followup[0], records[0]);
        records.extend_from_slice(&followup[1..]);
    }
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if let Some(stages) = case.get("schema_stages") {
            assert_eq!(case["expected"]["schema"], stages["analyzed"]);
        }
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let expected = &case["expected"];
            if let Some(condition) = expected["condition"].as_str() {
                let expected_cause = match condition {
                    "UNRESOLVED_COLUMN.WITH_SUGGESTION" => "missing from the schema",
                    "CANNOT_RESOLVE_STAR_EXPAND" => "cannot resolve wildcard",
                    "UNRESOLVED_USING_COLUMN_FOR_JOIN" => "[UNRESOLVED_COLUMN]",
                    _ => return Err(format!("unclassified reference error: {condition}").into()),
                };
                let actual = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                match actual {
                    Err(error) if error.to_string().contains(expected_cause) => {}
                    other => differences.push(format!(
                        "{} ANSI={} batch={batch_size}: expected {condition} during planning, got {other:?}",
                        case["id"], case["ansi"]
                    )),
                }
            } else {
                let actual = capture_spark_query_result(&session, case).await;
                if actual.as_ref().ok() != Some(expected) {
                    differences.push(format!(
                        "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn using_key_aliases_do_not_merge_distinct_source_fields() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/using-key-metadata-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    register_recorded_inputs(&session, &metadata, 32)?;
    for (criteria, cause) in [
        ("USING(id)", "[AMBIGUOUS_REFERENCE]"),
        ("ON l.id=r.id", "ambiguous attribute"),
    ] {
        let sql = format!(
            "SELECT l.id FROM (SELECT id,id FROM left_mixed) l \
             LEFT JOIN right_mixed r {criteria}"
        );
        let result = plan_query(&session, &sql, &SparkSqlOptions::default()).await;
        assert!(
            result.is_err_and(|error| error.to_string().contains(cause)),
            "{sql} must reject distinct source fields"
        );
    }
    let result = plan_query(
        &session,
        "WITH shared AS (SELECT id FROM left_mixed) \
         SELECT id FROM shared l JOIN shared r ON l.id=r.id",
        &SparkSqlOptions::default(),
    )
    .await;
    assert!(result.is_err_and(|error| error.to_string().contains("ambiguous attribute")));
    Ok(())
}

#[tokio::test]
async fn using_key_metadata_keeps_native_sql_unchanged() -> TestResult {
    async fn capture_native(
        session: &SessionContext,
        sql: &str,
    ) -> TestResult<(DFSchema, Arc<Schema>, Vec<RecordBatch>)> {
        let frame = session.sql(sql).await?;
        let logical = frame.schema().clone();
        let physical = frame.create_physical_plan().await?;
        Ok((
            logical,
            physical.schema(),
            collect(physical, session.task_ctx()).await?,
        ))
    }

    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/using-key-metadata-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &metadata, 32)?;
    }
    let mut differences = Vec::new();
    let mut snapshots = Vec::new();
    for shape in ["mixed", "nulls", "empty"] {
        for join in ["INNER", "LEFT", "RIGHT", "FULL", "LEFT SEMI", "LEFT ANTI"] {
            let sql = format!(
                "SELECT l.id AS result_id,l.lv AS result_value \
                 FROM left_{shape} l {join} JOIN right_{shape} r USING(id) \
                 ORDER BY result_id,result_value"
            );
            let expected = capture_native(&native, &sql).await;
            let before = capture_native(&configured, &sql).await;
            if expected.is_err() || before.as_ref().ok() != expected.as_ref().ok() {
                differences.push(format!(
                    "{sql} before Spark: expected {expected:?}, got {before:?}"
                ));
            }
            snapshots.push((sql, expected, before));
        }
    }
    let spark_result: TestResult = async {
        let query = plan_query(
            &configured,
            "SELECT l.*,r.* FROM left_mixed l FULL JOIN right_mixed r USING(id)",
            &SparkSqlOptions::default(),
        )
        .await?;
        collect(
            query.create_physical_plan(&configured).await?,
            configured.task_ctx(),
        )
        .await?;
        Ok(())
    }
    .await;
    if let Err(error) = spark_result {
        differences.push(format!("Spark USING exercise failed: {error}"));
    }
    for (sql, expected, before) in snapshots {
        let after = capture_native(&configured, &sql).await;
        if after.is_err() || after.as_ref().ok() != expected.as_ref().ok() {
            differences.push(format!(
                "{sql} after Spark: expected native {expected:?}, got {after:?}"
            ));
        }
        if after.as_ref().ok() != before.as_ref().ok() {
            differences.push(format!(
                "{sql} after Spark: expected configured baseline {before:?}, got {after:?}"
            ));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn using_keys_preserve_arrow_extension_and_user_metadata() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let input_metadata = HashMap::from([
        ("ARROW:extension:name".to_owned(), "arrow.json".to_owned()),
        ("ARROW:extension:metadata".to_owned(), "".to_owned()),
        ("purpose".to_owned(), "join-key".to_owned()),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, true).with_metadata(input_metadata.clone()),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some("1"), None]))],
    )?;
    session.register_table(
        "json_keys",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    for ansi_mode in [true, false] {
        for (criteria, hidden_right) in [("USING(k)", true), ("ON l.k=r.k", false)] {
            let sql = format!(
                "SELECT l.k AS left_key,r.k AS right_key \
                 FROM json_keys l LEFT JOIN json_keys r {criteria}"
            );
            let query = plan_query(
                &session,
                &sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let physical = query.create_physical_plan(&session).await?;
            for (index, field) in physical.schema().fields().iter().enumerate() {
                let logical = query.logical_plan.schema().field(index);
                assert_eq!(field.data_type(), logical.data_type());
                assert_eq!(field.is_nullable(), logical.is_nullable());
                assert_eq!(field.metadata(), logical.metadata());
                let mut expected = input_metadata.clone();
                if hidden_right && index == 1 {
                    expected.insert("__metadata_col".into(), "k".into());
                }
                assert_eq!(field.metadata(), &expected, "{sql}");
            }
            assert_eq!(
                collect(physical, session.task_ctx())
                    .await?
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                2,
            );
        }
    }
    Ok(())
}
