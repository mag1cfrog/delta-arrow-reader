//! UNION output aliases follow the left input without changing native metadata policy.

use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

#[tokio::test]
async fn union_output_metadata_matches_pinned_spark_stages() -> TestResult {
    let records = include_str!("fixtures/conditional-nested-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let cases = records[1..]
        .iter()
        .filter(|case| {
            matches!(
                case["id"].as_str(),
                Some(
                    "union_struct_reordered_names"
                        | "union_struct_different_names"
                        | "union_struct_same_names_numeric_control"
                )
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 6);
    let mut differences = Vec::new();
    let mut comparisons = 0;
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
        for case in &cases {
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            assert_eq!(
                query.column_names.len(),
                query.logical_plan.schema().fields().len()
            );
            let public = spark_results::capture_schema(
                query.logical_plan.schema().fields(),
                Some(&query.column_names),
            )?;
            let physical = query.create_physical_plan(&session).await?;
            let before = spark_results::capture_schema(physical.schema().fields(), None)?;
            let batches = collect(physical.clone(), session.task_ctx()).await?;
            let schema = physical.schema();
            let executed = spark_results::capture_schema(schema.fields(), None)?;
            let mut rows = Vec::new();
            for batch in batches {
                spark_results::validate_batch(schema.as_ref(), &batch)?;
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| spark_results::encode_value(column.as_ref(), row))
                            .collect::<TestResult<Vec<_>>>()?,
                    );
                }
            }
            rows.sort_by_key(|row| json!(row).to_string());
            comparisons += 1;
            let matches = public == case["schema_stages"]["analyzed"]
                && before == case["schema_stages"]["executed_before_collect"]
                && executed == case["schema_stages"]["executed"]
                && json!(rows) == case["expected"]["rows"];
            eprintln!("UNION_METADATA_OBSERVATION {}", json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "public_schema":public, "before_collect":before,
                "physical_schema":executed, "rows":rows, "matches":matches,
            }));
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {}",
                    case["id"], case["ansi"], case["schema_stages"],
                    json!({"public":public,"before":before,"executed":executed,"rows":rows})
                ));
            }
        }
    }
    assert_eq!(comparisons, 12);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_union_keeps_its_metadata_intersection() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let metadata = [
        ("__autoGeneratedAlias".to_string(), "true".to_string()),
        ("source".to_string(), "right".to_string()),
    ]
    .into_iter()
    .collect();
    let field = Field::new("v", DataType::Int32, false).with_metadata(metadata);
    for session in [&native, &configured] {
        for (table, field, value) in [
            ("bare", Field::new("v", DataType::Int32, false), 1),
            ("marked", field.clone(), 2),
        ] {
            session.register_batch(
                table,
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![field])),
                    vec![Arc::new(Int32Array::from(vec![value]))],
                )?,
            )?;
        }
    }
    let sql = "SELECT v AS r FROM bare UNION ALL SELECT v FROM marked";
    let expected = native.sql(sql).await?.collect().await?;
    let actual = configured.sql(sql).await?.collect().await?;
    assert_eq!(actual, expected);
    assert_eq!(actual[0].schema().field(0).metadata(), field.metadata());
    Ok(())
}
