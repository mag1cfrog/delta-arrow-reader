//! Unordered CONCAT declarations match Spark without changing source-column metadata.

use arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_result, register_inputs};

fn row_multiset(rows: &Value) -> Option<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::new();
    for row in rows.as_array()? {
        *counts.entry(row.to_string()).or_insert(0) += 1;
    }
    Some(counts)
}

async fn native_projection(
    session: &SessionContext,
    table: &str,
) -> TestResult<(SchemaRef, BTreeMap<String, usize>)> {
    let batches = session
        .sql(&format!("SELECT b,s FROM {table}"))
        .await?
        .collect()
        .await?;
    let schema = batches.first().ok_or("missing native batches")?.schema();
    assert!(!schema.field(0).metadata().is_empty());
    assert!(!schema.field(1).metadata().is_empty());
    let mut rows = BTreeMap::new();
    for batch in batches {
        assert_eq!(batch.schema(), schema);
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .map(|column| {
                    ScalarValue::try_from_array(column.as_ref(), row)
                        .map(|value| format!("{value:?}"))
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            *rows.entry(json!(values).to_string()).or_insert(0) += 1;
        }
    }
    Ok((schema, rows))
}

// Direct columns retain their provider fields and rows, unlike derived CONCAT fields.
async fn direct_spark_columns_preserve_inputs(
    session: &SessionContext,
    metadata: &Value,
    table: &str,
) -> TestResult {
    let input = metadata["registered_inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .find(|input| input["name"] == table)
        .ok_or("missing source table")?;
    let fields = input["schema"]["fields"]
        .as_array()
        .ok_or("missing source fields")?;
    let indices = ["b", "s"].map(|name| {
        fields
            .iter()
            .position(|field| field["name"] == name)
            .ok_or("missing source column")
    });
    let [binary, string] = indices;
    let (binary, string) = (binary?, string?);
    let expected_fields = json!({"type":"struct","fields":[fields[binary],fields[string]]});
    let expected_rows = json!(
        input["rows"]
            .as_array()
            .ok_or("missing source rows")?
            .iter()
            .map(|row| json!([row[binary], row[string]]))
            .collect::<Vec<_>>()
    );
    for ansi in [true, false] {
        let actual = capture_result(
            session,
            &json!({
                "sql":format!("SELECT b,s FROM {table}"),"ansi":ansi,
            }),
        )
        .await?;
        eprintln!(
            "c06 direct-column control: {}",
            json!({
                "table":table,"ansi":ansi,"actual":actual,
            })
        );
        assert_eq!(actual["analyzed_schema"], expected_fields);
        assert_eq!(actual["physical_schema"], expected_fields);
        assert_eq!(row_multiset(&actual["rows"]), row_multiset(&expected_rows));
    }
    Ok(())
}

#[tokio::test]
async fn unordered_concat_metadata_matches_spark() -> TestResult {
    let records = include_str!("fixtures/concat-unordered-metadata-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 17);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[1..]
            .iter()
            .all(|case| case["expected"]["status"] == "ok")
    );
    let mut differences = Vec::new();
    let mut keys = HashSet::new();
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let config = SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(2);
            let plain = SessionContext::new_with_config(config.clone());
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(config),
                )
                .build(),
            );
            for target in [&plain, &session] {
                register_inputs(target, &records[0], representation, batch_size)?;
            }
            let mut native_expected = Vec::new();
            for table in ["c06_strings_required", "c06_strings_nullable"] {
                let expected = native_projection(&plain, table).await?;
                assert_eq!(native_projection(&session, table).await?, expected);
                native_expected.push((table, expected));
            }
            for case in &records[1..] {
                assert!(keys.insert((
                    case["id"].as_str().ok_or("missing ID")?.to_owned(),
                    case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    representation,
                    batch_size,
                )));
                let actual = capture_observation(&session, case).await?;
                eprintln!(
                    "c06 unordered observation: {}",
                    json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"actual":actual})
                );
                let expected = &case["expected"];
                if actual.get("error").is_some()
                    || actual.get("error_stage").is_some()
                    || !actual["analyzed_arrow_schema"].is_string()
                    || !actual["physical_arrow_schema"].is_string()
                    || actual["analyzed_schema"] != expected["analyzed_schema"]
                    || actual["physical_schema"] != expected["physical_schema"]
                    || row_multiset(&actual["rows"]) != row_multiset(&expected["rows"])
                {
                    differences.push(format!(
                        "{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {actual}",
                        case["id"], case["ansi"]
                    ));
                }
            }
            for (table, expected) in native_expected {
                direct_spark_columns_preserve_inputs(&session, &records[0], table).await?;
                assert_eq!(native_projection(&session, table).await?, expected);
            }
        }
    }
    assert_eq!(keys.len(), 96);
    eprintln!(
        "C06 unordered compared {}, strict mismatches {}",
        keys.len(),
        differences.len()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn nested_string_concat_keeps_one_flattened_call() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/concat-unordered-metadata-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing CONCAT input metadata")?,
    )?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    register_inputs(&session, &metadata, 0, 32)?;
    for width in [3, 16] {
        let columns = (0..width)
            .map(|index| if index % 2 == 0 { "s" } else { "t" })
            .collect::<Vec<_>>();
        let nested = columns[1..]
            .iter()
            .fold(columns[0].to_owned(), |left, right| {
                format!("concat({left},{right})")
            });
        let nested_identity = columns[1..]
            .iter()
            .fold("concat(s)".to_owned(), |left, right| {
                format!("concat({left},{right})")
            });
        let nested_empty = columns.iter().fold("concat()".to_owned(), |left, right| {
            format!("concat({left},{right})")
        });
        for (expression, arguments) in [
            (nested.clone(), width),
            (columns.join(" || "), width),
            (format!("concat({nested})"), width),
            (nested_identity, width),
            (nested_empty, width + 1),
        ] {
            for ansi_mode in [true, false] {
                let sql = format!("SELECT {expression} AS r FROM c06_strings_required");
                let query = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?;
                let mut state = session.state();
                let extensions = &mut state.config_mut().options_mut().extensions;
                extensions.insert(SparkSqlExpressionSemantics::default());
                extensions.insert(PreserveAggregateStatisticsSchema::default());
                let optimized = state.optimize(&query.logical_plan)?;
                let mut arities = Vec::new();
                optimized.apply(|plan| {
                    plan.apply_expressions(|expression| {
                        expression.apply(|expression| {
                            if let Expr::ScalarFunction(function) = expression
                                && function.func.inner().is::<SparkConcat>()
                            {
                                arities.push(function.args.len());
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })
                    })?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
                eprintln!(
                    "C06 CONCAT plan: {}",
                    json!({
                        "sql":sql,"ansi":ansi_mode,"concat_arities":arities,
                        "optimized":optimized.display_indent_schema().to_string(),
                    })
                );
                assert_eq!(arities, vec![arguments], "{sql} ANSI={ansi_mode}");
            }
        }
    }
    Ok(())
}
