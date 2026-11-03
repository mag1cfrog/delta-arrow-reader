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

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
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
    replay_set_operation_metadata(&records[0], &cases, "original_nested").await
}

#[tokio::test]
async fn set_operation_metadata_controls_match_pinned_spark_stages() -> TestResult {
    let records = include_str!("fixtures/union-metadata-stages/union-metadata-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 136);
    assert_eq!(
        records[0]["manifest_sha256"],
        "1ad939e5b35de84d86ee0dbcf6d9dc24c56bd13ec88699ce990c74f3fbd980bc"
    );
    assert_eq!(
        records[0]["capture_sha256"],
        "0852fac11447760bac59f48fef08c24c53f305bcfbad7dc1c7f91120d11bc262"
    );
    let cases = records[1..].iter().collect::<Vec<_>>();
    replay_set_operation_metadata(&records[0], &cases, "captured_controls").await
}

async fn replay_set_operation_metadata(
    header: &Value,
    cases: &[&Value],
    group: &str,
) -> TestResult {
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let mut identities = std::collections::BTreeSet::new();
    for case in cases {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if case["expected"]["condition"].is_null() {
            assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
            for stage in [
                "analyzed", "optimized", "executed_before_collect", "executed",
            ] {
                assert!(
                    case["schema_stages"][stage].is_object(),
                    "missing {stage}: {case}"
                );
            }
            assert!(case["expected"]["rows"].is_array(), "missing rows: {case}");
        }
    }
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
        spark_inputs::register_recorded_inputs(&session, header, batch_size)?;
        for case in cases {
            let mut phase = "resolve";
            let mut public = None;
            let mut before = None;
            let mut executed = None;
            let actual: TestResult<Vec<Vec<Value>>> = async {
                let query = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        session_timezone: header["timezone"].as_str().unwrap_or("UTC").into(),
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                phase = "public_schema";
                if query.column_names.len() != query.logical_plan.schema().fields().len() {
                    return Err("public field count differs from output names".into());
                }
                public = Some(spark_results::capture_schema(
                    query.logical_plan.schema().fields(),
                    Some(&query.column_names),
                )?);
                phase = "create_physical_plan";
                let physical = query.create_physical_plan(&session).await?;
                phase = "physical_schema";
                before = Some(spark_results::capture_schema(
                    physical.schema().fields(), None,
                )?);
                let batches = collect(physical.clone(), session.task_ctx()).await;
                phase = "executed_schema";
                let schema = physical.schema();
                executed = Some(spark_results::capture_schema(schema.fields(), None)?);
                phase = "collect";
                let batches = batches?;
                phase = "encode";
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
                Ok(rows)
            }.await;
            comparisons += 1;
            let public_matches = case["schema_stages"].get("analyzed")
                .is_none_or(|expected| public.as_ref() == Some(expected));
            let before_matches = case["schema_stages"].get("executed_before_collect")
                .is_none_or(|expected| before.as_ref() == Some(expected));
            let executed_matches = case["schema_stages"].get("executed")
                .is_none_or(|expected| executed.as_ref() == Some(expected));
            let rows_match = case["expected"].get("rows")
                .is_none_or(|expected| actual.as_ref().is_ok_and(|rows| json!(rows) == *expected));
            let error_matches = if let Some(condition) = case["expected"]["condition"].as_str() {
                let expected_phase = match case["spark_error_phase"].as_str() {
                    Some("sql") => "resolve",
                    Some("collect") => "collect",
                    other => return Err(format!("unsupported reference error phase: {other:?}").into()),
                };
                phase == expected_phase && actual.as_ref().err().is_some_and(|error| {
                    error.to_string().contains(&format!("[{condition}]"))
                })
            } else {
                actual.is_ok()
            };
            let matches = public_matches && before_matches && executed_matches
                && rows_match && error_matches;
            eprintln!("UNION_METADATA_OBSERVATION {}", json!({
                "group":group, "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "public_schema":public, "before_collect":before,
                "physical_schema":executed, "rows":actual.as_ref().ok(),
                "native_phase":phase, "error":actual.as_ref().err().map(ToString::to_string),
                "public_matches":public_matches, "before_matches":before_matches,
                "physical_matches":executed_matches, "rows_match":rows_match,
                "error_matches":error_matches, "matches":matches,
            }));
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: public={public_matches} before={before_matches} executed={executed_matches} rows={rows_match} error={error_matches}; phase={phase}, expected={}, stages={}, actual={actual:?}, public={public:?}, before={before:?}, executed={executed:?}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(comparisons, cases.len() * 2);
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
