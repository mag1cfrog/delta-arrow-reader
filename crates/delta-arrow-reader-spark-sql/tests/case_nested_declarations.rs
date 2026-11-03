//! Spark CASE merges nested declarations and retains value casts while pruning.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
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

const PACKAGES: &str = include_str!("fixtures/case-nested-declarations/delivery-packages.json");
const REFERENCES: &[&str] = &[
    include_str!("fixtures/case-nested-declarations/spark.jsonl"),
    include_str!("fixtures/case-nested-declarations/pruning-spark.jsonl"),
    include_str!("fixtures/case-nested-declarations/numeric-pruning-spark.jsonl"),
];

#[tokio::test]
async fn nested_case_declarations_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(PACKAGES)?;
    let packages = manifest["packages"].as_array().ok_or("missing packages")?;
    assert_eq!(packages.len(), REFERENCES.len());
    let mut identities = BTreeSet::new();
    let mut differences = Vec::new();
    let mut comparisons = 0;
    let mut strict_matches = 0;
    let mut inherited_condition_batches = BTreeSet::new();
    for (package, reference) in packages.iter().zip(REFERENCES) {
        let records = reference
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], manifest["spark_version"]);
        assert_eq!(records[0]["spark_revision"], manifest["spark_revision"]);
        assert_eq!(records[0]["timezone"], manifest["timezone"]);
        assert_eq!(records[0]["capture_sha256"], manifest["capture_script_sha256"]);
        assert_eq!(records[0]["manifest_sha256"], package["capture_manifest_sha256"]);
        assert_eq!(json!(records.len() - 1), package["records"]);
        for case in &records[1..] {
            assert!(identities.insert((
                package["package"].to_string(),
                case["id"].as_str().ok_or("missing ID")?.to_owned(),
                case["input"].as_str().ok_or("missing input")?.to_owned(),
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            )));
            if case["expected"].get("condition").is_none() {
                for stage in [
                    "analyzed",
                    "optimized",
                    "executed_before_collect",
                    "executed",
                ] {
                    assert!(case["schema_stages"].get(stage).is_some());
                }
                assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
                assert!(case["expected"]["rows"].is_array());
            } else {
                assert!(case["spark_error_phase"].is_string());
            }
        }
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
            spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
            for case in &records[1..] {
                comparisons += 1;
                let mut comparison =
                    spark_results::compare_exact_spark_query_stages(&session, case).await?;
                comparison["package"] = package["package"].clone();
                comparison["id"] = case["id"].clone();
                comparison["input"] = case["input"].clone();
                comparison["ansi"] = case["ansi"].clone();
                comparison["batch"] = json!(batch_size);
                eprintln!("CASE_NESTED_DECLARATIONS_OBSERVATION {comparison}");
                if package["package"] == "fields"
                    && case["id"] == "required_condition_error"
                    && case["input"] == "literal"
                    && case["ansi"] == true
                {
                    // #870 preserves the full pre-change observation, including its late phase.
                    assert_eq!(case["expected"]["condition"], "CAST_INVALID_INPUT");
                    assert_eq!(case["spark_error_phase"], "optimized_schema");
                    let schema = &case["schema_stages"]["analyzed"];
                    assert_eq!(
                        comparison,
                        json!({
                            "public_schema":schema, "before_collect":schema,
                            "physical_schema":schema, "rows":null, "rows_raw":null,
                            "native_phase":"collect",
                            "error":"Execution error: [CAST_INVALID_INPUT] Cannot cast STRING to BOOLEAN",
                            "public_matches":true, "before_matches":true,
                            "physical_matches":true, "rows_match":true,
                            "ordered_rows_match":true, "error_matches":false, "matches":false,
                            "package":"fields", "id":"required_condition_error",
                            "input":"literal", "ansi":true, "batch":batch_size,
                        }),
                        "issue #870, batch={batch_size}"
                    );
                    assert!(inherited_condition_batches.insert(batch_size));
                } else if comparison["matches"] == true {
                    strict_matches += 1;
                } else {
                    differences.push(format!("expected={case}, actual={comparison}"));
                }
            }
        }
    }
    assert_eq!(identities.len(), 98);
    assert_eq!(comparisons, 196);
    assert_eq!(strict_matches, 194);
    assert_eq!(inherited_condition_batches, BTreeSet::from([1, 32]));
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_nested_case() -> TestResult {
    let list = |values: &[ScalarValue], data_type: &DataType, nullable: bool| {
        ScalarValue::List(ScalarValue::new_list(values, data_type, nullable))
            .to_array_of_size(3)
    };
    let required_values = list(
        &[ScalarValue::Boolean(Some(true))],
        &DataType::Boolean,
        false,
    )?;
    let nullable_values = list(
        &[ScalarValue::Boolean(Some(false)), ScalarValue::Boolean(None)],
        &DataType::Boolean,
        true,
    )?;
    let narrow_values = list(
        &[ScalarValue::Int32(Some(1)), ScalarValue::Int32(None)],
        &DataType::Int32,
        true,
    )?;
    let wide_values = list(&[ScalarValue::Int64(Some(2))], &DataType::Int64, false)?;
    let record = |items: ArrayRef| -> TestResult<ArrayRef> {
        Ok(Arc::new(StructArray::try_new(
            vec![Field::new("items", items.data_type().clone(), false)].into(),
            vec![items],
            None,
        )?))
    };
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None])) as ArrayRef,
        ),
        ("required_record", record(required_values.clone())?),
        ("nullable_record", record(nullable_values.clone())?),
        ("required_values", required_values),
        ("nullable_values", nullable_values),
        ("narrow_values", narrow_values),
        ("wide_values", wide_values),
    ])?;
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let config = SessionConfig::new()
            .with_batch_size(batch_size)
            .with_target_partitions(2);
        let native = SessionContext::new_with_config(config.clone());
        let configured = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config),
            )
            .build(),
        );
        for session in [&native, &configured] {
            session.register_batch("native_nested", batch.clone())?;
            session.register_batch("native_nested_empty", batch.slice(0, 0))?;
        }
        for after_spark in [false, true] {
            if after_spark {
                let query = plan_query(
                    &configured,
                    "SELECT CASE WHEN FALSE THEN wide_values ELSE narrow_values END AS r FROM native_nested",
                    &SparkSqlOptions::default(),
                )
                .await?;
                collect(
                    query.create_physical_plan(&configured).await?,
                    configured.task_ctx(),
                )
                .await?;
            }
            for (provider, rows) in [("native_nested", 3), ("native_nested_empty", 0)] {
                for expression in [
                    "CASE WHEN true THEN required_values ELSE nullable_values END",
                    "CASE WHEN false THEN nullable_values ELSE required_values END",
                    "CASE WHEN CAST(NULL AS BOOLEAN) THEN required_values ELSE nullable_values END",
                    "CASE WHEN id=0 THEN required_values ELSE nullable_values END",
                    "CASE WHEN false THEN wide_values ELSE narrow_values END",
                    "CASE WHEN id=0 THEN narrow_values ELSE wide_values END",
                    "CASE WHEN id=0 THEN required_record ELSE nullable_record END",
                    "CASE WHEN id=0 THEN nullable_record END",
                    "CASE WHEN false THEN required_values END",
                    "CASE WHEN id=0 THEN NULL ELSE nullable_values END",
                ] {
                    comparisons += 1;
                    let sql = format!("SELECT {expression} AS r FROM {provider}");
                    let expected =
                        spark_results::capture_native_query_observation(&native, &sql).await?;
                    let actual =
                        spark_results::capture_native_query_observation(&configured, &sql).await?;
                    eprintln!(
                        "CASE_NATIVE_NESTED_DECLARATIONS_OBSERVATION {}",
                        json!({
                            "sql":sql, "provider":provider, "batch":batch_size,
                            "after_spark":after_spark, "native":expected, "configured":actual,
                        })
                    );
                    assert_eq!(expected["phase"], "encode", "{sql}");
                    assert_eq!(
                        expected["rows"].as_array().map(Vec::len),
                        Some(rows),
                        "{sql}"
                    );
                    assert_eq!(
                        actual, expected,
                        "{sql}, batch={batch_size}, after Spark={after_spark}"
                    );
                }
            }
        }
    }
    assert_eq!(comparisons, 80);
    Ok(())
}
