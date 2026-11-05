//! Independent priority among nested aggregates, input typing and volatile arguments.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_schema, capture_spark_optimized_observation, encode_value, validate_batch,
};

fn references() -> TestResult<Vec<Value>> {
    let fixture = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/global-aggregate-admission-priority-spark.jsonl"
    ))?;
    let references = fixture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/global-aggregate-admission-priority-cases.json"
    ))?;
    for key in [
        "input_version",
        "capture_kind",
        "spark_version",
        "spark_revision",
        "timezone",
        "main_manifest_sha256",
        "registered_udf",
    ] {
        assert_eq!(references[0][key], manifest[key]);
    }
    assert_eq!(
        references[0]["manifest_sha256"],
        "8e06b315624b6f76c00345e28440cc80f7865e9f4f956d47d854dd80587336de"
    );
    assert!(
        references[0]["spark_build"]
            .as_str()
            .ok_or("missing Spark build")?
            .contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    );
    let cases = manifest["cases"]
        .as_array()
        .ok_or("missing priority cases")?;
    assert_eq!(cases.len(), 17);
    assert_eq!(references.len(), 35);
    let mut udf_references = 0;
    for (mode, ansi) in [true, false].into_iter().enumerate() {
        for (index, case) in cases.iter().enumerate() {
            let reference = &references[1 + mode * cases.len() + index];
            assert_eq!(reference["ansi"], ansi);
            for key in ["id", "scope", "sql", "udf_sql", "success_assertions"] {
                assert_eq!(reference[key], case[key]);
            }
            let mut observations = vec![reference];
            if case.get("udf_sql").is_some() {
                observations.push(&reference["udf_reference"]);
                udf_references += 1;
            }
            for observed in observations {
                assert!(
                    observed.get("expected").is_some(),
                    "capture failed: {observed}"
                );
                assert_eq!(
                    observed["schema_stages"]["public"],
                    observed["schema_stages"]["analyzed"]
                );
            }
        }
    }
    assert_eq!(udf_references, 18);
    Ok(references)
}

async fn observe(
    session: &SessionContext,
    sql: &str,
    ansi: bool,
    callbacks: &AtomicUsize,
) -> TestResult<Value> {
    let mut phase = "resolve";
    let mut schemas = json!({});
    let mut counts = json!({});
    let mut optimized_observation = Value::Null;
    let mut optimized_error = Value::Null;
    let mut rows_raw = Value::Null;
    let outcome: TestResult<Vec<Vec<Value>>> = async {
        let query = plan_query(
            session,
            sql,
            &SparkSqlOptions {
                ansi_mode: ansi,
                session_timezone: "UTC".into(),
                ..Default::default()
            },
        )
        .await;
        counts["after_resolve"] = json!(callbacks.load(Ordering::SeqCst));
        let query = query?;
        phase = "public_schema";
        if query.column_names.len() != query.logical_plan.schema().fields().len() {
            return Err("public field count differs from output names".into());
        }
        schemas["public"] = capture_schema(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        )?;
        let optimized =
            capture_spark_optimized_observation(session, &json!({"sql":sql,"ansi":ansi})).await;
        if let Ok(observation) = &optimized {
            schemas["optimized"] = observation["schema"].clone();
            optimized_observation = observation.clone();
        }
        optimized_error = json!(optimized.as_ref().err().map(ToString::to_string));
        counts["after_optimized"] = json!(callbacks.load(Ordering::SeqCst));
        phase = "create_physical_plan";
        let physical = query.create_physical_plan(session).await;
        counts["after_physical_planning"] = json!(callbacks.load(Ordering::SeqCst));
        let physical = physical?;
        let schema = physical.schema();
        schemas["executed"] = capture_schema(schema.fields(), None)?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await;
        counts["after_collect"] = json!(callbacks.load(Ordering::SeqCst));
        schemas["executed_after_collect"] = capture_schema(physical.schema().fields(), None)?;
        let batches = batches?;
        phase = "encode";
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                );
            }
        }
        rows_raw = json!(rows);
        rows.sort_by_key(|row| json!(row).to_string());
        phase = "success";
        Ok(rows)
    }
    .await;
    Ok(
        json!({"phase":phase,"schema_stages":schemas,"callback_counts":counts,
        "rows":outcome.as_ref().ok(),"rows_raw":rows_raw,
        "error":outcome.as_ref().err().map(ToString::to_string),
        "optimized_observation":optimized_observation,"optimized_error":optimized_error,
        "final_callbacks":callbacks.load(Ordering::SeqCst)}),
    )
}

fn matches_reference(case: &Value, reference: &Value, actual: &Value, policy: &str) -> bool {
    if let Some(condition) = reference["expected"]["condition"].as_str() {
        let cause = match condition {
            "NESTED_AGGREGATE_FUNCTION" if case["scope"] == "nested_count_if" => {
                "COUNT_IF argument cannot contain an aggregate function"
            }
            "NESTED_AGGREGATE_FUNCTION" => "[NESTED_AGGREGATE_FUNCTION]",
            "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION" => {
                "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION"
            }
            "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" => {
                "No function matches the given name and argument types 'sum(List"
            }
            "_LEGACY_ERROR_TEMP_1031" => "[_LEGACY_ERROR_TEMP_1031]",
            _ => return false,
        };
        reference["phase"] == "planning"
            && reference["failure_stage"] == "spark.sql"
            && actual["phase"] == "resolve"
            && actual["error"]
                .as_str()
                .is_some_and(|error| error.contains(cause))
            && actual["final_callbacks"] == 0
    } else {
        reference["phase"] == "success"
            && actual["phase"] == "success"
            && actual["optimized_error"].is_null()
            && actual["optimized_observation"]["names_match"] == true
            && ["public", "optimized", "executed", "executed_after_collect"]
                .into_iter()
                .all(|stage| {
                    reference["schema_stages"].get(stage).is_some()
                        && actual["schema_stages"][stage] == reference["schema_stages"][stage]
                })
            && if policy == "exact_schema_and_rows" {
                actual["rows"] == reference["expected"]["rows"]
            } else if policy == "admission_fields_and_cardinality" {
                actual["rows"].as_array().map(Vec::len)
                    == reference["expected"]["rows"].as_array().map(Vec::len)
            } else {
                false
            }
    }
}

#[tokio::test]
async fn aggregate_priority_and_scalar_subquery_admission_match_independent_spark() -> TestResult {
    let references = references()?;
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
        let callbacks = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&callbacks);
        session.register_udf(create_udf(
            "observed_arg",
            vec![DataType::Int32],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5))))
            }),
        ));
        for case in &references[1..] {
            let mut variants = vec![(
                "original",
                &case["sql"],
                case,
                case["success_assertions"]
                    .as_str()
                    .ok_or("missing success policy")?,
            )];
            if let Some(sql) = case.get("udf_sql") {
                variants.push(("udf", sql, &case["udf_reference"], "exact_schema_and_rows"));
            }
            for (variant, sql, reference, policy) in variants {
                callbacks.store(0, Ordering::SeqCst);
                let actual = observe(
                    &session,
                    sql.as_str().ok_or("missing SQL")?,
                    case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    &callbacks,
                )
                .await?;
                let no_planning_callbacks = [
                    "after_resolve",
                    "after_optimized",
                    "after_physical_planning",
                ]
                .into_iter()
                .all(|stage| {
                    actual["callback_counts"][stage].is_null()
                        || actual["callback_counts"][stage] == 0
                });
                let live_control = case["scope"] != "callback_control"
                    || actual["final_callbacks"]
                        .as_u64()
                        .is_some_and(|count| count > 0);
                let matches = no_planning_callbacks
                    && live_control
                    && matches_reference(case, reference, &actual, policy);
                eprintln!(
                    "global_aggregate_priority_observation={}",
                    json!({
                    "id":case["id"],"scope":case["scope"],"ansi":case["ansi"],
                    "batch":batch_size,"variant":variant,"sql":sql,"reference":reference,
                    "policy":policy,"matches":matches,"observation":actual})
                );
                if !matches {
                    differences.push(format!("{} variant={variant} ANSI={} batch={batch_size}: expected {}, got {actual}",
                        case["id"], case["ansi"], reference["expected"]));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn aggregate_argument_structure_and_subquery_scope_match_independent_spark() -> TestResult {
    let fixture = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/global-aggregate-argument-structure-spark.jsonl"
    ))?;
    let references = fixture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/global-aggregate-argument-structure-cases.json"
    ))?;
    assert_eq!(references.len(), 11);
    assert_eq!(
        references[0]["manifest_sha256"],
        "817d3dfa29f2afdd326bc975755614a32541246f3d13006fac12ddb58b64b9cf"
    );
    for key in [
        "input_version",
        "capture_kind",
        "spark_version",
        "spark_revision",
        "timezone",
    ] {
        assert_eq!(references[0][key], manifest[key]);
    }
    assert!(
        references[0]["spark_build"]
            .as_str()
            .ok_or("missing Spark build")?
            .contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    );
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
        let callbacks = AtomicUsize::new(0);
        for (index, case) in references[1..].iter().enumerate() {
            for key in ["id", "scope", "sql", "success_assertions"] {
                assert_eq!(case[key], manifest["cases"][index % 5][key]);
            }
            assert_eq!(case["ansi"], index < 5);
            assert!(case.get("expected").is_some(), "capture failed: {case}");
            assert_eq!(
                case["schema_stages"]["public"],
                case["schema_stages"]["analyzed"]
            );
            let actual = observe(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                &callbacks,
            )
            .await?;
            let matches = matches_reference(case, case, &actual, "exact_schema_and_rows");
            eprintln!(
                "global_aggregate_argument_structure_observation={}",
                json!({
                "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                "reference":case,"matches":matches,"observation":actual})
            );
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn pending_count_if_and_count_keep_the_nested_argument_cause() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let planned = plan_query(
        &session,
        "SELECT COUNT_IF(SUM(v)>0) AS a,COUNT(SUM(v)>0) AS b FROM VALUES(1),(2)t(v)",
        &SparkSqlOptions::default(),
    )
    .await;
    assert!(
        planned.as_ref().is_err_and(|error| error
            .to_string()
            .contains("COUNT_IF argument cannot contain an aggregate function")),
        "{planned:?}"
    );
    Ok(())
}

#[tokio::test]
async fn additional_aggregate_argument_controls_match_independent_spark() -> TestResult {
    let mut differences = Vec::new();
    for (manifest_source, fixture_name, manifest_digest, case_count, udf_count) in [
        (
            include_str!("fixtures/global-aggregate-listagg-identity-cases.json"),
            "global-aggregate-listagg-identity-spark.jsonl",
            "2b7569c9aef1cb63a632562064c07e8d3fa3562aa05dfa6dc4e28e740a3e1136",
            12,
            6,
        ),
        (
            include_str!("fixtures/global-aggregate-sibling-priority-cases.json"),
            "global-aggregate-sibling-priority-spark.jsonl",
            "14ab52cfcd36e42497ea5ef1355ab4162d4ddc3bad793d4c282c6363423ec099",
            6,
            4,
        ),
        (
            include_str!("fixtures/global-aggregate-subquery-producer-cases.json"),
            "global-aggregate-subquery-producer-spark.jsonl",
            "8263c8567d289c84f9012ec2c2160852777811461cd0a9f172889b4929faeea4",
            4,
            0,
        ),
    ] {
        let manifest: Value = serde_json::from_str(manifest_source)?;
        let fixture = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures")
                .join(fixture_name),
        )?;
        let records = fixture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        let cases = manifest["cases"]
            .as_array()
            .ok_or("missing control cases")?;
        assert_eq!(cases.len(), case_count);
        assert_eq!(
            cases
                .iter()
                .filter(|case| case.get("udf_sql").is_some())
                .count(),
            udf_count
        );
        assert_eq!(records.len(), 1 + case_count * 2);
        assert_eq!(records[0]["manifest_sha256"], manifest_digest);
        for key in [
            "input_version",
            "capture_kind",
            "spark_version",
            "spark_revision",
            "timezone",
            "registered_udf",
        ] {
            assert_eq!(records[0][key], manifest[key]);
        }
        assert!(
            records[0]["spark_build"]
                .as_str()
                .ok_or("missing Spark build")?
                .contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        );
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
            let callbacks = Arc::new(AtomicUsize::new(0));
            for (name, data_type, value) in [
                (
                    "observed_arg",
                    DataType::Float64,
                    ScalarValue::Float64(Some(0.5)),
                ),
                ("observed_null", DataType::Null, ScalarValue::Null),
            ] {
                let observed = Arc::clone(&callbacks);
                session.register_udf(create_udf(
                    name,
                    vec![DataType::Int32],
                    data_type,
                    Volatility::Volatile,
                    Arc::new(move |_| {
                        observed.fetch_add(1, Ordering::SeqCst);
                        Ok(ColumnarValue::Scalar(value.clone()))
                    }),
                ));
            }
            for (index, case) in records[1..].iter().enumerate() {
                for key in ["id", "scope", "sql", "udf_sql", "success_assertions"] {
                    assert_eq!(case[key], cases[index % case_count][key]);
                }
                assert_eq!(case["ansi"], index < case_count);
                let mut variants = vec![("original", &case["sql"], case)];
                if let Some(sql) = case.get("udf_sql") {
                    variants.push(("udf", sql, &case["udf_reference"]));
                }
                for (variant, sql, reference) in variants {
                    assert!(
                        reference.get("expected").is_some(),
                        "capture failed: {reference}"
                    );
                    assert_eq!(
                        reference["schema_stages"]["public"],
                        reference["schema_stages"]["analyzed"]
                    );
                    callbacks.store(0, Ordering::SeqCst);
                    let actual = observe(
                        &session,
                        sql.as_str().ok_or("missing SQL")?,
                        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        &callbacks,
                    )
                    .await?;
                    let no_planning_callbacks = [
                        "after_resolve",
                        "after_optimized",
                        "after_physical_planning",
                    ]
                    .into_iter()
                    .all(|stage| {
                        actual["callback_counts"][stage].is_null()
                            || actual["callback_counts"][stage] == 0
                    });
                    let matches = no_planning_callbacks
                        && matches_reference(case, reference, &actual, "exact_schema_and_rows");
                    eprintln!(
                        "global_{}_observation={}",
                        manifest["capture_kind"]
                            .as_str()
                            .ok_or("missing capture kind")?,
                        json!({
                        "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                        "variant":variant,"sql":sql,"reference":reference,
                        "matches":matches,"observation":actual})
                    );
                    comparisons += 1;
                    if !matches {
                        differences.push(format!("{} variant={variant} ANSI={} batch={batch_size}: expected {}, got {actual}",
                            case["id"], case["ansi"], reference["expected"]));
                    }
                }
            }
            for ansi in [true, false] {
                for name in ["observed_arg", "observed_null"] {
                    callbacks.store(0, Ordering::SeqCst);
                    let actual = observe(
                        &session,
                        &format!("SELECT {name}(v) AS result FROM VALUES(1),(2)t(v)"),
                        ansi,
                        &callbacks,
                    )
                    .await?;
                    let live = actual["phase"] == "success"
                        && actual["rows"].as_array().map(Vec::len) == Some(2)
                        && actual["final_callbacks"]
                            .as_u64()
                            .is_some_and(|count| count > 0)
                        && [
                            "after_resolve",
                            "after_optimized",
                            "after_physical_planning",
                        ]
                        .into_iter()
                        .all(|stage| actual["callback_counts"][stage] == 0);
                    eprintln!(
                        "global_aggregate_callback_liveness={}",
                        json!({
                        "name":name,"ansi":ansi,"batch":batch_size,"matches":live,
                        "capture_kind":manifest["capture_kind"],"observation":actual})
                    );
                    if !live {
                        differences.push(format!(
                            "{name} scalar liveness ANSI={ansi} batch={batch_size}: {actual}"
                        ));
                    }
                }
            }
        }
        assert_eq!(comparisons, (case_count + udf_count) * 4);
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
