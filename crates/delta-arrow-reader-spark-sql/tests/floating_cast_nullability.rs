//! Floating conversion stages, values and required errors retain their recorded contracts.

use std::collections::BTreeSet;
use std::error::Error;
use std::path::Path;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn matches_retained_type_function_names(
    case: &Value,
    observation: &Value,
    batch_size: usize,
    controls: &Value,
) -> TestResult<Option<bool>> {
    let control = controls["controls"]
        .as_array()
        .ok_or("missing retained name controls")?
        .iter()
        .find(|control| {
            control["id"] == case["id"]
                && control["reference"]["sql"] == case["sql"]
                && control["ansi"] == case["ansi"]
                && control["batch"] == batch_size
        });
    let Some(control) = control else {
        return Ok(None);
    };
    let mut expected = control.clone();
    let public = expected["observation"]["public_schema"]["fields"]
        .as_array_mut()
        .ok_or("missing retained public Fields")?;
    let spark = case["schema_stages"]["analyzed"]["fields"]
        .as_array()
        .ok_or("missing independent public Fields")?;
    if public.len() != 4 || spark.len() != 4 {
        return Ok(Some(false));
    }
    for (field, spark) in public.iter_mut().zip(spark) {
        if field["nullable"] != false || spark["nullable"] != true {
            return Ok(Some(false));
        }
        field["nullable"] = spark["nullable"].clone();
    }
    let mut comparison = observation.clone();
    comparison["batch"] = json!(batch_size);
    Ok(Some(spark_results::matches_inherited_query_observation(
        case,
        &comparison,
        &expected,
    )))
}

#[test]
fn retained_type_function_names_require_the_nullable_repair_and_full_observation() -> TestResult {
    let controls: Value = serde_json::from_str(include_str!(
        "fixtures/numeric-cast-nullability/retained-type-function-names.json"
    ))?;
    let control = &controls["controls"][0];
    let case = &control["reference"];
    let batch = control["batch"].as_u64().ok_or("missing batch")? as usize;
    let mut repaired = control["observation"].clone();
    for index in 0..4 {
        repaired["public_schema"]["fields"][index]["nullable"] =
            case["schema_stages"]["analyzed"]["fields"][index]["nullable"].clone();
    }
    assert_eq!(
        matches_retained_type_function_names(case, &repaired, batch, &controls)?,
        Some(true)
    );
    assert_eq!(
        matches_retained_type_function_names(case, &control["observation"], batch, &controls)?,
        Some(false)
    );
    for (pointer, value) in [
        ("/public_schema/fields/0/name", json!("unrelated")),
        ("/public_schema/fields/0/type", json!("long")),
        ("/public_schema/fields/0/nullable", json!(false)),
        (
            "/public_schema/fields/0/metadata",
            json!({"new":"metadata"}),
        ),
        ("/optimized_schema/fields/0/nullable", json!(true)),
        ("/before_collect/fields/0/name", json!("unrelated")),
        ("/physical_schema", Value::Null),
        ("/rows", json!([["2", "1", "1", "1"]])),
        ("/rows_raw", Value::Null),
        ("/error", json!("unrelated failure")),
        ("/optimized_matches", json!(true)),
    ] {
        let mut changed = repaired.clone();
        *changed
            .pointer_mut(pointer)
            .ok_or("missing mutation path")? = value;
        assert_eq!(
            matches_retained_type_function_names(case, &changed, batch, &controls)?,
            Some(false),
            "{pointer}"
        );
    }
    let mut different_sql = case.clone();
    different_sql["sql"] = json!("SELECT 1");
    assert_eq!(
        matches_retained_type_function_names(&different_sql, &repaired, batch, &controls)?,
        None
    );
    let mut different_id = case.clone();
    different_id["id"] = json!("another_case");
    assert_eq!(
        matches_retained_type_function_names(&different_id, &repaired, batch, &controls)?,
        None
    );
    let mut different_ansi = case.clone();
    different_ansi["ansi"] = json!(!case["ansi"].as_bool().ok_or("missing ANSI mode")?);
    assert_ne!(
        matches_retained_type_function_names(&different_ansi, &repaired, batch, &controls)?,
        Some(true)
    );
    assert_eq!(
        matches_retained_type_function_names(case, &repaired, 64, &controls)?,
        None
    );
    Ok(())
}

#[tokio::test]
async fn floating_integer_cast_fields_match_recorded_stages() -> TestResult {
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/numeric-cast-nullability/manifest.json"
    ))?;
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/numeric-cast-nullability/spark.jsonl");
    let records = std::fs::read_to_string(path)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 264);
    let header = &records[0];
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(header["timezone"], "UTC");
    assert_eq!(header["selection"], "all");
    assert_eq!(
        header["manifest_sha256"],
        "c5b970e8239fcb61637d52b9a7d45484f76865bffe136169609f329b9d37d4bc"
    );
    assert_eq!(header["inputs"], manifest["inputs"]);
    assert_eq!(header["native"], manifest["native"]);
    let definitions = manifest["cases"].as_array().ok_or("missing cases")?;
    let mut expected_identities = BTreeSet::new();
    for case in definitions {
        for ansi in [true, false] {
            expected_identities.insert((case["id"].to_string(), ansi));
        }
    }
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(identities.insert((case["id"].to_string(), ansi)));
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("undeclared captured case")?;
        for key in ["id", "domain", "sql", "ordered", "original_fixture"] {
            assert_eq!(case[key], definition[key], "{key}: {}", case["id"]);
        }
        assert_eq!(case["settings"]["spark.sql.session.timeZone"], "UTC");
        assert_eq!(case["settings"]["spark.sql.ansi.enabled"], ansi.to_string());
        if case["expected"].get("rows").is_some() {
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"][stage].is_object(), "{stage}: {case}");
            }
        }
    }
    assert_eq!(identities, expected_identities);
    let mut original_count = 0;
    for (fixture, source) in [
        (
            "trim-case-connected-spark.jsonl",
            include_str!("fixtures/trim-case-connected-spark.jsonl"),
        ),
        (
            "substring-arguments-spark.jsonl",
            include_str!("fixtures/substring-arguments-spark.jsonl"),
        ),
        (
            "regexp-replace-position-int-spark.jsonl",
            include_str!("fixtures/regexp-replace-position-int-spark.jsonl"),
        ),
    ] {
        let original = source
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        for case in records[1..]
            .iter()
            .filter(|case| case["original_fixture"] == fixture)
        {
            let original = original[1..]
                .iter()
                .find(|original| original["id"] == case["id"] && original["ansi"] == case["ansi"])
                .ok_or("missing original consumer")?;
            assert_eq!(case["sql"], original["sql"]);
            assert_eq!(
                case["expected"]["schema"],
                original["expected"]["analyzed_schema"]
            );
            assert_eq!(case["expected"]["rows"], original["expected"]["rows"]);
            assert_eq!(
                case["schema_stages"]["executed"],
                original["expected"]["physical_schema"]
            );
            if let Some(optimized) = original["expected"].get("optimized_schema") {
                assert_eq!(&case["schema_stages"]["optimized"], optimized);
            }
            original_count += 1;
        }
    }
    assert_eq!(original_count, 18);
    let retained_names: Value = serde_json::from_str(include_str!(
        "fixtures/numeric-cast-nullability/retained-type-function-names.json"
    ))?;
    assert_eq!(retained_names["issue"], 1019);
    assert_eq!(
        retained_names["baseline_head"],
        "83924271c761979616842f7ea731bf0bd9aab120"
    );
    assert_eq!(retained_names["controls"].as_array().map(Vec::len), Some(4));

    let mut differences = Vec::new();
    let mut comparisons = 0;
    let mut retained_contexts = 0;
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
        spark_inputs::register_recorded_inputs_with_slices(&session, header, batch_size)?;
        for case in &records[1..] {
            let actual =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await?;
            // The shared observer reports optimized Fields separately from its base match.
            let matches = actual["matches"] == true && actual["optimized_matches"] == true;
            let retained_name_contract =
                matches_retained_type_function_names(case, &actual, batch_size, &retained_names)?;
            let scoped_matches = if let Some(retained) = retained_name_contract {
                retained_contexts += 1;
                retained && !matches
            } else {
                matches
            };
            if !scoped_matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual}",
                    case["id"], case["ansi"]
                ));
            }
            eprintln!(
                "NUMERIC_CAST_FIELD_RESULT {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "domain":case["domain"],"reference":case,"observation":actual,
                    "all_stages_match":matches,"retained_1019_contract_match":retained_name_contract,
                    "scoped_796_and_retained_contract_match":scoped_matches})
            );
            comparisons += 1;
        }
    }
    assert_eq!(comparisons, 528);
    assert_eq!(retained_contexts, 4);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn floating_integer_cast_null_inspection_matches_recorded_contracts() -> TestResult {
    let records = include_str!("fixtures/numeric-cast-nullability/null-inspection-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let facets: Value = serde_json::from_str(include_str!(
        "fixtures/numeric-cast-nullability/null-inspection-retained-facets.json"
    ))?;
    assert_eq!(records.len() - 1, 12);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(
        facets["proof_head"],
        "8d317f38798ad9b6393e783b2529f6f2bfc2daa9"
    );
    assert_eq!(
        facets["spark_reference_sha256"],
        "d38a3a81ed7ba724785eb9415fc66964e86fcad65f6f5466289649d580c030eb"
    );
    assert_eq!(facets["contexts"].as_array().map(Vec::len), Some(8));
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((case["id"].to_string(), case["ansi"].to_string())));
    }
    let mut comparisons = 0;
    let mut strict_successes = 0;
    let mut required_overflow_contexts = 0;
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
        spark_inputs::register_recorded_inputs_with_slices(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await?;
            let strict = actual["matches"] == true && actual["optimized_matches"] == true;
            if case["expected"].get("rows").is_some() {
                assert!(
                    strict,
                    "{} ANSI={} batch={batch_size}: {actual}",
                    case["id"], case["ansi"]
                );
                strict_successes += 1;
            } else {
                assert_eq!(case["ansi"], true);
                assert!(strict, "{} batch={batch_size}: {actual}", case["id"]);
                assert!(
                    floating_integer_scoped_contract_matches(case, &actual, batch_size, &facets)?,
                    "{} batch={batch_size}: {actual}",
                    case["id"]
                );
                required_overflow_contexts += 1;
            }
            eprintln!(
                "FLOATING_INTEGER_NULL_INSPECTION_RESULT {}",
                json!({
                    "id":case["id"],"ansi":case["ansi"],"batch":batch_size,"reference":case,
                    "observation":actual,"strict_reference_match":strict,
                })
            );
            comparisons += 1;
        }
    }
    assert_eq!(comparisons, 24);
    assert_eq!(strict_successes, 16);
    assert_eq!(required_overflow_contexts, 8);
    Ok(())
}

#[tokio::test]
async fn floating_integer_conversion_evaluates_its_input_once() -> TestResult {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    for (name, source) in [
        ("count_float_input", ScalarValue::Float32(Some(1.9))),
        ("count_double_input", ScalarValue::Float64(Some(1.9))),
    ] {
        let observed_calls = Arc::clone(&calls);
        session.register_udf(create_udf(
            name,
            vec![],
            source.data_type(),
            Volatility::Volatile,
            Arc::new(move |_| {
                observed_calls.fetch_add(1, Ordering::SeqCst);
                Ok(ColumnarValue::Scalar(source.clone()))
            }),
        ));
        for ansi_mode in [false, true] {
            for function in ["CAST", "TRY_CAST"] {
                for target in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
                    let before = calls.load(Ordering::SeqCst);
                    let query = plan_query(
                        &session,
                        &format!("SELECT {function}({name}() AS {target}) AS r"),
                        &SparkSqlOptions {
                            ansi_mode,
                            ..SparkSqlOptions::default()
                        },
                    )
                    .await?;
                    let physical = query.create_physical_plan(&session).await?;
                    assert_eq!(calls.load(Ordering::SeqCst), before);
                    let batches = collect(physical, session.task_ctx()).await?;
                    assert_eq!(calls.load(Ordering::SeqCst), before + 1);
                    assert_eq!(
                        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                        1
                    );
                    let batch = batches
                        .iter()
                        .find(|batch| batch.num_rows() == 1)
                        .ok_or("missing result row")?;
                    assert_eq!(
                        ScalarValue::try_from_array(batch.column(0), 0)?
                            .cast_to(&DataType::Int64)?,
                        ScalarValue::Int64(Some(1))
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn ansi_numeric_integer_cast_overflow_keeps_condition_and_root_cause() -> TestResult {
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{DataFusionError, ScalarValue};
    use std::sync::Arc;

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for (name, source, value) in [
        ("float_nan", ScalarValue::Float32(Some(f32::NAN)), "NaN"),
        ("double_nan", ScalarValue::Float64(Some(f64::NAN)), "NaN"),
        (
            "float_inf",
            ScalarValue::Float32(Some(f32::INFINITY)),
            "inf",
        ),
        (
            "double_inf",
            ScalarValue::Float64(Some(f64::INFINITY)),
            "inf",
        ),
        (
            "float_neg_inf",
            ScalarValue::Float32(Some(f32::NEG_INFINITY)),
            "-inf",
        ),
        (
            "double_neg_inf",
            ScalarValue::Float64(Some(f64::NEG_INFINITY)),
            "-inf",
        ),
        (
            "float_finite_overflow",
            ScalarValue::Float32(Some(1e30)),
            "1e30",
        ),
        (
            "double_finite_overflow",
            ScalarValue::Float64(Some(1e100)),
            "1e100",
        ),
        (
            "bigint_overflow",
            ScalarValue::Int64(Some(4294967296)),
            "4294967296",
        ),
    ] {
        session.register_batch(
            name,
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "v",
                    source.data_type(),
                    false,
                )])),
                vec![source.to_array_of_size(1)?],
            )?,
        )?;
        for (target, data_type) in [
            ("TINYINT", DataType::Int8),
            ("SMALLINT", DataType::Int16),
            ("INT", DataType::Int32),
            ("BIGINT", DataType::Int64),
        ] {
            if source.data_type() == DataType::Int64 && data_type != DataType::Int32 {
                continue;
            }
            let sql = format!("SELECT CAST(v AS {target}) AS r FROM {name}");
            let query = plan_query(
                &session,
                &sql,
                &SparkSqlOptions {
                    ansi_mode: true,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let field = query.logical_plan.schema().field(0);
            assert_eq!(field.data_type(), &data_type, "{sql}");
            assert_eq!(
                field.is_nullable(),
                source.data_type() != DataType::Int64,
                "{sql}"
            );
            let physical = query.create_physical_plan(&session).await?;
            let error = collect(physical, session.task_ctx())
                .await
                .err()
                .ok_or("ANSI column CAST must overflow")?;
            assert!(
                matches!(error.find_root(), DataFusionError::ArrowError(_, _)),
                "{sql}: {error}"
            );
            assert_eq!(
                error.find_root().to_string(),
                format!("Arrow error: Cast error: Can't cast value {value} to type {data_type}"),
                "{sql}"
            );
            assert!(
                error.to_string().contains("[CAST_OVERFLOW]"),
                "{sql}: {error}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn floating_integer_casts_propagate_producer_errors() -> TestResult {
    use arrow::error::ArrowError;
    use datafusion::common::DataFusionError;
    use datafusion::logical_expr::{Volatility, create_udf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    for (name, source, typed_arrow) in [
        ("failing_float_producer", DataType::Float32, false),
        ("failing_double_producer", DataType::Float64, false),
        ("failing_float_arrow_producer", DataType::Float32, true),
        ("failing_double_arrow_producer", DataType::Float64, true),
    ] {
        let observed_calls = Arc::clone(&calls);
        session.register_udf(create_udf(
            name,
            vec![],
            source,
            Volatility::Volatile,
            Arc::new(move |_| {
                observed_calls.fetch_add(1, Ordering::SeqCst);
                let message = format!("{name} failed before conversion");
                Err(if typed_arrow {
                    DataFusionError::ArrowError(Box::new(ArrowError::CastError(message)), None)
                } else {
                    DataFusionError::Execution(message)
                })
            }),
        ));
        for ansi_mode in [true, false] {
            for function in ["CAST", "TRY_CAST"] {
                for target in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
                    let before = calls.load(Ordering::SeqCst);
                    let query = plan_query(
                        &session,
                        &format!("SELECT {function}({name}() AS {target}) AS r"),
                        &SparkSqlOptions {
                            ansi_mode,
                            ..SparkSqlOptions::default()
                        },
                    )
                    .await?;
                    let physical = query.create_physical_plan(&session).await?;
                    assert_eq!(calls.load(Ordering::SeqCst), before);
                    let error = collect(physical, session.task_ctx())
                        .await
                        .err()
                        .ok_or("numeric CAST swallowed a producer error")?;
                    if typed_arrow {
                        assert!(
                            matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
                            if matches!(cause.as_ref(), ArrowError::CastError(message)
                            if message == &format!("{name} failed before conversion")))
                        );
                    } else {
                        assert!(
                            matches!(error.find_root(), DataFusionError::Execution(message)
                            if message == &format!("{name} failed before conversion"))
                        );
                    }
                    assert!(!error.to_string().contains("[CAST_OVERFLOW]"));
                    assert_eq!(calls.load(Ordering::SeqCst), before + 1);
                }
            }
        }
    }
    Ok(())
}

// Compare only the exact accepted-runtime facets assigned to following slices.
fn floating_integer_scoped_contract_matches(
    case: &Value,
    actual: &Value,
    batch: usize,
    facets: &Value,
) -> TestResult<bool> {
    let Some(control) = facets["contexts"]
        .as_array()
        .ok_or("missing retained contexts")?
        .iter()
        .find(|control| {
            control["id"] == case["id"]
                && control["ansi"] == case["ansi"]
                && control["batch"] == batch
        })
    else {
        return Ok(actual["matches"] == true && actual["optimized_matches"] == true);
    };
    if control["sql"] != case["sql"] {
        return Ok(false);
    }
    let keys = [
        "public_schema",
        "before_collect",
        "physical_schema",
        "rows",
        "rows_raw",
        "native_phase",
        "error",
        "public_matches",
        "before_matches",
        "physical_matches",
        "rows_match",
        "ordered_rows_match",
        "error_matches",
        "matches",
        "optimized_matches",
        "optimized_schema",
        "optimized_observation",
        "optimized_error",
    ];
    if !actual.as_object().is_some_and(|object| {
        object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key))
    }) {
        return Ok(false);
    }
    let mut normalized = actual.clone();
    // #1031 retains only these exact later substring failures and extra native Fields.
    let later_substring_error = case["ansi"] == true
        && matches!(
            case["id"].as_str(),
            Some("float_nan_substring" | "double_infinity_substring")
        );
    for kind in ["fields", "availability", "rows"] {
        for facet in control[kind].as_array().ok_or("missing retained facets")? {
            let key = facet["key"].as_str().ok_or("missing observed key")?;
            let owner = facet["owner"].as_u64().ok_or("missing facet owner")?;
            if kind == "availability" {
                assert!(matches!(owner, 922 | 1020));
                let mut expected_native = facet["inherited_dar"].clone();
                if owner == 922 && later_substring_error {
                    assert!(matches!(
                        key,
                        "optimized_schema" | "before_collect" | "physical_schema"
                    ));
                    assert_eq!(
                        expected_native,
                        json!({"type":"struct","fields":[{
                            "name":"r","type":"string","nullable":false,"metadata":{}
                        }]})
                    );
                    // Spark has no later schema here; retain the native boundary with the repaired declaration.
                    expected_native["fields"][0]["nullable"] = json!(true);
                }
                if normalized[key] != expected_native {
                    return Ok(false);
                }
                normalized[key] = facet["spark"].clone();
                continue;
            }
            let pointer = facet["json_pointer"]
                .as_str()
                .ok_or("missing facet pointer")?;
            if kind == "fields" {
                assert!(matches!(owner, 796 | 1020));
                assert!(pointer.ends_with("/nullable"));
                // The old796 leaves remain history; the final schema comparison now requires Spark.
                if owner == 796 {
                    continue;
                }
            } else {
                assert_eq!(owner, 1020);
                assert!(matches!(key, "rows" | "rows_raw"));
                assert!(
                    case["sql"]
                        .as_str()
                        .is_some_and(|sql| sql.contains("TRY_CAST"))
                );
                let proof = &facets["endpoint_baseline_proofs"][facet["baseline_proof"]
                    .as_u64()
                    .ok_or("missing endpoint proof")?
                    as usize];
                assert_eq!(proof["source_type"], case["source_type"]);
                assert_eq!(proof["target_type"], case["target_type"]);
                assert_eq!(proof["ansi"], case["ansi"]);
                assert_eq!(proof["batch"], batch);
                assert_eq!(proof["inherited_dar"], facet["inherited_dar"]);
                assert_eq!(proof["spark"], facet["spark"]);
            }
            let Some(value) = normalized[key].pointer_mut(pointer) else {
                return Ok(false);
            };
            if *value != facet["inherited_dar"] && *value != facet["spark"] {
                return Ok(false);
            }
            *value = facet["spark"].clone();
        }
    }
    let retained_error = &control["error"];
    if !retained_error.is_null() {
        assert!(matches!(
            retained_error["owner"].as_u64(),
            Some(865 | 922 | 1020)
        ));
        let mut expected_error = retained_error.clone();
        if matches!(retained_error["owner"].as_u64(), Some(922 | 1020)) {
            for key in ["error", "optimized_error"] {
                if let Some(error) = retained_error[key].as_str() {
                    assert!(!error.contains("[CAST_OVERFLOW]"));
                    assert_eq!(error.matches("Arrow error: Cast error: ").count(), 1);
                    expected_error[key] = json!(error.replacen(
                        "Arrow error: Cast error: ",
                        "[CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: ",
                        1,
                    ));
                }
            }
        }
        if later_substring_error {
            assert_eq!(retained_error["owner"], 922);
            assert_eq!(retained_error["native_phase"], "collect");
            assert_eq!(case["spark_error_phase"], "optimized");
        }
        let error_matches = retained_error["owner"] == 922 && !later_substring_error;
        if ["error", "native_phase", "optimized_error"]
            .into_iter()
            .any(|key| actual[key] != expected_error[key])
            || !actual["rows"].is_null()
            || !actual["rows_raw"].is_null()
            || actual["error_matches"] != error_matches
        {
            return Ok(false);
        }
    } else if !actual["error"].is_null()
        || !actual["optimized_error"].is_null()
        || actual["native_phase"] != "encode"
        || actual["error_matches"] != true
    {
        return Ok(false);
    }
    for (stage, key, flag) in [
        ("analyzed", "public_schema", "public_matches"),
        ("optimized", "optimized_schema", "optimized_matches"),
        (
            "executed_before_collect",
            "before_collect",
            "before_matches",
        ),
        ("executed", "physical_schema", "physical_matches"),
    ] {
        let expected = &case["schema_stages"][stage];
        if normalized[key] != *expected {
            return Ok(false);
        }
        if stage != "optimized" {
            let raw_match = expected.is_null() || actual[key] == *expected;
            if actual[flag] != raw_match {
                return Ok(false);
            }
        }
    }
    if actual["optimized_observation"].is_null() {
        if actual["optimized_matches"] != actual["error_matches"] || retained_error.is_null() {
            return Ok(false);
        }
    } else {
        let optimized = &actual["optimized_observation"];
        if !optimized.as_object().is_some_and(|object| {
            object.len() == 3
                && ["raw_schema", "schema", "names_match"]
                    .iter()
                    .all(|key| object.contains_key(*key))
        }) || optimized["names_match"] != true
            || optimized["schema"] != actual["optimized_schema"]
            || actual["optimized_matches"]
                != (optimized["schema"] == case["schema_stages"]["optimized"])
        {
            return Ok(false);
        }
        let mut raw = optimized["raw_schema"].clone();
        let Some(raw_fields) = raw["fields"].as_array_mut() else {
            return Ok(false);
        };
        let mapped_fields = actual["optimized_schema"]["fields"]
            .as_array()
            .ok_or("missing optimized fields")?;
        if raw_fields.len() != mapped_fields.len() {
            return Ok(false);
        }
        for (raw, mapped) in raw_fields.iter_mut().zip(mapped_fields) {
            raw["name"] = mapped["name"].clone();
        }
        if raw != actual["optimized_schema"] {
            return Ok(false);
        }
    }
    let expected_rows = case["expected"].get("rows");
    let mut ordered_expected = expected_rows.cloned().unwrap_or(Value::Null);
    if case["ordered"] == true
        && let Some(rows) = ordered_expected.as_array_mut()
    {
        rows.sort_by_key(Value::to_string);
    }
    let rows_match = expected_rows.is_none() || actual["rows"] == ordered_expected;
    let order_matches = expected_rows.is_none()
        || case["ordered"] != true
        || Some(&actual["rows_raw"]) == expected_rows;
    if actual["rows_match"] != rows_match || actual["ordered_rows_match"] != order_matches {
        return Ok(false);
    }
    let base_matches = [
        "public_matches",
        "before_matches",
        "physical_matches",
        "rows_match",
        "ordered_rows_match",
        "error_matches",
    ]
    .into_iter()
    .all(|key| actual[key] == true);
    if actual["matches"] != base_matches {
        return Ok(false);
    }
    if retained_error["owner"] == 1020 {
        // The four checked +2^63 literals retain their exact early failure until #1020.
        return Ok(true);
    }
    let mut raw_rows = normalized["rows_raw"].clone();
    if let Some(rows) = raw_rows.as_array_mut() {
        rows.sort_by_key(Value::to_string);
    }
    Ok(normalized["rows"] == ordered_expected
        && raw_rows == ordered_expected
        && (expected_rows.is_none()
            || case["ordered"] != true
            || Some(&normalized["rows_raw"]) == expected_rows))
}

#[test]
fn floating_integer_retained_facets_reject_unowned_changes() -> TestResult {
    let records = include_str!("fixtures/floating-integer-values/spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let facets: Value = serde_json::from_str(include_str!(
        "fixtures/floating-integer-values/retained-baseline-facets.json"
    ))?;
    let case = records
        .iter()
        .find(|case| {
            case["id"] == "float_int_floating_integer_required_cast" && case["ansi"] == false
        })
        .ok_or("missing Legacy provider control")?;
    let control = facets["contexts"]
        .as_array()
        .ok_or("missing retained contexts")?
        .iter()
        .find(|control| {
            control["id"] == case["id"] && control["ansi"] == case["ansi"] && control["batch"] == 1
        })
        .ok_or("missing exact Field control")?;
    let mut rows = case["expected"]["rows"]
        .as_array()
        .ok_or("missing rows")?
        .clone();
    rows.sort_by_key(Value::to_string);
    let mut actual = json!({
        "public_schema":case["schema_stages"]["analyzed"],
        "optimized_schema":case["schema_stages"]["optimized"],
        "before_collect":case["schema_stages"]["executed_before_collect"],
        "physical_schema":case["schema_stages"]["executed"],
        "rows":rows,"rows_raw":case["expected"]["rows"],
        "native_phase":"encode","error":null,"optimized_error":null,
        "public_matches":false,"before_matches":false,"physical_matches":false,
        "rows_match":true,"ordered_rows_match":true,"error_matches":true,
        "matches":false,"optimized_matches":false,"optimized_observation":null,
    });
    for field in control["fields"].as_array().ok_or("missing Field facets")? {
        let key = field["key"].as_str().ok_or("missing Field key")?;
        *actual[key]
            .pointer_mut(field["json_pointer"].as_str().ok_or("missing pointer")?)
            .ok_or("missing observed Field")? = field["inherited_dar"].clone();
    }
    actual["optimized_observation"] = json!({
        "raw_schema":actual["optimized_schema"],"schema":actual["optimized_schema"],
        "names_match":true,
    });
    assert!(!floating_integer_scoped_contract_matches(
        case, &actual, 1, &facets
    )?);
    let inherited = actual.clone();
    for (key, stage) in [
        ("public_schema", "analyzed"),
        ("optimized_schema", "optimized"),
        ("before_collect", "executed_before_collect"),
        ("physical_schema", "executed"),
    ] {
        actual[key] = case["schema_stages"][stage].clone();
    }
    for flag in [
        "public_matches",
        "optimized_matches",
        "before_matches",
        "physical_matches",
        "matches",
    ] {
        actual[flag] = json!(true);
    }
    actual["optimized_observation"] = json!({
        "raw_schema":actual["optimized_schema"],"schema":actual["optimized_schema"],"names_match":true,
    });
    assert!(floating_integer_scoped_contract_matches(
        case, &actual, 1, &facets
    )?);
    for (pointer, value) in [
        ("/public_schema/fields/0/name", json!("wrong")),
        ("/public_schema/fields/0/type", json!("long")),
        (
            "/public_schema/fields/0/metadata",
            json!({"wrong":"metadata"}),
        ),
        ("/public_schema/fields/0/nullable", json!(true)),
        ("/public_schema/fields/1/nullable", Value::Null),
        ("/before_collect", Value::Null),
        ("/native_phase", json!("collect")),
        ("/optimized_error", json!("unrelated error")),
        ("/matches", json!(false)),
    ] {
        let mut changed = actual.clone();
        *changed
            .pointer_mut(pointer)
            .ok_or("missing mutation pointer")? = value;
        assert!(
            !floating_integer_scoped_contract_matches(case, &changed, 1, &facets)?,
            "accepted unowned change at {pointer}"
        );
    }
    let mut changed = actual.clone();
    changed["rows_raw"][0][1] = json!("999");
    let mut changed_rows = changed["rows_raw"]
        .as_array()
        .ok_or("missing changed rows")?
        .clone();
    changed_rows.sort_by_key(Value::to_string);
    changed["rows"] = json!(changed_rows);
    changed["rows_match"] = json!(false);
    changed["ordered_rows_match"] = json!(false);
    changed["matches"] = json!(false);
    assert!(
        !floating_integer_scoped_contract_matches(case, &changed, 1, &facets)?,
        "accepted changed Legacy values with coherent comparison flags"
    );
    let mut changed = actual.clone();
    changed["unexpected"] = json!(true);
    assert!(!floating_integer_scoped_contract_matches(
        case, &changed, 1, &facets
    )?);
    assert!(!floating_integer_scoped_contract_matches(
        case, &inherited, 2, &facets
    )?);
    for (key, value) in [
        ("sql", json!("SELECT 99")),
        ("id", json!("unlisted")),
        ("ansi", json!(true)),
    ] {
        let mut changed = case.clone();
        changed[key] = value;
        assert!(!floating_integer_scoped_contract_matches(
            &changed, &inherited, 1, &facets
        )?);
    }

    for id in ["float_tinyint_literal_nan", "float_bigint_literal_bigint_upper_cutoff"] {
        let case = records
            .iter()
            .find(|case| case["id"] == id && case["ansi"] == true)
            .ok_or("missing checked cause control")?;
        let mut error = facets["contexts"]
            .as_array()
            .ok_or("missing retained contexts")?
            .iter()
            .find(|control| {
                control["id"] == case["id"] && control["ansi"] == case["ansi"] && control["batch"] == 1
            })
            .ok_or("missing exact cause")?["error"]
            .clone();
        for key in ["error", "optimized_error"] {
            error[key] = json!(error[key].as_str().ok_or("missing checked error")?
                .replacen("Arrow error: ", "[CAST_OVERFLOW]\ncaused by\nArrow error: ", 1));
        }
        let condition_matches = error["owner"] == 922;
        let actual = json!({
            "public_schema":case["schema_stages"]["analyzed"],
            "optimized_schema":null,"before_collect":null,"physical_schema":null,
            "rows":null,"rows_raw":null,"native_phase":error["native_phase"],
            "error":error["error"],"optimized_error":error["optimized_error"],
            "public_matches":true,
            "before_matches":case["schema_stages"]["executed_before_collect"].is_null(),
            "physical_matches":case["schema_stages"]["executed"].is_null(),
            "rows_match":case["expected"].get("rows").is_none(),"ordered_rows_match":true,
            "error_matches":condition_matches,"matches":condition_matches,
            "optimized_matches":condition_matches,"optimized_observation":null,
        });
        assert!(floating_integer_scoped_contract_matches(case, &actual, 1, &facets)?);
        for key in ["error", "optimized_error", "native_phase"] {
            let mut changed = actual.clone();
            changed[key] = json!("[CAST_OVERFLOW] a different cause or stage");
            assert!(!floating_integer_scoped_contract_matches(case, &changed, 1, &facets)?);
        }
        let mut missing_marker = actual.clone();
        for key in ["error", "optimized_error"] {
            missing_marker[key] = json!(actual[key].as_str().ok_or("missing checked error")?
                .replace("[CAST_OVERFLOW]", ""));
        }
        assert!(!floating_integer_scoped_contract_matches(case, &missing_marker, 1, &facets)?);
        let mut wrong_flags = actual.clone();
        for key in ["error_matches", "matches", "optimized_matches"] {
            wrong_flags[key] = json!(!condition_matches);
        }
        assert!(!floating_integer_scoped_contract_matches(case, &wrong_flags, 1, &facets)?);
    }

    let case = records
        .iter()
        .find(|case| case["id"] == "double_infinity_substring" && case["ansi"] == true)
        .ok_or("missing later native schema control")?;
    let control = facets["contexts"]
        .as_array()
        .ok_or("missing retained contexts")?
        .iter()
        .find(|control| {
            control["id"] == case["id"] && control["ansi"] == case["ansi"] && control["batch"] == 1
        })
        .ok_or("missing exact native schema boundary")?;
    let error = &control["error"];
    let mut actual = json!({
        "public_schema":case["schema_stages"]["analyzed"],
        "optimized_schema":null,"before_collect":null,"physical_schema":null,
        "rows":null,"rows_raw":null,"native_phase":error["native_phase"],
        "error":format!("[CAST_OVERFLOW]\ncaused by\n{}", error["error"].as_str().ok_or("missing later cause")?),
        "optimized_error":error["optimized_error"],
        "public_matches":true,"before_matches":true,"physical_matches":true,
        "rows_match":true,"ordered_rows_match":true,"error_matches":false,
        "matches":false,"optimized_matches":false,"optimized_observation":null,
    });
    for facet in control["availability"]
        .as_array()
        .ok_or("missing stage controls")?
    {
        let key = facet["key"].as_str().ok_or("missing native schema key")?;
        actual[key] = facet["inherited_dar"].clone();
        actual[key]["fields"][0]["nullable"] = json!(true);
    }
    actual["optimized_observation"] = json!({
        "raw_schema":actual["optimized_schema"],"schema":actual["optimized_schema"],"names_match":true,
    });
    assert!(floating_integer_scoped_contract_matches(
        case, &actual, 1, &facets
    )?);
    let mut missing_marker = actual.clone();
    missing_marker["error"] = json!(actual["error"].as_str().ok_or("missing later cause")?
        .replace("[CAST_OVERFLOW]", ""));
    assert!(!floating_integer_scoped_contract_matches(case, &missing_marker, 1, &facets)?);
    for (pointer, wrong) in [
        ("/physical_schema/fields/0/nullable", json!(false)),
        ("/physical_schema/fields/0/name", json!("wrong")),
        ("/physical_schema/fields/0/type", json!("integer")),
        (
            "/physical_schema/fields/0/metadata",
            json!({"wrong":"metadata"}),
        ),
        ("/before_collect", Value::Null),
        ("/native_phase", json!("create_physical_plan")),
        ("/error", json!("[CAST_OVERFLOW] unrelated cause")),
    ] {
        let mut changed = actual.clone();
        *changed
            .pointer_mut(pointer)
            .ok_or("missing stage mutation")? = wrong;
        assert!(
            !floating_integer_scoped_contract_matches(case, &changed, 1, &facets)?,
            "accepted changed later native schema boundary at {pointer}"
        );
    }

    let case = records
        .iter()
        .find(|case| {
            case["id"] == "float_bigint_floating_integer_nullable_sliced_try_cast"
                && case["ansi"] == false
        })
        .ok_or("missing ordered TRY endpoint control")?;
    assert_eq!(case["ordered"], true);
    let control = facets["contexts"]
        .as_array()
        .ok_or("missing retained contexts")?
        .iter()
        .find(|control| {
            control["id"] == case["id"] && control["ansi"] == case["ansi"] && control["batch"] == 1
        })
        .ok_or("missing ordered endpoint facets")?;
    assert_eq!(control["fields"], json!([]));
    let mut rows = case["expected"]["rows"]
        .as_array()
        .ok_or("missing TRY rows")?
        .clone();
    rows.sort_by_key(Value::to_string);
    let mut actual = json!({
        "public_schema":case["schema_stages"]["analyzed"],
        "optimized_schema":case["schema_stages"]["optimized"],
        "before_collect":case["schema_stages"]["executed_before_collect"],
        "physical_schema":case["schema_stages"]["executed"],
        "rows":rows,"rows_raw":case["expected"]["rows"],
        "native_phase":"encode","error":null,"optimized_error":null,
        "public_matches":true,"before_matches":true,"physical_matches":true,
        "rows_match":false,"ordered_rows_match":false,"error_matches":true,
        "matches":false,"optimized_matches":true,"optimized_observation":null,
    });
    for facet in control["rows"]
        .as_array()
        .ok_or("missing ordered endpoint rows")?
    {
        let key = facet["key"].as_str().ok_or("missing row key")?;
        *actual[key]
            .pointer_mut(
                facet["json_pointer"]
                    .as_str()
                    .ok_or("missing row pointer")?,
            )
            .ok_or("missing ordered endpoint cell")? = facet["inherited_dar"].clone();
    }
    actual["optimized_observation"] = json!({
        "raw_schema":actual["optimized_schema"],"schema":actual["optimized_schema"],
        "names_match":true,
    });
    assert!(floating_integer_scoped_contract_matches(
        case, &actual, 1, &facets
    )?);
    let mut changed = actual.clone();
    for facet in control["rows"]
        .as_array()
        .ok_or("missing ordered endpoint rows")?
    {
        let key = facet["key"].as_str().ok_or("missing row key")?;
        *changed[key]
            .pointer_mut(
                facet["json_pointer"]
                    .as_str()
                    .ok_or("missing row pointer")?,
            )
            .ok_or("missing ordered endpoint cell")? = json!("123");
    }
    assert!(
        !floating_integer_scoped_contract_matches(case, &changed, 1, &facets)?,
        "accepted an arbitrary endpoint value with coherent comparison flags"
    );
    let mut changed = actual.clone();
    changed["rows_raw"][0][1] = json!("123");
    let mut changed_rows = changed["rows_raw"]
        .as_array()
        .ok_or("missing changed TRY rows")?
        .clone();
    changed_rows.sort_by_key(Value::to_string);
    changed["rows"] = json!(changed_rows);
    assert!(
        !floating_integer_scoped_contract_matches(case, &changed, 1, &facets)?,
        "accepted an unlisted TRY row with coherent comparison flags"
    );

    let case = records
        .iter()
        .find(|case| case["id"] == "float_bigint_literal_try_matrix" && case["ansi"] == false)
        .ok_or("missing TRY endpoint control")?;
    let control = facets["contexts"]
        .as_array()
        .ok_or("missing retained contexts")?
        .iter()
        .find(|control| {
            control["id"] == case["id"] && control["ansi"] == case["ansi"] && control["batch"] == 1
        })
        .ok_or("missing endpoint facets")?;
    let mut actual = json!({
        "public_schema":case["schema_stages"]["analyzed"],
        "optimized_schema":case["schema_stages"]["optimized"],
        "before_collect":case["schema_stages"]["executed_before_collect"],
        "physical_schema":case["schema_stages"]["executed"],
        "rows":case["expected"]["rows"],"rows_raw":case["expected"]["rows"],
        "native_phase":"encode","error":null,"optimized_error":null,
        "public_matches":true,"before_matches":false,"physical_matches":false,
        "rows_match":false,"ordered_rows_match":true,"error_matches":true,
        "matches":false,"optimized_matches":false,"optimized_observation":null,
    });
    for kind in ["fields", "rows"] {
        for facet in control[kind].as_array().ok_or("missing endpoint facet")? {
            let key = facet["key"].as_str().ok_or("missing facet key")?;
            *actual[key]
                .pointer_mut(facet["json_pointer"].as_str().ok_or("missing pointer")?)
                .ok_or("missing endpoint cell")? = facet["inherited_dar"].clone();
        }
    }
    actual["optimized_observation"] = json!({
        "raw_schema":actual["optimized_schema"],"schema":actual["optimized_schema"],
        "names_match":true,
    });
    assert!(floating_integer_scoped_contract_matches(
        case, &actual, 1, &facets
    )?);
    for pointer in ["/rows/0/0", "/rows/0/4", "/rows_raw/0/0", "/rows_raw/0/4"] {
        let mut changed = actual.clone();
        *changed
            .pointer_mut(pointer)
            .ok_or("missing endpoint mutation")? = json!("123");
        assert!(
            !floating_integer_scoped_contract_matches(case, &changed, 1, &facets)?,
            "accepted an unproved endpoint value at {pointer}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn floating_integer_casts_match_recorded_stages() -> TestResult {
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/floating-integer-values/sources/897-floating-integer-inputs-current-v1-manifest.json"
    ))?;
    let records = include_str!("fixtures/floating-integer-values/spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 352);
    let header = &records[0];
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(header["timezone"], "UTC");
    assert_eq!(header["inputs"], manifest["inputs"]);
    assert_eq!(header["native"], manifest["native"]);
    assert_eq!(
        header["manifest_sha256"],
        "49a514c85966cebdaf2ff9b574bd4032e153b61c678af30565845332819b3c2e"
    );
    let definitions = manifest["cases"].as_array().ok_or("missing cases")?;
    let mut expected_ids = BTreeSet::new();
    for case in definitions {
        for ansi in [true, false] {
            expected_ids.insert((case["id"].to_string(), ansi));
        }
    }
    let mut actual_ids = BTreeSet::new();
    for case in &records[1..] {
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(actual_ids.insert((case["id"].to_string(), ansi)));
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("undeclared case")?;
        for (key, value) in definition.as_object().ok_or("invalid definition")? {
            if key != "ansi_modes" {
                assert_eq!(&case[key], value, "{key}: {}", case["id"]);
            }
        }
        assert_eq!(case["settings"]["spark.sql.ansi.enabled"], ansi.to_string());
        if case["expected"].get("rows").is_some() {
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"][stage].is_object(), "{stage}: {case}");
            }
        } else {
            assert!(case["spark_error_details"]["parameters"].is_object());
        }
    }
    assert_eq!(actual_ids, expected_ids);

    let facets: Value = serde_json::from_str(include_str!(
        "fixtures/floating-integer-values/retained-baseline-facets.json"
    ))?;
    assert_eq!(
        facets["baseline_head"],
        "7bfcde8f3a1ee2f1669f485dcad9994e5297a99a"
    );
    assert_eq!(
        facets["spark_reference_sha256"],
        "e8945dc1ab19cd6148fc506eec6381c91c9f98a9cfe9c1407b79740420618857"
    );
    let contexts = facets["contexts"]
        .as_array()
        .ok_or("missing retained contexts")?;
    assert_eq!(contexts.len(), 260);
    let mut historical_keys = BTreeSet::new();
    let mut retained_keys = BTreeSet::new();
    for control in contexts {
        let key = (
            control["id"].to_string(),
            control["ansi"].to_string(),
            control["batch"].as_u64().ok_or("missing batch")?,
        );
        assert!(historical_keys.insert(key.clone()));
        if (!control["error"].is_null() && control["error"]["owner"] != 922)
            || !control["availability"]
                .as_array()
                .ok_or("missing availability")?
                .is_empty()
            || !control["rows"]
                .as_array()
                .ok_or("missing retained rows")?
                .is_empty()
            || control["fields"]
                .as_array()
                .ok_or("missing retained fields")?
                .iter()
                .any(|field| field["owner"] != 796)
        {
            assert!(retained_keys.insert(key));
        }
        let case = records[1..]
            .iter()
            .find(|case| case["id"] == control["id"] && case["ansi"] == control["ansi"])
            .ok_or("retained context has no original reference")?;
        assert_eq!(control["sql"], case["sql"]);
        for row in control["rows"].as_array().ok_or("missing endpoint rows")? {
            let proof = &facets["endpoint_baseline_proofs"][row["baseline_proof"]
                .as_u64()
                .ok_or("missing endpoint proof")?
                as usize];
            let source = records[1..]
                .iter()
                .find(|record| record["id"] == proof["id"] && record["ansi"] == proof["ansi"])
                .ok_or("missing independent endpoint source")?;
            assert_eq!(source["sql"], proof["sql"]);
            assert_eq!(source["source_type"], case["source_type"]);
            assert_eq!(source["target_type"], "BIGINT");
            assert_eq!(source["expected"]["rows"][0][4], proof["spark"]);
            assert_eq!(proof["json_pointer"], "/0/4");
            assert!(proof["inherited_dar"].is_null());
            let role = manifest["value_roles"]
                .as_array()
                .ok_or("missing IEEE roles")?
                .iter()
                .find(|role| role["name"] == "bigint_upper_cutoff")
                .ok_or("missing endpoint bits")?;
            let column = if case["source_type"] == "FLOAT" {
                "f"
            } else {
                "d"
            };
            assert_eq!(proof["source_bits"], role[column]);
            if row["input_witness"].is_null() {
                assert_eq!(case["sql"], source["sql"]);
            } else {
                let witness = &row["input_witness"];
                assert_eq!(witness["column"], column);
                assert_eq!(witness["source_bits"], proof["source_bits"]);
                let table = manifest["inputs"]
                    .as_array()
                    .ok_or("missing inputs")?
                    .iter()
                    .find(|table| table["name"] == witness["table"])
                    .ok_or("missing endpoint provider")?;
                assert!(
                    case["sql"]
                        .as_str()
                        .ok_or("missing SQL")?
                        .contains(&format!(
                            "FROM {} ORDER BY",
                            table["name"].as_str().ok_or("missing table name")?
                        ))
                );
                let input = table["rows"]
                    .as_array()
                    .ok_or("missing input rows")?
                    .iter()
                    .find(|input| input[0] == witness["id"])
                    .ok_or("missing endpoint input")?;
                assert_eq!(
                    input[if column == "f" { 1 } else { 2 }],
                    proof["source_bits"]
                );
                let row_index = row["json_pointer"]
                    .as_str()
                    .ok_or("missing row pointer")?
                    .split('/')
                    .nth(1)
                    .ok_or("missing row index")?
                    .parse::<usize>()?;
                let mut expected = case["expected"]["rows"]
                    .as_array()
                    .ok_or("missing rows")?
                    .clone();
                if row["key"] == "rows" {
                    expected.sort_by_key(Value::to_string);
                }
                assert_eq!(expected[row_index][0], witness["id"]);
                assert_eq!(expected[row_index][1], proof["spark"]);
            }
        }
    }
    let mut differences = Vec::new();
    let mut comparisons = 0;
    let mut strict_matches = 0;
    let mut retained_contexts = 0;
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
        spark_inputs::register_recorded_inputs_with_slices(&session, header, batch_size)?;
        for case in &records[1..] {
            let actual =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await?;
            let matches = actual["matches"] == true && actual["optimized_matches"] == true;
            let scoped_matches =
                floating_integer_scoped_contract_matches(case, &actual, batch_size, &facets)?;
            if !scoped_matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual}",
                    case["id"], case["ansi"]
                ));
            }
            eprintln!(
                "FLOATING_INTEGER_VALUE_RESULT {}",
                json!({
                    "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "domain":case["domain"],"reference":case,"observation":actual,
                    "strict_reference_match":matches,
                    "owned_897_and_retained_facets_match":scoped_matches
                })
            );
            strict_matches += usize::from(matches);
            retained_contexts += usize::from(retained_keys.contains(&(
                case["id"].to_string(),
                case["ansi"].to_string(),
                batch_size as u64,
            )));
            comparisons += 1;
        }
    }
    assert_eq!(comparisons, 704);
    assert_eq!(historical_keys.len(), 260);
    assert_eq!(retained_contexts, 36);
    assert_eq!(strict_matches, 668);
    eprintln!(
        "FLOATING_INTEGER_VALUE_SUMMARY {}",
        json!({
            "comparisons":comparisons,"strict_reference_matches":strict_matches,
            "historical_contexts":historical_keys.len(),"retained_contexts":retained_contexts,
            "unexpected_differences":differences.len(),
            "full_compatibility_pass":strict_matches==comparisons,
        })
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn floating_casts_retain_spark_schemas_and_required_errors() -> TestResult {
    let records = include_str!("fixtures/floating-cast-nullability-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 172);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
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
        for case in &records[1..] {
            let actual = capture_floating_cast_result(&session, case)
                .await
                .unwrap_or_else(|error| {
                    let message = error.to_string();
                    if message.contains("[CAST_INVALID_INPUT] Cannot cast string") {
                        json!({"condition":"CAST_INVALID_INPUT"})
                    } else {
                        json!({"unexpected_error":message})
                    }
                });
            if actual != case["expected"] {
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

async fn capture_floating_cast_result(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    let fields = query
        .logical_plan
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let physical = schema.field(index);
            assert_eq!(physical.name(), &query.column_names[index]);
            assert_eq!(physical.data_type(), field.data_type());
            assert_eq!(physical.metadata(), field.metadata());
            // Optimizers may narrow nullability after folding constants or unreachable branches.
            assert!(!physical.is_nullable() || field.is_nullable());
            let data_type = match field.data_type() {
                DataType::Int32 => "integer",
                DataType::Int64 => "long",
                DataType::Boolean => "boolean",
                DataType::Float32 => "float",
                DataType::Float64 => "double",
                DataType::Utf8 => "string",
                other => return Err(format!("unexpected result type: {other}").into()),
            };
            Ok(json!({"name":physical.name(),"type":data_type,
                "nullable":field.is_nullable(),"metadata":field.metadata()}))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for (index, column) in batch.columns().iter().enumerate() {
            assert!(schema.field(index).is_nullable() || column.null_count() == 0);
        }
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        Ok(if column.is_null(row) {
                            Value::Null
                        } else {
                            json!(match column.data_type() {
                                DataType::Float32 => format!(
                                    "{:08x}",
                                    column.as_primitive::<Float32Type>().value(row).to_bits()
                                ),
                                DataType::Float64 => format!(
                                    "{:016x}",
                                    column.as_primitive::<Float64Type>().value(row).to_bits()
                                ),
                                _ => array_value_to_string(column.as_ref(), row)?,
                            })
                        })
                    })
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({"schema":{"type":"struct","fields":fields},"rows":rows}))
}

#[tokio::test]
async fn native_null_safe_comparisons_remain_nonnullable() -> TestResult {
    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::logical_expr::{ExprSchemable, Operator, binary_expr, lit};

    for operator in [Operator::IsDistinctFrom, Operator::IsNotDistinctFrom] {
        let expression = binary_expr(lit(ScalarValue::Int32(None)), operator, lit(1_i32));
        assert!(!expression.nullable(&DFSchema::empty())?);
        assert!(!expression.to_field(&DFSchema::empty())?.1.is_nullable());
    }
    let session = SessionContext::new();
    let query = session
        .sql(
            "SELECT a IS DISTINCT FROM b AS different,a IS NOT DISTINCT FROM b AS same \
         FROM (VALUES(1,1),(1,2),(NULL,1),(1,NULL),(NULL,NULL)) t(a,b)",
        )
        .await?;
    assert!(
        query
            .schema()
            .fields()
            .iter()
            .all(|field| !field.is_nullable())
    );
    let batches = query.collect().await?;
    let mut rows = Vec::new();
    for batch in batches {
        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .all(|field| !field.is_nullable())
        );
        for column in batch.columns() {
            assert_eq!(column.null_count(), 0);
        }
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| column.as_boolean().value(row))
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![false, true],
            vec![false, true],
            vec![true, false],
            vec![true, false],
            vec![true, false]
        ]
    );
    Ok(())
}

#[tokio::test]
async fn configured_session_retains_native_cast_behavior() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let mut controls = vec![
        ("SELECT CAST('1.5' AS DOUBLE) AS r", false),
        ("SELECT TRY_CAST('bad' AS FLOAT) AS r", false),
        ("SELECT CAST(NULL AS FLOAT) AS r", false),
        ("SELECT CAST(CAST(4294967296 AS BIGINT) AS INT) AS r", true),
    ]
    .into_iter()
    .map(|(sql, fails)| (sql.to_owned(), fails))
    .collect::<Vec<_>>();
    for source in ["FLOAT", "DOUBLE"] {
        for cast in ["CAST", "TRY_CAST"] {
            for value in ["1.9", "'NaN'", "'Infinity'"] {
                if cast == "CAST" && value != "1.9" {
                    for target in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
                        controls.push((
                            format!("SELECT CAST(CAST({value} AS {source}) AS {target}) AS r"),
                            true,
                        ));
                    }
                    continue;
                }
                let outputs = ["TINYINT", "SMALLINT", "INT", "BIGINT"]
                    .into_iter()
                    .enumerate()
                    .map(|(index, target)| {
                        format!("{cast}(CAST({value} AS {source}) AS {target}) AS r{index}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                controls.push((format!("SELECT {outputs}"), false));
            }
        }
    }
    let mut before = Vec::new();
    for (sql, fails) in &controls {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        if *fails {
            for observation in [&expected, &actual] {
                assert!(
                    observation["error"]
                        .as_str()
                        .is_some_and(|error| !error.contains("[CAST_OVERFLOW]")),
                    "before Spark planning: {sql}: {observation}"
                );
            }
        }
        assert_eq!(actual, expected, "before Spark planning: {sql}");
        before.push(actual);
    }
    for ansi_mode in [true, false] {
        for (sql, _) in &controls {
            if sql.contains("1.9") || sql.contains("TRY_CAST(CAST('NaN'") {
                let query = plan_query(
                    &configured,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&configured).await?;
                let batches = collect(physical, configured.task_ctx()).await?;
                assert_eq!(
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    1
                );
            }
        }
    }
    for ((sql, fails), before) in controls.iter().zip(before) {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        if *fails {
            for observation in [&expected, &actual] {
                assert!(
                    observation["error"]
                        .as_str()
                        .is_some_and(|error| !error.contains("[CAST_OVERFLOW]")),
                    "after Spark planning: {sql}: {observation}"
                );
            }
        }
        assert_eq!(actual, expected, "after Spark planning: {sql}");
        assert_eq!(
            actual, before,
            "native behavior changed after Spark planning: {sql}"
        );
    }
    Ok(())
}
