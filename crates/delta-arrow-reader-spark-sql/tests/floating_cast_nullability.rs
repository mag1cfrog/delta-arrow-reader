//! Floating conversion stages, values and required errors retain their recorded contracts.

use std::collections::BTreeSet;
use std::error::Error;

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
async fn floating_try_cast_propagates_producer_errors() -> TestResult {
    use datafusion::common::DataFusionError;
    use datafusion::logical_expr::{Volatility, create_udf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    for (name, source) in [
        ("failing_float_producer", DataType::Float32),
        ("failing_double_producer", DataType::Float64),
    ] {
        let observed_calls = Arc::clone(&calls);
        session.register_udf(create_udf(
            name,
            vec![],
            source,
            Volatility::Volatile,
            Arc::new(move |_| {
                observed_calls.fetch_add(1, Ordering::SeqCst);
                Err(DataFusionError::Execution(format!(
                    "{name} failed before conversion"
                )))
            }),
        ));
        for ansi_mode in [true, false] {
            for target in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
                let before = calls.load(Ordering::SeqCst);
                let query = plan_query(
                    &session,
                    &format!("SELECT TRY_CAST({name}() AS {target}) AS r"),
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
                    .ok_or("TRY_CAST swallowed a producer error")?;
                assert!(
                    matches!(error.find_root(), DataFusionError::Execution(message)
                    if message == &format!("{name} failed before conversion"))
                );
                assert_eq!(calls.load(Ordering::SeqCst), before + 1);
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
    for kind in ["fields", "availability", "rows"] {
        for facet in control[kind].as_array().ok_or("missing retained facets")? {
            let key = facet["key"].as_str().ok_or("missing observed key")?;
            let owner = facet["owner"].as_u64().ok_or("missing facet owner")?;
            if kind == "availability" {
                assert!(matches!(owner, 922 | 1020));
                if normalized[key] != facet["inherited_dar"] {
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
        if ["error", "native_phase", "optimized_error"]
            .into_iter()
            .any(|key| actual[key] != retained_error[key])
            || !actual["rows"].is_null()
            || !actual["rows_raw"].is_null()
            || actual["error_matches"] != false
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
        if actual["optimized_matches"] != false || retained_error.is_null() {
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
        ("/matches", json!(true)),
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
        case, &actual, 2, &facets
    )?);
    for (key, value) in [
        ("sql", json!("SELECT 99")),
        ("id", json!("unlisted")),
        ("ansi", json!(true)),
    ] {
        let mut changed = case.clone();
        changed[key] = value;
        assert!(!floating_integer_scoped_contract_matches(
            &changed, &actual, 1, &facets
        )?);
    }

    let case = records
        .iter()
        .find(|case| case["id"] == "float_tinyint_literal_nan" && case["ansi"] == true)
        .ok_or("missing checked cause control")?;
    let error = facets["contexts"]
        .as_array()
        .ok_or("missing retained contexts")?
        .iter()
        .find(|control| {
            control["id"] == case["id"] && control["ansi"] == case["ansi"] && control["batch"] == 1
        })
        .ok_or("missing exact cause")?["error"]
        .clone();
    let actual = json!({
        "public_schema":case["schema_stages"]["analyzed"],
        "optimized_schema":null,"before_collect":null,"physical_schema":null,
        "rows":null,"rows_raw":null,"native_phase":error["native_phase"],
        "error":error["error"],"optimized_error":error["optimized_error"],
        "public_matches":true,"before_matches":true,"physical_matches":true,
        "rows_match":true,"ordered_rows_match":true,"error_matches":false,
        "matches":false,"optimized_matches":false,"optimized_observation":null,
    });
    assert!(floating_integer_scoped_contract_matches(
        case, &actual, 1, &facets
    )?);
    for key in ["error", "optimized_error", "native_phase"] {
        let mut changed = actual.clone();
        changed[key] = json!("[CAST_OVERFLOW] a different cause or stage");
        assert!(!floating_integer_scoped_contract_matches(
            case, &changed, 1, &facets
        )?);
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
    let mut retained_keys = BTreeSet::new();
    for control in contexts {
        assert!(retained_keys.insert((
            control["id"].to_string(),
            control["ansi"].to_string(),
            control["batch"].as_u64().ok_or("missing batch")?
        )));
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
    assert_eq!(retained_contexts, 260);
    eprintln!(
        "FLOATING_INTEGER_VALUE_SUMMARY {}",
        json!({
            "comparisons":comparisons,"strict_reference_matches":strict_matches,
            "retained_contexts":retained_contexts,"unexpected_differences":differences.len(),
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
        "SELECT CAST('1.5' AS DOUBLE) AS r",
        "SELECT TRY_CAST('bad' AS FLOAT) AS r",
        "SELECT CAST(NULL AS FLOAT) AS r",
        "SELECT CAST(CAST(4294967296 AS BIGINT) AS INT) AS r",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    for source in ["FLOAT", "DOUBLE"] {
        for cast in ["CAST", "TRY_CAST"] {
            for value in ["1.9", "'NaN'", "'Infinity'"] {
                if cast == "CAST" && value != "1.9" {
                    for target in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
                        controls.push(format!(
                            "SELECT CAST(CAST({value} AS {source}) AS {target}) AS r"
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
                controls.push(format!("SELECT {outputs}"));
            }
        }
    }
    let mut before = Vec::new();
    for sql in &controls {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        assert_eq!(actual, expected, "before Spark planning: {sql}");
        before.push(actual);
    }
    for ansi_mode in [true, false] {
        for sql in &controls {
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
    for (sql, before) in controls.iter().zip(before) {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        assert_eq!(actual, expected, "after Spark planning: {sql}");
        assert_eq!(
            actual, before,
            "native behavior changed after Spark planning: {sql}"
        );
    }
    Ok(())
}
