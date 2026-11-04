//! ARRAYS_OVERLAP result fields and values match independently captured Spark plans.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

#[tokio::test]
async fn arrays_overlap_result_fields_match_spark() -> TestResult {
    assert_arrays_overlap_result_fields(
        include_str!("fixtures/arrays-overlap-result-fields-spark.jsonl"),
        26,
    )
    .await
}

#[tokio::test]
async fn arrays_overlap_numeric_declaration_controls_match_spark() -> TestResult {
    let records = include_str!("fixtures/array-result-fields-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 13);
    let numeric = records
        .into_iter()
        .filter(|record| {
            record.get("id").is_none()
                || matches!(
                    record["id"].as_str(),
                    Some("overlap_numeric_widening" | "overlap_numeric_reverse")
                )
        })
        .collect::<Vec<_>>();
    assert_arrays_overlap_result_records(&numeric, 4).await
}

#[tokio::test]
async fn arrays_overlap_incompatible_element_types_match_spark() -> TestResult {
    let records = include_str!("fixtures/array-result-fields-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|record| {
            record.get("id").is_none()
                || matches!(
                    record["id"].as_str(),
                    Some("overlap_bad_implicit_cast" | "overlap_bad_implicit_cast_is_null")
                )
        })
        .collect::<Vec<_>>();
    assert_arrays_overlap_result_records(&records, 4).await
}

#[tokio::test]
async fn arrays_overlap_common_element_types_match_spark() -> TestResult {
    for (source, count) in [
        (
            include_str!("fixtures/arrays-overlap-narrowing-spark.jsonl"),
            32,
        ),
        (
            include_str!("fixtures/arrays-overlap-coercion-result-fields-spark.jsonl"),
            40,
        ),
        (
            include_str!("fixtures/arrays-overlap-positive-coercion-result-fields-spark.jsonl"),
            16,
        ),
    ] {
        assert_arrays_overlap_result_fields(source, count).await?;
    }
    Ok(())
}

#[tokio::test]
async fn arrays_overlap_common_temporal_conversion_matches_spark() -> TestResult {
    let records = include_str!("fixtures/arrays-overlap-common-type-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 33);
    assert_eq!(records[0]["timezone"], "America/New_York");
    // The unchanged reference also contains independent #843 floating-equality cases.
    let temporal = records
        .into_iter()
        .filter(|record| {
            record.get("id").is_none()
                || record["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("dst_"))
        })
        .collect::<Vec<_>>();
    assert_arrays_overlap_result_records(&temporal, 16).await
}

#[tokio::test]
async fn arrays_overlap_binary_admission_matches_spark() -> TestResult {
    assert_arrays_overlap_result_fields(
        include_str!("fixtures/arrays-overlap-binary-admission-spark.jsonl"),
        136,
    )
    .await
}

#[derive(Clone, Copy, Debug)]
enum ArrayLayout {
    Lists,
    MixedOffsets,
    MixedViews,
}

fn binary_storage_type(
    data_type: &arrow::datatypes::DataType,
    storage: &arrow::datatypes::DataType,
) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    match data_type {
        DataType::Binary => storage.clone(),
        DataType::List(field) => DataType::List(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(binary_storage_type(field.data_type(), storage)),
        )),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(binary_storage_type(field.data_type(), storage))
                })
                .collect(),
        ),
        _ => data_type.clone(),
    }
}

#[tokio::test]
async fn arrays_overlap_binary_storage_matches_recorded_logical_types() -> TestResult {
    use arrow::datatypes::{DataType, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    let records = include_str!("fixtures/arrays-overlap-binary-admission-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 137);
    let binary_types = [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ];
    let mut comparisons = 0;
    let mut differences = Vec::new();
    for left_storage in &binary_types {
        for right_storage in &binary_types {
            for layout in [
                ArrayLayout::Lists,
                ArrayLayout::MixedOffsets,
                ArrayLayout::MixedViews,
            ] {
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
                    for input in records[0]["inputs"].as_array().ok_or("missing inputs")? {
                        let source = spark_inputs::decode_recorded_batch(input)?;
                        let fields = source
                            .schema()
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(index, field)| {
                                let storage = if index == 1 {
                                    left_storage
                                } else {
                                    right_storage
                                };
                                let target = binary_storage_type(field.data_type(), storage);
                                let target = if let DataType::List(child) = target {
                                    match (layout, index) {
                                        (ArrayLayout::MixedOffsets, 2) => {
                                            DataType::LargeList(child)
                                        }
                                        (ArrayLayout::MixedViews, 1) => DataType::ListView(child),
                                        (ArrayLayout::MixedViews, 2) => {
                                            DataType::LargeListView(child)
                                        }
                                        _ => DataType::List(child),
                                    }
                                } else {
                                    target
                                };
                                field.as_ref().clone().with_data_type(target)
                            })
                            .collect::<Vec<_>>();
                        let schema = Arc::new(Schema::new(fields));
                        let columns = source
                            .columns()
                            .iter()
                            .zip(schema.fields())
                            .map(|(column, field)| {
                                let value =
                                    arrow::compute::cast(column.as_ref(), field.data_type())?;
                                value.to_data().validate_full()?;
                                Ok(value)
                            })
                            .collect::<TestResult<Vec<_>>>()?;
                        let batch = RecordBatch::try_new(schema.clone(), columns)?;
                        let mut partitions = vec![vec![batch.slice(1, 0)], Vec::new()];
                        for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate()
                        {
                            partitions[chunk % 2].push(
                                batch.slice(offset, batch_size.min(batch.num_rows() - offset)),
                            );
                        }
                        session.register_table(
                            input["name"].as_str().ok_or("missing table name")?,
                            Arc::new(MemTable::try_new(schema, partitions)?),
                        )?;
                    }
                    for case in records[1..].iter().filter(|case| {
                        case["order"] == "both"
                            || (left_storage == right_storage
                                && case["source"] == "provider"
                                && case["family"]
                                    .as_str()
                                    .is_some_and(|family| family.contains("binary_string")))
                    }) {
                        let mut comparison =
                            spark_results::compare_exact_spark_query_stages(&session, case).await?;
                        comparison["id"] = case["id"].clone();
                        comparison["ansi"] = case["ansi"].clone();
                        comparison["batch"] = json!(batch_size);
                        comparison["left_storage"] = json!(left_storage.to_string());
                        comparison["right_storage"] = json!(right_storage.to_string());
                        comparison["layout"] = json!(format!("{layout:?}"));
                        eprintln!("ARRAYS_OVERLAP_BINARY_STORAGE_OBSERVATION {comparison}");
                        comparisons += 1;
                        if comparison["matches"] != true {
                            differences.push(comparison.to_string());
                        }
                    }
                }
            }
        }
    }
    assert_eq!(comparisons, 1512);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn assert_arrays_overlap_result_fields(source: &str, expected_cases: usize) -> TestResult {
    let records = source
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_arrays_overlap_result_records(&records, expected_cases).await
}

async fn assert_arrays_overlap_result_records(
    records: &[Value],
    expected_cases: usize,
) -> TestResult {
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, expected_cases);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            ))
        })
        .collect::<TestResult<std::collections::BTreeSet<_>>>()?;
    assert_eq!(identities.len(), expected_cases);
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
        if records[0].get("inputs").is_some() {
            spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        }
        for case in &records[1..] {
            comparisons += 1;
            if case["expected"]["condition"].is_null() {
                assert_eq!(
                    case["expected"]["schema"],
                    case["schema_stages"]["analyzed"]
                );
                assert_eq!(
                    case["schema_stages"]["executed_before_collect"],
                    case["schema_stages"]["executed"]
                );
            }
            let mut comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            apply_arrays_overlap_diagnostic_alias(case, &mut comparison);
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("ARRAY_RESULT_FIELDS_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(comparisons, expected_cases * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

// The resolver reports these input errors without Spark condition tags.
fn apply_arrays_overlap_diagnostic_alias(case: &Value, comparison: &mut Value) {
    let diagnostic = match (case["expected"]["condition"].as_str(), case["sql"].as_str()) {
        (
            Some("DATATYPE_MISMATCH.BINARY_ARRAY_DIFF_TYPES"),
            Some("SELECT ARRAYS_OVERLAP(1, ARRAY(1)) AS r"),
        ) => "ARRAYS_OVERLAP requires two array arguments",
        (
            Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION"),
            Some("SELECT ARRAYS_OVERLAP(ARRAY(1)) AS r"),
        ) => "ARRAYS_OVERLAP requires two arguments, got 1",
        _ => return,
    };
    comparison["error_matches"] = json!(
        case["spark_error_phase"] == "sql"
            && comparison["native_phase"] == "resolve"
            && comparison["error"]
                .as_str()
                .is_some_and(|error| error.contains(diagnostic))
    );
    comparison["matches"] = json!(
        [
            "public_matches",
            "before_matches",
            "physical_matches",
            "rows_match",
            "ordered_rows_match",
            "error_matches"
        ]
        .iter()
        .all(|key| comparison[key] == true)
    );
}

#[test]
fn arrays_overlap_diagnostic_alias_keeps_exact_boundaries() -> TestResult {
    let case = json!({
        "sql":"SELECT ARRAYS_OVERLAP(1, ARRAY(1)) AS r",
        "expected":{"condition":"DATATYPE_MISMATCH.BINARY_ARRAY_DIFF_TYPES"},
        "spark_error_phase":"sql",
    });
    let observation = json!({
        "error":"Invalid argument: ARRAYS_OVERLAP requires two array arguments",
        "native_phase":"resolve",
        "public_matches":true, "before_matches":true, "physical_matches":true,
        "rows_match":true, "ordered_rows_match":true,
        "error_matches":false, "matches":false,
    });
    let mut comparison = observation.clone();
    apply_arrays_overlap_diagnostic_alias(&case, &mut comparison);
    assert_eq!(comparison["matches"], true);
    for (key, wrong) in [
        ("error", json!("unrelated error")),
        ("native_phase", json!("collect")),
        ("public_matches", json!(false)),
        ("before_matches", json!(false)),
        ("physical_matches", json!(false)),
        ("rows_match", json!(false)),
        ("ordered_rows_match", json!(false)),
    ] {
        let mut comparison = observation.clone();
        comparison[key] = wrong;
        apply_arrays_overlap_diagnostic_alias(&case, &mut comparison);
        assert_eq!(comparison["matches"], false, "{key}");
    }
    for wrong in [
        json!({"sql":"SELECT ARRAYS_OVERLAP(2, ARRAY(1)) AS r"}),
        json!({"expected":{"condition":"CAST_INVALID_INPUT"}}),
        json!({"spark_error_phase":"collect"}),
    ] {
        let mut reference = case.clone();
        for (key, value) in wrong.as_object().ok_or("missing negative control")? {
            reference[key] = value.clone();
        }
        let mut comparison = observation.clone();
        apply_arrays_overlap_diagnostic_alias(&reference, &mut comparison);
        assert_eq!(comparison["matches"], false, "{wrong}");
    }
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_array_overlap() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT ARRAY_HAS_ANY([1], [1]) AS r",
        "SELECT ARRAY_HAS_ANY([CAST(1 AS INT)], [CAST(2147483648 AS BIGINT)]) AS r",
        "SELECT ARRAY_HAS_ANY([CAST('-0.0' AS FLOAT)], [CAST('0.0' AS DOUBLE)]) AS r",
        "SELECT ARRAY_HAS_ANY([1, NULL], [2]) AS r",
        "SELECT ARRAY_HAS_ANY([1, NULL], []) AS r",
        "SELECT ARRAY_HAS_ANY(CAST(NULL AS INT[]), [1]) AS r",
    ] {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        let observation = json!({"sql":sql,"native":expected,"configured":actual});
        eprintln!("ARRAYS_OVERLAP_NATIVE_OBSERVATION {observation}");
        assert!(observation["native"]["error"].is_null(), "{observation}");
        assert_eq!(observation["native"]["phase"], "encode", "{observation}");
        for stage in ["public_schema", "before_collect", "physical_schema"] {
            assert!(observation["native"][stage].is_object(), "{observation}");
        }
        assert!(observation["native"]["rows"].is_array(), "{observation}");
        assert_eq!(
            observation["configured"], observation["native"],
            "{observation}"
        );
    }
    Ok(())
}
