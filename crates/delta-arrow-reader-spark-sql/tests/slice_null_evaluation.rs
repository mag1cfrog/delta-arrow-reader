//! SLICE NULL evaluation matches independently captured Spark schemas and errors.

use arrow::compute::concat_batches;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const ORIGINAL_IDS: &[&str] = &[
    "null_array_bad_start",
    "bad_array_null_start",
    "bad_array_null_start_is_null",
    "constant_bad_array_null_start",
];

const COMPARISON_FLAGS: &[&str] = &[
    "matches",
    "optimized_matches",
    "public_matches",
    "before_matches",
    "physical_matches",
    "rows_match",
    "ordered_rows_match",
    "error_matches",
    "cast_input_matches",
];

fn slice_null_comparison_matches(case: &Value, comparison: &Value) -> bool {
    comparison["id"] == case["id"]
        && comparison["ansi"] == case["ansi"]
        && COMPARISON_FLAGS.iter().all(|flag| comparison[flag] == true)
}

#[test]
fn cast_error_priority_rejects_a_different_failing_argument() {
    let case = json!({
        "expected":{"condition":"CAST_INVALID_INPUT"},
        "spark_error_details":{"message_parameters":{"expression":"'array_bad'"}},
    });
    for input in ["array_bad", "start_bad", "length_bad"] {
        let comparison = json!({"error":format!(
            "[CAST_INVALID_INPUT]\nArrow error: Cast error: Cannot cast string '{input}' to value of Int32 type"
        )});
        assert_eq!(
            spark_results::matches_spark_int32_cast_input(&case, &comparison),
            input == "array_bad"
        );
    }
    assert!(!spark_results::matches_spark_int32_cast_input(
        &case,
        &json!({"error":"[CAST_INVALID_INPUT]"})
    ));
    assert!(!spark_results::matches_spark_int32_cast_input(
        &case,
        &json!({"error":concat!(
            "SQL: CAST('array_bad' AS INT), CAST('start_bad' AS INT)\n",
            "[CAST_INVALID_INPUT]\n",
            "Arrow error: Cast error: Cannot cast string 'start_bad' to value of Int32 type",
        )})
    ));
}

#[tokio::test]
async fn ordered_observer_rejects_unexpected_success_and_reordered_rows() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let schema = json!({"type":"struct","fields":[{
        "name":"r","type":"integer","nullable":false,"metadata":{},
    }]});
    let mut case = json!({
        "sql":"SELECT CAST(1 AS INT) AS r", "ansi":true, "ordered":true,
        "expected":{"condition":"CAST_INVALID_INPUT"}, "spark_error_phase":"collect",
        "schema_stages":{
            "public":schema, "analyzed":schema, "optimized":schema,
            "executed_before_collect":schema, "executed":schema,
        },
    });
    let unexpected_success =
        spark_results::compare_exact_spark_query_stages_with_optimized(&session, &case).await?;
    assert_eq!(unexpected_success["ordered_rows_match"], true);
    assert_eq!(unexpected_success["error_matches"], false);
    assert_eq!(unexpected_success["matches"], false);

    case["sql"] = json!("SELECT k AS r FROM VALUES (1), (2) t(k) ORDER BY k");
    case["expected"] = json!({"schema":schema,"rows":[["2"],["1"]]});
    let reordered =
        spark_results::compare_exact_spark_query_stages_with_optimized(&session, &case).await?;
    assert_eq!(reordered["rows_match"], true);
    assert_eq!(reordered["ordered_rows_match"], false);
    assert_eq!(reordered["matches"], false);

    case["expected"] = json!({"schema":schema});
    let missing =
        spark_results::compare_exact_spark_query_stages_with_optimized(&session, &case).await?;
    assert_eq!(missing["ordered_rows_match"], false);
    assert_eq!(missing["matches"], false);

    case["sql"] = json!("SELECT CAST(1 AS INT) AS r WHERE false");
    case["expected"] = json!({"schema":schema,"rows":[]});
    let empty =
        spark_results::compare_exact_spark_query_stages_with_optimized(&session, &case).await?;
    assert_eq!(empty["ordered_rows_match"], true);
    Ok(())
}

async fn compare_null_evaluation(records: &[Value], cases: &[&Value]) -> TestResult {
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch)
                            .with_target_partitions(1),
                    ),
            )
            .build(),
        );
        // Both Spark captures use local[1] and one shuffle partition.
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &records[0],
            batch,
            1,
        )?;
        for input in records[0]["inputs"].as_array().ok_or("missing inputs")? {
            let table = input["name"].as_str().ok_or("missing table name")?;
            let provider = session.table_provider(table).await?;
            let state = session.state();
            let scan = provider.scan(&state, None, &[], None).await?;
            assert_eq!(
                scan.output_partitioning().partition_count(),
                1,
                "{table} batch={batch}"
            );
            let expected = spark_inputs::decode_recorded_batch(input)?;
            assert_eq!(scan.schema(), expected.schema(), "{table} batch={batch}");
            let batches = collect(scan, session.task_ctx()).await?;
            for actual in &batches {
                spark_results::validate_batch(expected.schema().as_ref(), actual)?;
            }
            assert_eq!(
                concat_batches(&expected.schema(), &batches)?,
                expected,
                "{table} unsorted input order, batch={batch}"
            );
        }
        for case in cases {
            let mut comparison =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await
                    .unwrap_or_else(|error| json!({"observer_error":error.to_string()}));
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["sql"] = case["sql"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch);
            comparison["cast_input_matches"] = json!(
                spark_results::matches_spark_int32_cast_input(case, &comparison)
            );
            eprintln!("SLICE_NULL_EVALUATION_OBSERVATION {comparison}");
            if !slice_null_comparison_matches(case, &comparison) {
                differences.push(comparison.to_string());
            }
            assert!(completed.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch,
            )));
        }
    }
    assert_eq!(completed.len(), cases.len() * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn slice_null_evaluation_preserves_original_references() -> TestResult {
    let records = include_str!("fixtures/slice-parent-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["group"], "slice-parent-878-current-v1");
    let cases = records[1..]
        .iter()
        .filter(|case| {
            case["id"]
                .as_str()
                .is_some_and(|id| ORIGINAL_IDS.contains(&id))
        })
        .collect::<Vec<_>>();
    let identities = cases
        .iter()
        .map(|case| (case["id"].as_str(), case["ansi"].as_bool()))
        .collect::<Vec<_>>();
    let expected = [true, false]
        .into_iter()
        .flat_map(|ansi| ORIGINAL_IDS.iter().map(move |id| (Some(*id), Some(ansi))))
        .collect::<Vec<_>>();
    assert_eq!(identities, expected);
    compare_null_evaluation(&records, &cases).await
}

#[tokio::test]
async fn slice_null_evaluation_matches_fresh_spark_capture() -> TestResult {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/slice-null-evaluation/manifest.json"))?;
    let records = include_str!("fixtures/slice-null-evaluation/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(manifest["scenario_count"], 49);
    assert_eq!(manifest["observation_count"], 98);
    assert_eq!(manifest["ansi_modes"], json!([true, false]));
    assert_eq!(records.len(), 99);
    assert_eq!(records[0]["group"], "slice-881-null-evaluation-current-v1");
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(
        records[0]["source_commit"],
        "085986ca2a53d8e249222ecd4052ea884386a203"
    );
    assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
    for key in [
        "spark_version",
        "spark_revision",
        "timezone",
        "group",
        "source_commit",
        "inputs",
        "input_encoding",
    ] {
        assert_eq!(records[0][key], manifest[key], "{key}");
    }
    assert_eq!(
        records[0]["capture_sha256"],
        manifest["capture_script_sha256"]
    );
    let identities = records[1..]
        .iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    assert_eq!(json!(identities), manifest["identities"]);
    let row_priority_ids = [
        "rows_zero_start_before_later_fallible_operands",
        "rows_negative_length_before_later_fallible_operands",
    ];
    let row_priority = records[1..]
        .iter()
        .filter(|case| {
            case["id"]
                .as_str()
                .is_some_and(|id| row_priority_ids.contains(&id))
        })
        .collect::<Vec<_>>();
    let expected_row_priority = [true, false]
        .into_iter()
        .flat_map(|ansi| {
            row_priority_ids
                .iter()
                .map(move |id| json!({"id":id,"ansi":ansi}))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        row_priority
            .iter()
            .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
            .collect::<Vec<_>>(),
        expected_row_priority,
    );
    for case in row_priority {
        assert_eq!(case["spark_error_phase"], "collect");
        let stages = case["schema_stages"]
            .as_object()
            .ok_or("missing row-priority schema stages")?;
        assert_eq!(stages.len(), 6);
        for stage in [
            "public",
            "resolved",
            "analyzed",
            "optimized",
            "physical",
            "executed_before_collect",
        ] {
            assert!(stages.get(stage).is_some_and(Value::is_object), "{stage}");
        }
    }
    let original = include_str!("fixtures/slice-parent-fields-spark.jsonl")
        .lines()
        .skip(1)
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let mut outcomes = (0, 0);
    for case in &records[1..] {
        let definition = manifest["scenarios"]
            .as_array()
            .ok_or("missing scenarios")?
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("missing scenario")?;
        for (key, value) in definition.as_object().ok_or("missing definition")? {
            assert_eq!(&case[key], value, "{} {key}", case["id"]);
        }
        let stages = &case["schema_stages"];
        assert!(stages["public"].is_object());
        assert_eq!(stages["public"], stages["resolved"]);
        assert_eq!(stages["public"], stages["analyzed"]);
        if let Some(physical) = stages.get("physical") {
            assert_eq!(physical, &stages["executed_before_collect"]);
        }
        if case["expected"]["condition"].is_string() {
            outcomes.1 += 1;
            assert_eq!(case["query_failure_kind"], "sql_error");
            assert_eq!(
                case["expected"]["condition"],
                case["spark_error_details"]["condition"]
            );
        } else {
            outcomes.0 += 1;
            assert!(case["expected"]["rows"].is_array());
            assert_eq!(case["expected"]["schema"], stages["public"]);
            assert!(case.get("query_failure_kind").is_none());
        }
        if case["id"]
            .as_str()
            .is_some_and(|id| ORIGINAL_IDS.contains(&id))
        {
            let reference = original
                .iter()
                .find(|reference| {
                    reference["id"] == case["id"] && reference["ansi"] == case["ansi"]
                })
                .ok_or("missing original reference")?;
            for key in ["sql", "expected"] {
                assert_eq!(case[key], reference[key], "{} {key}", case["id"]);
            }
            for (stage, schema) in reference["schema_stages"]
                .as_object()
                .ok_or("missing original schema stages")?
            {
                assert_eq!(&stages[stage], schema, "{} {stage}", case["id"]);
            }
        }
    }
    assert_eq!(outcomes, (61, 37));
    let cases = records[1..].iter().collect::<Vec<_>>();
    compare_null_evaluation(&records, &cases).await
}

#[tokio::test]
async fn slice_concat_coordinates_preserve_null_short_circuiting() -> TestResult {
    let header = json!({"inputs":[{
        "name":"slice_concat_input",
        "schema":{"type":"struct","fields":[
            {"name":"k","type":"integer","nullable":false,"metadata":{}},
            {"name":"a","type":{"type":"array","elementType":"integer","containsNull":false},"nullable":true,"metadata":{}},
            {"name":"txt","type":"string","nullable":true,"metadata":{}},
            {"name":"length_txt","type":"string","nullable":false,"metadata":{}},
        ]},
        "rows":[
            ["0",null,"bad","bad"],
            ["1",["10","11"],"1","1"],
            ["2",["20","21"],null,"bad"],
            ["3",["30","31"],"2","1"],
        ],
    }]});
    let sql = "SELECT k, SLICE(a, \
        CAST(CONCAT('', CAST(CAST(txt AS INT) AS STRING)) AS INT), \
        CAST(length_txt AS INT)) AS r FROM slice_concat_input ORDER BY k";
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch_size)),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(&session, &header, batch_size)?;
        for ansi_mode in [true, false] {
            let query = delta_arrow_reader_spark_sql::plan_query(
                &session,
                sql,
                &delta_arrow_reader_spark_sql::SparkSqlOptions {
                    ansi_mode,
                    ..Default::default()
                },
            )
            .await?;
            let physical = query.create_physical_plan(&session).await?;
            let plan = datafusion::physical_plan::displayable(physical.as_ref())
                .indent(true)
                .to_string();
            assert!(plan.contains("spark_concat_runtime"), "{plan}");
            let actual = spark_results::capture_spark_query_result(
                &session,
                &json!({
                    "sql":sql, "ansi":ansi_mode,
                }),
            )
            .await?;
            assert_eq!(
                actual["rows"],
                json!([["0", null], ["1", ["10"]], ["2", null], ["3", ["31"]],]),
                "ANSI={ansi_mode} batch={batch_size}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn slice_selected_lambda_coordinates_keep_variables_and_captures() -> TestResult {
    let header = json!({"inputs":[{
        "name":"slice_projection_input",
        "schema":{"type":"struct","fields":[
            {"name":"k","type":"integer","nullable":false,"metadata":{}},
            {"name":"payload","type":"string","nullable":false,"metadata":{"probe":"kept"}},
            {"name":"a","type":{"type":"array","elementType":"integer","containsNull":false},"nullable":true,"metadata":{}},
            {"name":"starts","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":false,"metadata":{}},
            {"name":"bias","type":"integer","nullable":false,"metadata":{}},
            {"name":"n","type":"integer","nullable":false,"metadata":{}},
        ]},
        "rows":[
            ["0","kept",null,["0","1",null],"1","1"],
            ["1","kept",["1","2"],["0","1",null],"1","1"],
            ["2","kept",["3","4"],["1","2",null],"0","1"],
        ],
    }]});
    let expected = json!([
        ["0", "kept", [null, null, null], null],
        ["1", "kept", [["1"], ["2"], null], ["2"]],
        ["2", "kept", [["3"], ["4"], null], ["4"]],
    ]);
    let sql = "SELECT k, payload, TRANSFORM(starts, x -> SLICE(a, x + bias, n)) AS r, \
        SLICE(a, ARRAY_MAX(TRANSFORM(starts, y -> y + bias)), n) AS captured_start \
        FROM slice_projection_input ORDER BY k";
    let concat_sql = "SELECT k, payload, TRANSFORM(starts, x -> SLICE(a, \
        CAST(CONCAT('', CAST(x + bias AS STRING)) AS INT), n)) AS r, \
        SLICE(a, ARRAY_MAX(TRANSFORM(starts, y -> y + bias)), n) AS captured_start \
        FROM slice_projection_input ORDER BY k";
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch_size)),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(&session, &header, batch_size)?;
        for ansi_mode in [true, false] {
            for (sql, runtime_concat) in [(sql, false), (concat_sql, true)] {
                let query = delta_arrow_reader_spark_sql::plan_query(
                    &session,
                    sql,
                    &delta_arrow_reader_spark_sql::SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                if runtime_concat {
                    let plan = datafusion::physical_plan::displayable(physical.as_ref())
                        .indent(true)
                        .to_string();
                    assert!(plan.contains("spark_concat_runtime"), "{plan}");
                }
                let schema = physical.schema();
                assert_eq!(
                    schema.field(1).metadata().get("probe").map(String::as_str),
                    Some("kept")
                );
                let batches =
                    datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
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
                assert_eq!(json!(rows), expected, "ANSI={ansi_mode} batch={batch_size}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn slice_local_null_masks_skip_only_discarded_coordinate_rows() -> TestResult {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};

    let cases = [
        (
            "slice_literal_null_div",
            "SELECT SLICE(CAST(NULL AS ARRAY<INT>), 1 DIV 0, 1) AS r",
            json!([[null]]),
            true,
            false,
        ),
        (
            "slice_local_null_div",
            "SELECT SLICE(a, 1 DIV 0, 1) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>)) AS t(a)",
            json!([[null]]),
            true,
            false,
        ),
        (
            "slice_all_null_column_div",
            "SELECT k, SLICE(a, 1 DIV d, 1) AS r FROM VALUES (0,CAST(NULL AS ARRAY<INT>),0),(1,CAST(NULL AS ARRAY<INT>),0) AS t(k,a,d) ORDER BY k",
            json!([["0", null], ["1", null]]),
            true,
            true,
        ),
        (
            "slice_all_null_column_cast",
            "SELECT k, SLICE(a, CAST(txt AS INT), 1) AS r FROM VALUES (0,CAST(NULL AS ARRAY<INT>),'bad'),(1,CAST(NULL AS ARRAY<INT>),'worse') AS t(k,a,txt) ORDER BY k",
            json!([["0", null], ["1", null]]),
            true,
            true,
        ),
        (
            "slice_mixed_array_div",
            "SELECT k, SLICE(a, 1 DIV d, 1) AS r FROM VALUES (0,CAST(NULL AS ARRAY<INT>),0),(1,ARRAY(7,8),1),(2,CAST(NULL AS ARRAY<INT>),0),(3,ARRAY(9,10),1) AS t(k,a,d) ORDER BY k",
            json!([["0", null], ["1", ["7"]], ["2", null], ["3", ["9"]]]),
            true,
            true,
        ),
        (
            "slice_mixed_array_cast",
            "SELECT k, SLICE(a, CAST(txt AS INT), 1) AS r FROM VALUES (0,CAST(NULL AS ARRAY<INT>),'bad'),(1,ARRAY(7,8),'1'),(2,CAST(NULL AS ARRAY<INT>),'worse'),(3,ARRAY(9,10),'2') AS t(k,a,txt) ORDER BY k",
            json!([["0", null], ["1", ["7"]], ["2", null], ["3", ["10"]]]),
            true,
            true,
        ),
        (
            "slice_mixed_start_div",
            "SELECT k, SLICE(a, s, 1 DIV d) AS r FROM VALUES (0,ARRAY(7,8),CAST(NULL AS INT),0),(1,ARRAY(7,8),1,1),(2,ARRAY(9,10),CAST(NULL AS INT),0),(3,ARRAY(9,10),2,1) AS t(k,a,s,d) ORDER BY k",
            json!([["0", null], ["1", ["7"]], ["2", null], ["3", ["10"]]]),
            false,
            true,
        ),
        (
            "slice_mixed_start_cast",
            "SELECT k, SLICE(a, s, CAST(txt AS INT)) AS r FROM VALUES (0,ARRAY(7,8),CAST(NULL AS INT),'bad'),(1,ARRAY(7,8),1,'1'),(2,ARRAY(9,10),CAST(NULL AS INT),'worse'),(3,ARRAY(9,10),2,'1') AS t(k,a,s,txt) ORDER BY k",
            json!([["0", null], ["1", ["7"]], ["2", null], ["3", ["10"]]]),
            false,
            true,
        ),
        (
            "slice_mixed_array_and_start_masks",
            "SELECT k, SLICE(a, CAST(txt AS INT), 1 DIV d) AS r FROM VALUES (0,CAST(NULL AS ARRAY<INT>),'bad',0),(1,ARRAY(7,8),CAST(NULL AS STRING),0),(2,ARRAY(7,8),'1',1),(3,CAST(NULL AS ARRAY<INT>),'worse',0),(4,ARRAY(9,10),'2',1) AS t(k,a,txt,d) ORDER BY k",
            json!([
                ["0", null],
                ["1", null],
                ["2", ["7"]],
                ["3", null],
                ["4", ["10"]]
            ]),
            true,
            true,
        ),
        (
            "slice_literal_null_cast",
            "SELECT SLICE(CAST(NULL AS ARRAY<INT>), CAST('bad' AS INT), 1) AS r",
            json!([[null]]),
            true,
            false,
        ),
    ];
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(1),
                    ),
            )
            .build(),
        );
        for ansi_mode in [true, false] {
            for (id, sql, expected_rows, contains_null, keyed) in &cases {
                let query = plan_query(
                    &session,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?;
                let mut expected_fields = Vec::new();
                if *keyed {
                    expected_fields.push(Field::new("k", DataType::Int32, false));
                }
                expected_fields.push(Field::new(
                    "r",
                    DataType::List(Arc::new(Field::new_list_field(
                        DataType::Int32,
                        *contains_null,
                    ))),
                    true,
                ));
                let expected_schema = Schema::new(expected_fields);
                let analyzed = query.logical_plan.schema().as_arrow();
                let public_fields = analyzed
                    .fields()
                    .iter()
                    .zip(&query.column_names)
                    .map(|(field, name)| field.as_ref().clone().with_name(name.clone()))
                    .collect::<Vec<_>>();
                assert_eq!(query.column_names.len(), analyzed.fields().len(), "{id}");
                assert_eq!(
                    Schema::new_with_metadata(public_fields, analyzed.metadata().clone()),
                    expected_schema,
                    "analyzed {id} ANSI={ansi_mode} batch={batch_size}",
                );
                let physical = query.create_physical_plan(&session).await?;
                let schema = physical.schema();
                assert_eq!(
                    schema.as_ref(),
                    &expected_schema,
                    "physical {id} ANSI={ansi_mode} batch={batch_size}",
                );
                let batches = collect(physical, session.task_ctx()).await?;
                let mut rows = Vec::new();
                for batch in batches {
                    assert_eq!(batch.schema(), schema, "{id}");
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
                assert_eq!(
                    json!(rows),
                    *expected_rows,
                    "{id} ANSI={ansi_mode} batch={batch_size}",
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn slice_local_null_masks_preserve_reached_coordinate_errors() -> TestResult {
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};

    let division: &[&str] = &[
        "preserve_spark_expression_errors\ncaused by\nArrow error: Divide by zero error",
        "Arrow error: Divide by zero error",
        "Divide by zero error",
    ];
    let cast: &[&str] = &[
        "[CAST_INVALID_INPUT]\ncaused by\nArrow error: Cast error: Cannot cast string 'reached_bad' to value of Int32 type",
        "Arrow error: Cast error: Cannot cast string 'reached_bad' to value of Int32 type",
        "Cast error: Cannot cast string 'reached_bad' to value of Int32 type",
    ];
    let folded_cast: &[&str] = &[
        "Optimizer rule 'simplify_expressions' failed\ncaused by\n[CAST_INVALID_INPUT]\ncaused by\nArrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
        "[CAST_INVALID_INPUT]\ncaused by\nArrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
        "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
        "Cast error: Cannot cast string 'bad' to value of Int32 type",
    ];
    let cases = [
        (
            "slice_local_live_div",
            "SELECT SLICE(a, 1 DIV 0, 1) AS r FROM VALUES(ARRAY(1)) AS t(a)",
            "physical",
            division,
            json!([[null]]),
        ),
        (
            "slice_mixed_reached_div",
            "SELECT k, SLICE(a, 1 DIV d, 1) AS r FROM VALUES (0,CAST(NULL AS ARRAY<INT>),0),(1,ARRAY(7,8),1),(2,ARRAY(9,10),0) AS t(k,a,d) ORDER BY k",
            "physical",
            division,
            json!([["0", null], ["1", ["7"]], ["2", null]]),
        ),
        (
            "slice_mixed_reached_cast",
            "SELECT k, SLICE(a, CAST(txt AS INT), 1) AS r FROM VALUES (0,CAST(NULL AS ARRAY<INT>),'discarded_bad'),(1,ARRAY(7,8),'1'),(2,ARRAY(9,10),'reached_bad') AS t(k,a,txt) ORDER BY k",
            "execute",
            cast,
            json!([["0", null], ["1", ["7"]], ["2", null]]),
        ),
        (
            "slice_mixed_reached_length_div",
            "SELECT k, SLICE(a, s, 1 DIV d) AS r FROM VALUES (0,ARRAY(7,8),CAST(NULL AS INT),0),(1,ARRAY(7,8),1,1),(2,ARRAY(9,10),1,0) AS t(k,a,s,d) ORDER BY k",
            "physical",
            division,
            json!([["0", null], ["1", ["7"]], ["2", null]]),
        ),
        (
            "slice_mixed_reached_length_cast",
            "SELECT k, SLICE(a, s, CAST(txt AS INT)) AS r FROM VALUES (0,ARRAY(7,8),CAST(NULL AS INT),'discarded_bad'),(1,ARRAY(7,8),1,'1'),(2,ARRAY(9,10),1,'reached_bad') AS t(k,a,s,txt) ORDER BY k",
            "execute",
            cast,
            json!([["0", null], ["1", ["7"]], ["2", null]]),
        ),
        (
            "slice_local_null_constant_cast",
            "SELECT SLICE(a, CAST('bad' AS INT), 1) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>)) AS t(a)",
            "physical",
            folded_cast,
            json!([[null]]),
        ),
        (
            "slice_mixed_constant_div",
            "SELECT k, SLICE(a, 1 DIV 0, 1) AS r FROM VALUES (0,CAST(NULL AS ARRAY<INT>)),(1,ARRAY(7)) AS t(k,a) ORDER BY k",
            "physical",
            division,
            json!([["0", null], ["1", null]]),
        ),
        (
            "slice_mixed_constant_cast",
            "SELECT k, SLICE(a, CAST('bad' AS INT), 1) AS r FROM VALUES (0,CAST(NULL AS ARRAY<INT>)),(1,ARRAY(7)) AS t(k,a) ORDER BY k",
            "physical",
            folded_cast,
            json!([["0", null], ["1", null]]),
        ),
        (
            "slice_literal_live_div",
            "SELECT SLICE(ARRAY(7), 1 DIV 0, 1) AS r",
            "physical",
            division,
            json!([[null]]),
        ),
        (
            "slice_literal_live_cast",
            "SELECT SLICE(ARRAY(7), CAST('bad' AS INT), 1) AS r",
            "physical",
            folded_cast,
            json!([[null]]),
        ),
    ];
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch_size)),
            )
            .build(),
        );
        for (id, sql, error_phase, expected_chain, legacy_rows) in &cases {
            let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
            let physical = query.create_physical_plan(&session).await;
            let error = if *error_phase == "physical" {
                physical.expect_err("reached ANSI error must fail physical preparation")
            } else {
                let physical = physical?;
                let analyzed = query.logical_plan.schema().as_arrow();
                let public_fields = analyzed
                    .fields()
                    .iter()
                    .zip(&query.column_names)
                    .map(|(field, name)| field.as_ref().clone().with_name(name.clone()))
                    .collect::<Vec<_>>();
                assert_eq!(query.column_names.len(), analyzed.fields().len(), "{id}");
                assert_eq!(
                    physical.schema().as_ref(),
                    &arrow::datatypes::Schema::new_with_metadata(
                        public_fields,
                        analyzed.metadata().clone()
                    ),
                    "physical {id} batch={batch_size}",
                );
                collect(physical, session.task_ctx())
                    .await
                    .expect_err("reached ANSI cast must fail during execution")
            };
            let mut chain = Vec::new();
            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
            while let Some(cause) = source {
                chain.push(cause.to_string());
                source = cause.source();
            }
            assert_eq!(chain, *expected_chain, "{id} batch={batch_size}");
            let actual = spark_results::capture_spark_query_result(
                &session,
                &json!({"sql":sql,"ansi":false}),
            )
            .await?;
            assert_eq!(actual["rows"], *legacy_rows, "{id} batch={batch_size}");
        }
    }
    Ok(())
}
