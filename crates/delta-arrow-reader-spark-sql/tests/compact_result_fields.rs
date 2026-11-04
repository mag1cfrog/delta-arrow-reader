//! ARRAY_COMPACT fields and consumers match independently captured Spark results.

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

const SCENARIO_IDS: &[&str] = &[
    "declarations_direct",
    "declarations_is_null",
    "declarations_struct",
    "declarations_empty_direct",
    "declarations_empty_is_null",
    "declarations_empty_struct",
    "compacted_twice",
    "membership_consumer",
    "overlap_consumer",
    "nested_direct",
    "nested_is_null",
    "nested_struct",
    "nested_empty_direct",
    "nested_empty_is_null",
    "nested_empty_struct",
    "flatten_consumer",
    "typed_literals_direct",
    "typed_literals_is_null",
    "typed_literals_struct",
    "untyped_null_direct",
    "untyped_null_is_null",
    "untyped_null_struct",
    "nested_literal_direct",
    "nested_literal_is_null",
    "nested_literal_struct",
    "constant_bad_cast_direct",
    "constant_bad_cast_is_null",
    "constant_bad_cast_struct",
    "runtime_bad_cast_direct",
    "runtime_bad_cast_is_null",
    "runtime_bad_cast_struct",
    "runtime_bad_cast_empty_direct",
    "runtime_bad_cast_empty_is_null",
    "runtime_bad_cast_empty_struct",
    "invalid_non_array",
];

#[tokio::test]
async fn compact_result_fields_match_spark() -> TestResult {
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    let mut strict_matches = 0;
    for (fixture, group, ids, record_count) in [
        (
            include_str!("fixtures/array-compact-fields-spark.jsonl"),
            "array-compact-844-current-v1",
            SCENARIO_IDS,
            71,
        ),
        (
            include_str!("fixtures/array-compact-slices-spark.jsonl"),
            "array-compact-842-independent",
            &["compact_integer", "compact_string"][..],
            13,
        ),
    ] {
        let records = fixture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records.len(), record_count);
        assert_eq!(records[0]["group"], group);
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(
            records[0]["spark_revision"],
            "32f7299601108917fb01920a54e084595b7b3bf8"
        );
        if group == "array-compact-844-current-v1" {
            assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
        }
        let cases = records[1..]
            .iter()
            .filter(|case| case["id"].as_str().is_some_and(|id| ids.contains(&id)))
            .collect::<Vec<_>>();
        let identities = cases
            .iter()
            .map(|case| {
                Ok((
                    case["id"].as_str().ok_or("missing ID")?,
                    case["ansi"].as_bool().ok_or("missing ANSI")?,
                ))
            })
            .collect::<TestResult<Vec<_>>>()?;
        let expected_identities = [true, false]
            .into_iter()
            .flat_map(|ansi| ids.iter().map(move |id| (*id, ansi)))
            .collect::<Vec<_>>();
        assert_eq!(identities, expected_identities);
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
            for case in &cases {
                let mut comparison =
                    spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                        .await?;
                comparison["group"] = json!(group);
                comparison["id"] = case["id"].clone();
                comparison["ansi"] = case["ansi"].clone();
                comparison["batch"] = json!(batch_size);
                let strict_match = [
                    "matches",
                    "optimized_matches",
                    "public_matches",
                    "before_matches",
                    "physical_matches",
                    "rows_match",
                    "ordered_rows_match",
                    "error_matches",
                ]
                .iter()
                .all(|flag| comparison[flag] == true);
                strict_matches += usize::from(strict_match);
                assert!(completed.insert((
                    group.to_owned(),
                    case["id"].to_string(),
                    case["ansi"].to_string(),
                    batch_size
                )));
                eprintln!("ARRAY_COMPACT_FIELDS_OBSERVATION {comparison}");
                if !strict_match {
                    differences.push(format!("{group} {} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}", case["id"], case["ansi"], case["expected"], case["schema_stages"]));
                }
            }
        }
    }
    assert_eq!(completed.len(), 148);
    eprintln!(
        "ARRAY_COMPACT_FIELDS_SUMMARY {}",
        json!({"comparisons":completed.len(),"strict_matches":strict_matches,"inherited_control_identities":0})
    );
    assert_eq!(strict_matches, 148);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn compact_input_types_match_spark() -> TestResult {
    let records = include_str!("fixtures/array-compact-input-types/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/array-compact-input-types/manifest.json"
    ))?;
    assert_eq!(records.len(), 15);
    assert_eq!(records[0]["group"], "array-compact-input-types-891-v1");
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(
        records[0]["capture_sha256"],
        manifest["capture_script_sha256"]
    );
    let identities = records[1..]
        .iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    assert_eq!(json!(identities), manifest["identities"]);
    let mut differences = Vec::new();
    let mut completed = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch_size)),
            )
            .build(),
        );
        for case in &records[1..] {
            let comparison =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await?;
            eprintln!(
                "ARRAY_COMPACT_INPUT_TYPES_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,"comparison":comparison})
            );
            if comparison["matches"] != true || comparison["optimized_matches"] != true {
                differences.push(format!(
                    "batch={batch_size}, expected={case}, actual={comparison}"
                ));
            }
            completed += 1;
        }
    }
    assert_eq!(completed, 28);
    eprintln!(
        "ARRAY_COMPACT_INPUT_TYPES_SUMMARY {}",
        json!({"comparisons":completed,"strict_matches":completed-differences.len()})
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn compact_input_validation_keeps_error_categories() -> TestResult {
    use datafusion::error::DataFusionError;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
    use sail_plan::error::PlanError;

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for ansi_mode in [true, false] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        };
        for sql in [
            "SELECT ARRAY_COMPACT() AS r",
            "SELECT ARRAY_COMPACT(ARRAY(1), ARRAY(2)) AS r",
        ] {
            let error = plan_query(&session, sql, &options)
                .await
                .err()
                .ok_or("wrong ARRAY_COMPACT arity was accepted")?;
            let DataFusionError::External(error) = error else {
                return Err(format!("missing resolver error for {sql}: {error}").into());
            };
            assert!(
                matches!(error.downcast_ref::<PlanError>(), Some(PlanError::DataFusionError(DataFusionError::Plan(message)))
                    if message.starts_with("one value expected: ")),
                "{sql}: {error}"
            );
        }
        let error = plan_query(&session, "SELECT ARRAY_COMPACT(1) AS r", &options)
            .await
            .err()
            .ok_or("non-array ARRAY_COMPACT input was accepted")?;
        let DataFusionError::External(error) = error else {
            return Err(format!("missing resolver error: {error}").into());
        };
        assert!(matches!(
            error.downcast_ref::<PlanError>(),
            Some(PlanError::InvalidArgument(message))
                if message.starts_with("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]")
        ));
    }
    Ok(())
}

#[tokio::test]
async fn spark_compaction_preserves_native_array_queries() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let sqls = [
        ("SELECT ARRAY_HAS_ANY([1, NULL], [2]) AS r", false),
        ("SELECT ARRAY_HAS_ANY([], [NULL]) AS r", false),
        ("SELECT ARRAY_HAS_ANY(CAST(NULL AS INT[]), [1]) AS r", false),
        ("SELECT [1, NULL, 2] AS r, [1, 2] IS NULL AS n", false),
        ("SELECT CAST(NULL AS INT[]) IS NULL AS n, [] AS r", false),
        ("SELECT [CAST('bad' AS INT), NULL] IS NULL AS r", true),
    ];
    let mut expected = Vec::new();
    for (sql, fails) in sqls {
        let observation = spark_results::capture_native_query_observation(&native, sql).await?;
        if fails {
            assert_eq!(
                observation["phase"], "create_physical_plan",
                "{sql}: {observation}"
            );
            assert!(
                observation["error"].as_str().is_some_and(
                    |error| error.contains("Cannot cast string 'bad' to value of Int32 type")
                ),
                "{sql}: {observation}"
            );
            expected.push(observation);
            continue;
        }
        assert!(observation["error"].is_null(), "{sql}: {observation}");
        assert_eq!(observation["phase"], "encode", "{sql}: {observation}");
        for stage in ["public_schema", "before_collect", "physical_schema"] {
            assert!(observation[stage].is_object(), "{sql}: {observation}");
        }
        assert!(observation["rows"].is_array(), "{sql}: {observation}");
        expected.push(observation);
    }
    for after_spark in [false, true] {
        if after_spark {
            let case = json!({"sql":"SELECT ARRAY_COMPACT(ARRAY(1, NULL, 2)) AS r", "ansi":true});
            spark_results::capture_spark_query_result(&configured, &case).await?;
        }
        for ((sql, _), expected) in sqls.into_iter().zip(&expected) {
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            eprintln!(
                "ARRAY_COMPACT_NATIVE_OBSERVATION {}",
                json!({"sql":sql,"after_spark":after_spark,"native":expected,"configured":actual})
            );
            assert_eq!(&actual, expected, "{sql} after_spark={after_spark}");
        }
        for sql in [
            "SELECT ARRAY_HAS_ANY(1, [2]) AS r",
            "SELECT ARRAY_COMPACT(1) AS r",
        ] {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            assert!(expected["error"].is_string(), "{sql}: {expected}");
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            assert_eq!(actual, expected, "{sql} after_spark={after_spark}");
        }
    }
    Ok(())
}

fn encoded_integer_at(array: &dyn arrow::array::Array, row: usize) -> TestResult<Option<i32>> {
    use arrow::array::{AsArray, DictionaryArray, RunArray, UnionArray};
    use arrow::datatypes::{DataType, Int8Type, Int32Type};

    if array
        .logical_nulls()
        .is_some_and(|nulls| nulls.is_null(row))
    {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int32 => Ok(Some(array.as_primitive::<Int32Type>().value(row))),
        DataType::Dictionary(_, _) => {
            let dictionary = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int8Type>>()
                .ok_or("expected Int8 dictionary")?;
            encoded_integer_at(
                dictionary.values().as_ref(),
                dictionary.key(row).ok_or("missing dictionary key")?,
            )
        }
        DataType::RunEndEncoded(_, _) => {
            let runs = array
                .as_any()
                .downcast_ref::<RunArray<Int32Type>>()
                .ok_or("expected Int32 run ends")?;
            encoded_integer_at(runs.values().as_ref(), runs.get_physical_index(row))
        }
        DataType::Union(_, _) => {
            let union = array
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or("expected union")?;
            encoded_integer_at(
                union.child(union.type_id(row)).as_ref(),
                union.value_offset(row),
            )
        }
        other => Err(format!("unexpected integer encoding: {other}").into()),
    }
}

#[tokio::test]
async fn compact_encoded_storage_boundaries() -> TestResult {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, ListArray, NullArray, RunArray,
        UnionArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Int8Type, Int32Type, Schema, UnionFields};
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    };
    use datafusion::physical_plan::collect;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
    use sail_function::scalar::array::spark_array_compact::SparkArrayCompact;

    let union_fields = UnionFields::try_new(
        vec![0, 1],
        vec![
            Field::new("integer", DataType::Int32, true),
            Field::new("unused", DataType::Null, true),
        ],
    )?;
    let cases: [(&str, ArrayRef, ArrayRef, Vec<Option<i32>>); 3] = [
        (
            "inactive_null_union",
            Arc::new(UnionArray::try_new(
                union_fields.clone(),
                vec![0, 0, 0, 0].into(),
                None,
                vec![
                    Arc::new(Int32Array::from(vec![900, 3, 4, 800])),
                    Arc::new(NullArray::new(4)),
                ],
            )?),
            Arc::new(UnionArray::try_new(
                union_fields,
                vec![1].into(),
                None,
                vec![
                    Arc::new(Int32Array::from(vec![0])),
                    Arc::new(NullArray::new(1)),
                ],
            )?),
            vec![Some(3), Some(4)],
        ),
        (
            "sliced_hidden_null_run",
            Arc::new(RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![1, 3, 4]),
                &Int32Array::from(vec![None, Some(3), None]),
            )?),
            Arc::new(RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![1]),
                &Int32Array::from(vec![None]),
            )?),
            vec![Some(3), Some(3)],
        ),
        (
            "all_valid_dictionary_keys",
            Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::new(vec![0, 0, 1, 1].into(), Some(NullBuffer::new_valid(4))),
                Arc::new(Int32Array::from(vec![3, 4])),
            )?),
            Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0]),
                Arc::new(Int32Array::from(vec![None])),
            )?),
            vec![Some(3), Some(4)],
        ),
    ];
    for (id, values, visible_null, expected) in cases {
        values.to_data().validate_full()?;
        visible_null.to_data().validate_full()?;
        assert_eq!(visible_null.logical_null_count(), 1, "{id}");
        let element = Arc::new(Field::new_list_field(values.data_type().clone(), true));
        let required = Arc::new(element.as_ref().clone().with_nullable(false));
        assert!(
            ListArray::try_new(
                required.clone(),
                OffsetBuffer::from_lengths([1]),
                visible_null,
                None
            )
            .is_err(),
            "{id}"
        );
        let input = Arc::new(
            ListArray::try_new(element, OffsetBuffer::from_lengths([1, 2, 1]), values, None)?
                .slice(1, 1),
        );
        input.to_data().validate_full()?;
        let field = Arc::new(Field::new("a", input.data_type().clone(), false));
        let udf = SparkArrayCompact::new();
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&field),
            scalar_arguments: &[None],
        })?;
        let expected_field = Field::new("r", DataType::List(required.clone()), false);
        assert_eq!(
            return_field.as_ref().clone().with_name("r"),
            expected_field,
            "{id}"
        );
        let output = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(input.clone())],
                arg_fields: vec![field.clone()],
                number_rows: 1,
                return_field: return_field.clone(),
                config_options: Arc::default(),
            })?
            .into_array(1)?;
        output.to_data().validate_full()?;
        assert_eq!(output.data_type(), return_field.data_type(), "{id}");
        let output = output
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected list output")?;
        assert!(!output.is_null(0), "{id}");
        let integers = (0..output.value(0).len())
            .map(|row| encoded_integer_at(output.value(0).as_ref(), row))
            .collect::<TestResult<Vec<_>>>()?;
        assert_eq!(integers, expected, "{id}");
        assert_eq!(output.values().logical_null_count(), 0, "{id}");
        let required_constructor_error = ListArray::try_new(
            required,
            output.offsets().clone(),
            output.values().clone(),
            output.nulls().cloned(),
        )
        .err()
        .map(|error| error.to_string());
        let session = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
        );
        session.register_batch(
            "encoded_arrays",
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![input])?,
        )?;
        let mut phase = "resolve";
        let public_result: TestResult<Value> = async {
            let query = plan_query(&session, "SELECT ARRAY_COMPACT(a) AS r FROM encoded_arrays", &SparkSqlOptions::default()).await?;
            phase = "public_schema";
            assert_eq!(query.column_names, vec!["r".to_owned()], "{id}");
            assert_eq!(query.logical_plan.schema().fields().len(), query.column_names.len(), "{id}");
            let public_fields = query.logical_plan.schema().fields().iter()
                .zip(&query.column_names)
                .map(|(field, name)| field.as_ref().clone().with_name(name))
                .collect::<Vec<_>>();
            assert_eq!(public_fields, vec![expected_field.clone()], "{id}");
            phase = "create_physical_plan";
            let physical = query.create_physical_plan(&session).await?;
            phase = "physical_schema";
            assert_eq!(physical.schema().fields().len(), query.column_names.len(), "{id}");
            assert_eq!(physical.schema().field(0), &expected_field, "{id}");
            phase = "collect";
            let batches = collect(physical.clone(), session.task_ctx()).await?;
            let mut rows = Vec::new();
            for batch in batches {
                spark_results::validate_batch(physical.schema().as_ref(), &batch)?;
                let array = batch.column(0).as_any().downcast_ref::<ListArray>().ok_or("expected public list")?;
                for row in 0..array.len() {
                    assert!(!array.is_null(row), "{id}");
                    let values = array.value(row);
                    rows.push((0..values.len()).map(|row| encoded_integer_at(values.as_ref(), row)).collect::<TestResult<Vec<_>>>()?);
                }
            }
            assert_eq!(rows, vec![expected.clone()], "{id}");
            phase = "complete";
            Ok(json!({"rows":rows,"public_fields":format!("{:?}", query.logical_plan.schema().fields()),"physical_fields":format!("{:?}", physical.schema().fields())}))
        }.await;
        eprintln!(
            "ARRAY_COMPACT_STORAGE_OBSERVATION {}",
            json!({"id":id,"expected":expected,"direct_rows":[integers],"direct_valid":true,"logical_null_count":output.values().logical_null_count(),"storage_nullable":output.values().is_nullable(),"required_constructor_error":required_constructor_error,"visible_null_rejected":true,"public_phase":phase,"public":public_result.as_ref().ok(),"public_error":public_result.as_ref().err().map(ToString::to_string)})
        );
        assert!(
            required_constructor_error.is_none(),
            "{id}: {required_constructor_error:?}"
        );
        assert!(public_result.is_ok(), "{id}: {public_result:?}");
    }
    Ok(())
}
