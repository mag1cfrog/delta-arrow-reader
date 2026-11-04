//! ARRAY_COMPACT fields and consumers match independently captured Spark results.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
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

async fn capture_compact_comparison(session: &SessionContext, case: &Value) -> Value {
    let observation = async {
        let mut comparison =
            spark_results::compare_exact_spark_query_stages(session, case).await?;
        let optimized = spark_results::capture_spark_optimized_observation(session, case).await;
        comparison["optimized_matches"] = json!(match &optimized {
            Ok(observation) =>
                observation["names_match"] == true
                    && Some(&observation["schema"]) == case["schema_stages"].get("optimized"),
            Err(error) =>
                case["schema_stages"].get("optimized").is_none()
                    && comparison["error_matches"] == true
                    && matches!(comparison["native_phase"].as_str(), Some("resolve" | "create_physical_plan"))
                    && comparison["error"].as_str() == Some(error.to_string().as_str()),
        });
        comparison["optimized_observation"] = optimized.as_ref().ok().cloned().unwrap_or(Value::Null);
        comparison["optimized_error"] = json!(optimized.as_ref().err().map(ToString::to_string));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(comparison)
    };
    match std::panic::AssertUnwindSafe(observation).catch_unwind().await {
        Ok(Ok(observation)) => observation,
        Ok(Err(error)) => json!({"matches":false,"observation_error":error.to_string()}),
        Err(payload) => {
            let panic = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|text| (*text).to_owned()))
                .unwrap_or_else(|| "non-string panic payload".into());
            json!({"matches":false,"panic":panic})
        }
    }
}

#[tokio::test]
async fn compact_result_fields_match_spark() -> TestResult {
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
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
        assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
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
                Ok((case["id"].as_str().ok_or("missing ID")?, case["ansi"].as_bool().ok_or("missing ANSI")?))
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
                        .with_config(SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2)),
                )
                .build(),
            );
            spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
            for case in &cases {
                let mut comparison = capture_compact_comparison(&session, case).await;
                comparison["group"] = json!(group);
                comparison["id"] = case["id"].clone();
                comparison["ansi"] = case["ansi"].clone();
                comparison["batch"] = json!(batch_size);
                assert!(completed.insert((group.to_owned(), case["id"].to_string(), case["ansi"].to_string(), batch_size)));
                eprintln!("ARRAY_COMPACT_FIELDS_OBSERVATION {comparison}");
                if comparison["matches"] != true || comparison["optimized_matches"] != true {
                    differences.push(format!("{group} {} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}", case["id"], case["ansi"], case["expected"], case["schema_stages"]));
                }
            }
        }
    }
    assert_eq!(completed.len(), 148);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_compaction_preserves_successful_native_array_queries() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let sqls = [
        "SELECT ARRAY_HAS_ANY([1, NULL], [2]) AS r",
        "SELECT ARRAY_HAS_ANY([], [NULL]) AS r",
        "SELECT ARRAY_HAS_ANY(CAST(NULL AS INT[]), [1]) AS r",
        "SELECT [1, NULL, 2] AS r, [1, 2] IS NULL AS n",
        "SELECT CAST(NULL AS INT[]) IS NULL AS n, [] AS r",
    ];
    let mut expected = Vec::new();
    for sql in sqls {
        let observation = spark_results::capture_native_query_observation(&native, sql).await?;
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
        for (sql, expected) in sqls.into_iter().zip(&expected) {
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            eprintln!("ARRAY_COMPACT_NATIVE_OBSERVATION {}", json!({"sql":sql,"after_spark":after_spark,"native":expected,"configured":actual}));
            assert_eq!(&actual, expected, "{sql} after_spark={after_spark}");
        }
    }
    Ok(())
}

fn encoded_integer_at(array: &dyn arrow::array::Array, row: usize) -> TestResult<Option<i32>> {
    use arrow::array::{AsArray, DictionaryArray, RunArray, UnionArray};
    use arrow::datatypes::{DataType, Int8Type, Int32Type};

    if array.logical_nulls().is_some_and(|nulls| nulls.is_null(row)) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int32 => Ok(Some(array.as_primitive::<Int32Type>().value(row))),
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any().downcast_ref::<DictionaryArray<Int8Type>>().ok_or("expected Int8 dictionary")?;
            encoded_integer_at(dictionary.values().as_ref(), dictionary.key(row).ok_or("missing dictionary key")?)
        }
        DataType::RunEndEncoded(_, _) => {
            let runs = array.as_any().downcast_ref::<RunArray<Int32Type>>().ok_or("expected Int32 run ends")?;
            encoded_integer_at(runs.values().as_ref(), runs.get_physical_index(row))
        }
        DataType::Union(_, _) => {
            let union = array.as_any().downcast_ref::<UnionArray>().ok_or("expected union")?;
            encoded_integer_at(union.child(union.type_id(row)).as_ref(), union.value_offset(row))
        }
        other => Err(format!("unexpected integer encoding: {other}").into()),
    }
}

#[tokio::test]
async fn compact_encoded_storage_boundaries() -> TestResult {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, ListArray, NullArray, RunArray, UnionArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Int8Type, Int32Type, Schema, UnionFields};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::collect;
    use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
    use sail_function::scalar::array::spark_array_compact::SparkArrayCompact;

    let union_fields = UnionFields::try_new(
        vec![0, 1],
        vec![Field::new("integer", DataType::Int32, true), Field::new("unused", DataType::Null, true)],
    )?;
    let cases: [(&str, ArrayRef, ArrayRef, Vec<Option<i32>>); 3] = [
        (
            "inactive_null_union",
            Arc::new(UnionArray::try_new(union_fields.clone(), vec![0, 0, 0, 0].into(), None, vec![Arc::new(Int32Array::from(vec![900, 3, 4, 800])), Arc::new(NullArray::new(4))])?),
            Arc::new(UnionArray::try_new(union_fields, vec![1].into(), None, vec![Arc::new(Int32Array::from(vec![0])), Arc::new(NullArray::new(1))])?),
            vec![Some(3), Some(4)],
        ),
        (
            "sliced_hidden_null_run",
            Arc::new(RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1, 3, 4]), &Int32Array::from(vec![None, Some(3), None]))?),
            Arc::new(RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1]), &Int32Array::from(vec![None]))?),
            vec![Some(3), Some(3)],
        ),
        (
            "all_valid_dictionary_keys",
            Arc::new(DictionaryArray::<Int8Type>::try_new(Int8Array::new(vec![0, 0, 1, 1].into(), Some(NullBuffer::new_valid(4))), Arc::new(Int32Array::from(vec![3, 4])))?),
            Arc::new(DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), Arc::new(Int32Array::from(vec![None])))?),
            vec![Some(3), Some(4)],
        ),
    ];
    for (id, values, visible_null, expected) in cases {
        values.to_data().validate_full()?;
        visible_null.to_data().validate_full()?;
        assert_eq!(visible_null.logical_null_count(), 1, "{id}");
        let element = Arc::new(Field::new_list_field(values.data_type().clone(), true));
        let required = Arc::new(element.as_ref().clone().with_nullable(false));
        assert!(ListArray::try_new(required.clone(), OffsetBuffer::from_lengths([1]), visible_null, None).is_err(), "{id}");
        let input = Arc::new(ListArray::try_new(element, OffsetBuffer::from_lengths([1, 2, 1]), values, None)?.slice(1, 1));
        input.to_data().validate_full()?;
        let field = Arc::new(Field::new("a", input.data_type().clone(), false));
        let udf = SparkArrayCompact::new();
        let return_field = udf.return_field_from_args(ReturnFieldArgs { arg_fields: std::slice::from_ref(&field), scalar_arguments: &[None] })?;
        let output = udf.invoke_with_args(ScalarFunctionArgs { args: vec![ColumnarValue::Array(input.clone())], arg_fields: vec![field.clone()], number_rows: 1, return_field, config_options: Arc::default() })?.into_array(1)?;
        output.to_data().validate_full()?;
        let output = output.as_any().downcast_ref::<ListArray>().ok_or("expected list output")?;
        assert!(!output.is_null(0), "{id}");
        let integers = (0..output.value(0).len()).map(|row| encoded_integer_at(output.value(0).as_ref(), row)).collect::<TestResult<Vec<_>>>()?;
        assert_eq!(integers, expected, "{id}");
        assert_eq!(output.values().logical_null_count(), 0, "{id}");
        let required_constructor_error = ListArray::try_new(required, output.offsets().clone(), output.values().clone(), output.nulls().cloned()).err().map(|error| error.to_string());
        let session = SessionContext::new_with_state(configure_spark_sql(SessionStateBuilder::new().with_default_features()).build());
        session.register_batch("encoded_arrays", RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![input])?)?;
        let mut phase = "resolve";
        let public_result: TestResult<Value> = async {
            let query = plan_query(&session, "SELECT ARRAY_COMPACT(a) AS r FROM encoded_arrays", &SparkSqlOptions::default()).await?;
            phase = "create_physical_plan";
            let physical = query.create_physical_plan(&session).await?;
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
        eprintln!("ARRAY_COMPACT_STORAGE_OBSERVATION {}", json!({"id":id,"expected":expected,"direct_rows":[integers],"direct_valid":true,"logical_null_count":output.values().logical_null_count(),"storage_nullable":output.values().is_nullable(),"required_constructor_error":required_constructor_error,"visible_null_rejected":true,"public_phase":phase,"public":public_result.as_ref().ok(),"public_error":public_result.as_ref().err().map(ToString::to_string)}));
        assert!(required_constructor_error.is_none(), "{id}: {required_constructor_error:?}");
        assert!(public_result.is_ok(), "{id}: {public_result:?}");
    }
    Ok(())
}
