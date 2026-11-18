//! HASH traverses ordered ARRAY elements and STRUCT fields without resetting row seeds.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, Int32Array,
    LargeListViewArray, ListArray, ListViewArray, StringArray, StringDictionaryBuilder,
    StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int8Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use sail_function::scalar::hash::spark_murmur3_hash::spark_murmur3_hash;
use serde_json::{Value, json};

use super::{spark_inputs, spark_results};
use spark_results::{TestResult, capture_executed_query_result, capture_native_query_observation};

fn references() -> TestResult<Vec<Value>> {
    let request: Value =
        serde_json::from_str(include_str!("../fixtures/hash-nested-values/requests.json"))?;
    let records = include_str!("../fixtures/hash-nested-values/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["expected_record_count"], 10);
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    let definitions = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    assert_eq!(definitions.len(), 10);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(identities.insert((id, ansi)), "duplicate {id} ANSI={ansi}");
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == id && definition["ansi"] == ansi)
            .ok_or("unknown reference case")?;
        assert_eq!(case["sql"], definition["sql"], "{id}");
        assert_eq!(case["status"], "ok", "{case}");
        assert_eq!(
            case["effective_settings"]["spark.sql.ansi.enabled"],
            ansi.to_string()
        );
        assert_eq!(
            case["schema_stage_availability"]["executed"]["available"],
            true
        );
    }
    assert_eq!(identities.len(), definitions.len());
    Ok(records)
}

fn result_types(schema: &Value) -> TestResult<Vec<Value>> {
    let mut schema = schema.clone();
    spark_results::remove_nullability_and_metadata(&mut schema);
    Ok(schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| field["type"].clone())
        .collect())
}

fn masked_sliced_batch(expected: &RecordBatch) -> TestResult<RecordBatch> {
    let DataType::List(item) = expected.column(1).data_type() else {
        return Err("expected list".into());
    };
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(777901),
        Some(1),
        None,
        Some(2),
        None,
        Some(998017),
        Some(998018),
        Some(-1),
        Some(2),
        Some(1),
        None,
        Some(2),
        Some(888902),
    ]));
    let lists = ListArray::new(
        Arc::clone(item),
        OffsetBuffer::new(vec![0, 1, 4, 4, 5, 7, 9, 12, 13].into()),
        values,
        Some(NullBuffer::from(vec![
            true, true, true, true, false, true, true, true,
        ])),
    )
    .slice(1, 6);
    assert_eq!(lists.value_offsets()[0], 1);
    assert!(lists.is_null(3));
    assert_eq!(lists.value(3).len(), 2);
    assert_eq!(lists.value(3).null_count(), 0);
    let original = expected
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("expected struct")?;
    let fields = original.fields().clone();
    let structure = StructArray::new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                None,
                Some(999017),
                Some(-1),
                Some(1),
            ])),
            Arc::new(StringArray::from(vec![
                Some("x"),
                None,
                Some(""),
                Some("hidden-parent"),
                Some("z"),
                Some("x"),
            ])),
        ],
        original.nulls().cloned(),
    );
    assert!(structure.is_null(3));
    assert!(!structure.column(0).is_null(3));
    assert!(!structure.column(1).is_null(3));
    let mut columns = expected.columns().to_vec();
    columns[1] = Arc::new(lists);
    columns[3] = Arc::new(structure);
    Ok(RecordBatch::try_new(expected.schema(), columns)?)
}

fn dictionary_children_batch(batch: &RecordBatch) -> TestResult<RecordBatch> {
    let list = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or("expected list")?;
    let dictionary_values: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(1),
        Some(2),
        Some(-1),
        Some(777901),
        Some(998017),
        Some(998018),
        Some(888902),
    ]));
    let keys = Int8Array::from(vec![
        Some(4),
        Some(1),
        Some(0),
        Some(2),
        None,
        Some(5),
        Some(6),
        Some(3),
        Some(2),
        Some(1),
        None,
        Some(2),
        Some(7),
    ]);
    let values: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
        keys,
        dictionary_values,
    )?);
    assert!(!values.is_null(2));
    let item = Arc::new(Field::new("item", values.data_type().clone(), true));
    let lists = ListArray::new(item, list.offsets().clone(), values, list.nulls().cloned());
    let original = batch
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("expected struct")?;
    let strings = original
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("expected strings")?;
    let mut builder = StringDictionaryBuilder::<Int8Type>::new();
    for value in strings.iter() {
        if let Some(value) = value {
            builder.append(value)?;
        } else {
            builder.append_null();
        }
    }
    let strings: ArrayRef = Arc::new(builder.finish());
    let fields = vec![
        Arc::clone(&original.fields()[0]),
        Arc::new(
            original.fields()[1]
                .as_ref()
                .clone()
                .with_data_type(strings.data_type().clone()),
        ),
    ];
    let structure = StructArray::new(
        fields.into(),
        vec![Arc::clone(original.column(0)), strings],
        original.nulls().cloned(),
    );
    let mut columns = batch.columns().to_vec();
    columns[1] = Arc::new(lists);
    columns[3] = Arc::new(structure);
    let fields = batch
        .schema()
        .fields()
        .iter()
        .zip(&columns)
        .map(|(field, column)| {
            field
                .as_ref()
                .clone()
                .with_data_type(column.data_type().clone())
        })
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn assert_logical_input(batch: &RecordBatch, expected: &RecordBatch) -> TestResult {
    assert_eq!(batch.num_rows(), expected.num_rows());
    for (column, expected_column) in batch.columns().iter().zip(expected.columns()) {
        let decoded = cast(column, expected_column.data_type())?;
        for row in 0..batch.num_rows() {
            assert_eq!(
                spark_results::encode_value(decoded.as_ref(), row)?,
                spark_results::encode_value(expected_column.as_ref(), row)?
            );
        }
    }
    Ok(())
}

fn register_input(
    session: &SessionContext,
    batch: &RecordBatch,
    batch_size: usize,
    partition_count: usize,
) -> TestResult {
    let mut partitions = vec![Vec::new(); partition_count];
    for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
        partitions[chunk % partition_count]
            .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
    }
    session.register_table(
        "nested_hash_inputs",
        Arc::new(MemTable::try_new(batch.schema(), partitions)?),
    )?;
    Ok(())
}

#[tokio::test]
async fn hash_nested_values_match_spark_with_masks_slices_and_dictionary_children() -> TestResult {
    let records = references()?;
    let expected =
        spark_inputs::decode_recorded_batch(&records[0]["corpora"][0]["metadata"]["inputs"][0])?;
    let masked = masked_sliced_batch(&expected)?;
    let dictionary = dictionary_children_batch(&masked)?;
    let native_sql = "SELECT id, md5(CAST(id AS STRING)) AS digest, sha256(CAST(id AS STRING)) AS sha FROM nested_hash_inputs ORDER BY id";
    let mut differences = Vec::new();
    for (storage, input) in [
        ("plain", &expected),
        ("masked_sliced", &masked),
        ("dictionary_children", &dictionary),
    ] {
        assert_logical_input(input, &expected)?;
        for (provider_partitions, input_batch, target_partitions, output_batch) in
            [(1, 1, 1, 1), (3, 2, 4, 3), (2, 32, 2, 32)]
        {
            let config = SessionConfig::new()
                .with_batch_size(output_batch)
                .with_target_partitions(target_partitions);
            let native = SessionContext::new_with_config(config.clone());
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(config),
                )
                .build(),
            );
            for context in [&native, &session] {
                register_input(context, input, input_batch, provider_partitions)?;
            }
            let scan = session
                .table_provider("nested_hash_inputs")
                .await?
                .scan(&session.state(), None, &[], None)
                .await?;
            assert_eq!(
                scan.output_partitioning().partition_count(),
                provider_partitions
            );
            let readback = session
                .sql("SELECT * FROM nested_hash_inputs ORDER BY id")
                .await?
                .collect()
                .await?;
            let readback = datafusion::arrow::compute::concat_batches(&input.schema(), &readback)?;
            assert_logical_input(&readback, &expected)?;
            let native_expected = capture_native_query_observation(&native, native_sql).await?;
            let native_before = capture_native_query_observation(&session, native_sql).await?;
            assert!(native_expected["error"].is_null(), "{native_expected}");
            assert_eq!(native_before, native_expected);
            for case in &records[1..] {
                let actual = capture_executed_query_result(&session, case).await;
                let matches = match &actual {
                    Ok(actual) => {
                        actual["rows"] == case["expected"]["rows"]
                            && result_types(&actual["schema"])?
                                == result_types(&case["schema_stages"]["executed"])?
                    }
                    Err(_) => false,
                };
                println!(
                    "HASH_NESTED_OBSERVATION {}",
                    json!({"id":case["id"],"ansi":case["ansi"],"storage":storage,"provider_partitions":provider_partitions,"input_batch":input_batch,"target_partitions":target_partitions,"output_batch":output_batch,"matches":matches,"actual":actual.as_ref().map_err(ToString::to_string)})
                );
                if !matches {
                    differences.push(format!("{} ANSI={} storage={storage} layout=({provider_partitions},{input_batch},{target_partitions},{output_batch}): expected {}, got {actual:?}",case["id"],case["ansi"],case["expected"]));
                }
            }
            assert_eq!(
                capture_native_query_observation(&session, native_sql).await?,
                native_before
            );
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn hash_with_seed(array: ArrayRef, seed: i32) -> TestResult<Int32Array> {
    let length = array.len();
    let output = spark_murmur3_hash(&[
        ColumnarValue::Array(array),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(seed))),
    ])?
    .into_array(length)?;
    Ok(output
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("expected INT hash output")?
        .clone())
}

#[test]
fn hash_list_storage_and_dictionary_cache_continue_spark_seeds() -> TestResult {
    let records = references()?;
    let recorded_hash = |id: &str, row: usize, column: usize| -> TestResult<i32> {
        let case = records[1..]
            .iter()
            .find(|case| case["id"] == id && case["ansi"] == false)
            .ok_or("missing reference case")?;
        Ok(case["expected"]["rows"][row][column]
            .as_str()
            .ok_or("missing recorded INT")?
            .parse()?)
    };
    let expected =
        spark_inputs::decode_recorded_batch(&records[0]["corpora"][0]["metadata"]["inputs"][0])?;
    let masked = masked_sliced_batch(&expected)?;
    let lists = masked
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or("expected lists")?;
    let DataType::List(item) = lists.data_type() else {
        return Err("expected list item".into());
    };
    let offsets = lists.value_offsets()[..lists.len()].to_vec();
    let sizes = lists
        .value_offsets()
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .collect::<Vec<_>>();
    let views: ArrayRef = Arc::new(ListViewArray::new(
        Arc::clone(item),
        offsets.clone().into(),
        sizes.clone().into(),
        Arc::clone(lists.values()),
        lists.nulls().cloned(),
    ));
    let large_views: ArrayRef = Arc::new(LargeListViewArray::new(
        Arc::clone(item),
        offsets
            .into_iter()
            .map(i64::from)
            .collect::<Vec<_>>()
            .into(),
        sizes.into_iter().map(i64::from).collect::<Vec<_>>().into(),
        Arc::clone(lists.values()),
        lists.nulls().cloned(),
    ));
    let large_lists = cast(masked.column(1), &DataType::LargeList(Arc::clone(item)))?;
    let expected_hashes = (0..expected.num_rows())
        .map(|row| recorded_hash("nested_columns", row, 1))
        .collect::<TestResult<Vec<_>>>()?;
    for storage in [large_lists, views, large_views] {
        assert_eq!(
            hash_with_seed(storage, 42)?,
            Int32Array::from(expected_hashes.clone())
        );
    }
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        Arc::clone(item),
        3,
        Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(2),
            Some(999017),
            Some(999018),
            Some(999019),
            Some(1),
            None,
            Some(2),
        ])),
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    assert_eq!(
        hash_with_seed(fixed, 42)?,
        Int32Array::from(vec![
            recorded_hash("nested_columns", 0, 1)?,
            recorded_hash("nested_columns", 3, 1)?,
            recorded_hash("nested_columns", 5, 1)?
        ])
    );

    // Repeated row 1 has one dictionary value, so seed 42 takes the cached path.
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 0, 0]),
        masked.column(1).slice(0, 1),
    )?);
    assert!(dictionary.len() > 1);
    assert_eq!(
        hash_with_seed(Arc::clone(&dictionary), 42)?,
        Int32Array::from(vec![recorded_hash("nested_columns", 0, 1)?; 3])
    );
    let prefix_seed = hash_with_seed(Arc::new(Int32Array::from(vec![1])), 42)?.value(0);
    assert_ne!(prefix_seed, 42);
    assert_eq!(
        hash_with_seed(dictionary, prefix_seed)?,
        Int32Array::from(vec![recorded_hash("changing_seeds", 0, 4)?; 3])
    );
    Ok(())
}
