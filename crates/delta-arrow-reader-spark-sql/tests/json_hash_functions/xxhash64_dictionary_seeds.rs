//! Nested dictionary values continue XXHASH64 row seeds while root dictionaries stay encoded.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, Int16Array, Int64Array,
    ListArray, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int8Type, Int16Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use sail_function::scalar::hash::xxhash64_spark_input_encoding::Xxhash64WithSparkInputEncoding;

use datafusion::arrow::compute::concat_batches;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

use super::{spark_inputs, spark_results};
use spark_results::{TestResult, capture_executed_query_result, capture_native_query_observation};

fn references() -> TestResult<Vec<Value>> {
    let request: Value = serde_json::from_str(include_str!(
        "../fixtures/xxhash64-dictionary-seeds/requests.json"
    ))?;
    let records = include_str!("../fixtures/xxhash64-dictionary-seeds/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["expected_record_count"], 8);
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    let definitions = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    assert_eq!(definitions.len(), 8);
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

#[tokio::test]
async fn xxhash64_nested_dictionaries_match_spark_and_preserve_roots() -> TestResult {
    let records = references()?;
    let inputs = &records[0]["corpora"][0]["metadata"];
    let native_sql = "SELECT id,md5(root_s) AS digest,sha256(root_s) AS sha FROM dictionary_seed_inputs ORDER BY id";
    let mut differences = Vec::new();
    for storage in ["plain", "dictionary_children"] {
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
                register_inputs(
                    context,
                    inputs,
                    input_batch,
                    provider_partitions,
                    storage == "dictionary_children",
                )?;
            }
            let state = session.state();
            for input in inputs["inputs"].as_array().ok_or("missing inputs")? {
                let name = input["name"].as_str().ok_or("missing input name")?;
                let scan = session
                    .table_provider(name)
                    .await?
                    .scan(&state, None, &[], None)
                    .await?;
                assert_eq!(
                    scan.output_partitioning().partition_count(),
                    provider_partitions
                );
                let expected = spark_inputs::decode_recorded_batch(input)?;
                let batches = session
                    .sql(&format!("SELECT * FROM {name} ORDER BY id"))
                    .await?
                    .collect()
                    .await?;
                let actual = concat_batches(&batches[0].schema(), &batches)?;
                let columns = actual
                    .columns()
                    .iter()
                    .zip(expected.columns())
                    .map(|(column, expected)| cast(column, expected.data_type()))
                    .collect::<Result<Vec<_>, _>>()?;
                assert_eq!(RecordBatch::try_new(expected.schema(), columns)?, expected);
            }
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
                    "XXHASH64_DICTIONARY_OBSERVATION {}",
                    json!({"id":case["id"],"ansi":case["ansi"],
                    "storage":storage,"provider_partitions":provider_partitions,"input_batch":input_batch,
                    "target_partitions":target_partitions,"output_batch":output_batch,
                    "matches":matches,"actual":actual.as_ref().map_err(ToString::to_string)})
                );
                if !matches {
                    differences.push(format!(
                    "{} ANSI={} storage={storage} layout=({provider_partitions},{input_batch},{target_partitions},{output_batch}): expected {}, got {actual:?}",
                    case["id"],case["ansi"],case["expected"]
                ));
                }
            }
            let native_after = capture_native_query_observation(&session, native_sql).await?;
            assert_eq!(native_after, native_before);
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn dictionary_storage(values: &ArrayRef) -> TestResult<ArrayRef> {
    let logical = (0..values.len())
        .map(|row| spark_results::encode_value(values.as_ref(), row))
        .collect::<TestResult<Vec<_>>>()?;
    let mut nulls = 0;
    let keys = logical
        .iter()
        .map(|value| -> TestResult<Option<i8>> {
            if value.is_null() {
                nulls += 1;
                if nulls % 2 == 0 {
                    return Ok(None);
                }
            }
            Ok(Some(i8::try_from(
                logical
                    .iter()
                    .position(|candidate| candidate == value)
                    .ok_or("missing dictionary value")?,
            )?))
        })
        .collect::<TestResult<Vec<_>>>()?;
    Ok(Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(keys),
        Arc::clone(values),
    )?))
}

fn dictionary_batch(batch: &RecordBatch) -> TestResult<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    for column in &mut columns[1..5] {
        *column = dictionary_storage(column)?;
    }
    // An outer root dictionary keeps its own keys while its encoded values are decoded.
    columns[1] = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from_iter_values(0..6),
        Arc::clone(&columns[1]),
    )?);
    for (index, column) in columns[5..9].iter_mut().enumerate() {
        let array = column
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected list")?;
        let values = dictionary_storage(array.values())?;
        let values: ArrayRef = if index == 0 {
            let inner = values
                .as_any()
                .downcast_ref::<DictionaryArray<Int8Type>>()
                .ok_or("expected inner dictionary")?;
            let original_keys = inner.keys().iter().collect::<Vec<_>>();
            let expanded_keys = (0..128 + original_keys.len())
                .map(|row| {
                    if row < 128 {
                        Some(0)
                    } else {
                        original_keys[row - 128]
                    }
                })
                .collect::<Vec<_>>();
            let expanded: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(expanded_keys),
                Arc::clone(inner.values()),
            )?);
            Arc::new(DictionaryArray::<Int16Type>::try_new(
                Int16Array::from_iter_values(128..128 + i16::try_from(values.len())?),
                expanded,
            )?)
        } else {
            values
        };
        let DataType::List(item) = array.data_type() else {
            return Err("expected list item".into());
        };
        *column = Arc::new(ListArray::new(
            Arc::new(
                item.as_ref()
                    .clone()
                    .with_data_type(values.data_type().clone()),
            ),
            array.offsets().clone(),
            values,
            array.nulls().cloned(),
        ));
    }
    for column in &mut columns[9..] {
        let array = column
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected struct")?;
        let values = array
            .columns()
            .iter()
            .map(dictionary_storage)
            .collect::<TestResult<Vec<_>>>()?;
        let fields = array
            .fields()
            .iter()
            .zip(&values)
            .map(|(field, child)| {
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(child.data_type().clone()),
                )
            })
            .collect::<Vec<_>>();
        *column = Arc::new(StructArray::new(
            fields.into(),
            values,
            array.nulls().cloned(),
        ));
    }
    let fields = batch
        .schema()
        .fields()
        .iter()
        .zip(&columns)
        .map(|(field, array)| {
            field
                .as_ref()
                .clone()
                .with_data_type(array.data_type().clone())
        })
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        Arc::new(datafusion::arrow::datatypes::Schema::new(fields)),
        columns,
    )?)
}

fn register_inputs(
    session: &SessionContext,
    inputs: &Value,
    batch_size: usize,
    partition_count: usize,
    encoded: bool,
) -> TestResult {
    let input = &inputs["inputs"][0];
    let mut batch = spark_inputs::decode_recorded_batch(input)?;
    if encoded {
        batch = dictionary_batch(&batch)?;
    }
    let mut partitions = vec![Vec::new(); partition_count];
    for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
        partitions[chunk % partition_count]
            .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
    }
    session.register_table(
        "dictionary_seed_inputs",
        Arc::new(MemTable::try_new(batch.schema(), partitions)?),
    )?;
    Ok(())
}

fn invoke_hash(arrays: Vec<ArrayRef>) -> TestResult<Int64Array> {
    let function = Xxhash64WithSparkInputEncoding::default();
    let length = arrays[0].len();
    let fields = arrays
        .iter()
        .enumerate()
        .map(|(index, array)| {
            Arc::new(Field::new(
                format!("arg{index}"),
                array.data_type().clone(),
                true,
            ))
        })
        .collect::<Vec<_>>();
    let output = function.return_field_from_args(ReturnFieldArgs {
        arg_fields: &fields,
        scalar_arguments: &vec![None; fields.len()],
    })?;
    let result = function
        .invoke_with_args(ScalarFunctionArgs {
            args: arrays.into_iter().map(ColumnarValue::Array).collect(),
            arg_fields: fields,
            number_rows: length,
            return_field: output,
            config_options: Arc::default(),
        })?
        .into_array(length)?;
    Ok(result
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("expected BIGINT output")?
        .clone())
}

#[test]
fn dictionary_keys_offsets_and_supported_lists_preserve_spark_values() -> TestResult {
    let records = references()?;
    let case = records[1..]
        .iter()
        .find(|case| case["id"] == "container_values" && case["ansi"] == false)
        .ok_or("missing container values")?;
    let recorded = |row: usize, column: usize| -> TestResult<i64> {
        Ok(case["expected"]["rows"][row][column]
            .as_str()
            .ok_or("missing BIGINT")?
            .parse()?)
    };
    let plain =
        spark_inputs::decode_recorded_batch(&records[0]["corpora"][0]["metadata"]["inputs"][0])?;
    let encoded = dictionary_batch(&plain)?;
    let list = encoded
        .column(5)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or("expected list")?;
    let outer = list
        .values()
        .as_any()
        .downcast_ref::<DictionaryArray<Int16Type>>()
        .ok_or("expected outer dictionary")?;
    assert_eq!(outer.keys().value(0), 128);
    assert!(outer.values().len() > 127);
    let inner = outer
        .values()
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .ok_or("expected inner dictionary")?;
    assert!(inner.keys().null_count() > 0);
    assert!(inner.values().null_count() > 0);
    assert_eq!(inner.keys().value(128), inner.keys().value(134));
    let sliced = encoded.slice(1, 4);
    let sliced_list = sliced
        .column(5)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or("expected sliced list")?;
    assert!(sliced_list.value_offsets()[0] > 0);
    assert_eq!(
        invoke_hash(vec![Arc::clone(sliced.column(5))])?,
        Int64Array::from(
            (1..5)
                .map(|row| recorded(row, 1))
                .collect::<TestResult<Vec<_>>>()?
        )
    );
    let DataType::List(item) = encoded.column(5).data_type() else {
        return Err("expected list item".into());
    };
    let large = cast(encoded.column(5), &DataType::LargeList(Arc::clone(item)))?;
    assert_eq!(
        invoke_hash(vec![large])?,
        Int64Array::from(
            (0..6)
                .map(|row| recorded(row, 1))
                .collect::<TestResult<Vec<_>>>()?
        )
    );
    let children = datafusion::arrow::compute::concat(&[
        list.value(0).as_ref(),
        list.value(0).as_ref(),
        list.value(5).as_ref(),
    ])?;
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        Arc::clone(item),
        3,
        children,
        Some(datafusion::arrow::buffer::NullBuffer::from(vec![
            true, false, true,
        ])),
    ));
    assert_eq!(
        invoke_hash(vec![fixed])?,
        Int64Array::from(vec![recorded(0, 1)?, recorded(3, 1)?, recorded(5, 1)?])
    );
    Ok(())
}
