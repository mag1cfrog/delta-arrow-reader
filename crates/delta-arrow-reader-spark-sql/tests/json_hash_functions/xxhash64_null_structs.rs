//! XXHASH64 ignores fields hidden beneath NULL STRUCT parents at every nesting level.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, Int32Array, Int64Array,
    ListArray, StringArray, StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Int8Type, Int32Type};
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
        "../fixtures/xxhash64-null-structs/requests.json"
    ))?;
    let records = include_str!("../fixtures/xxhash64-null-structs/spark-reference.jsonl")
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
async fn xxhash64_null_structs_match_spark_and_preserve_inputs() -> TestResult {
    let records = references()?;
    let inputs = &records[0]["corpora"][0]["metadata"];
    let native_sql = "SELECT id,md5(CAST(id AS STRING)) AS digest,sha256(CAST(id AS STRING)) AS sha FROM null_struct_inputs ORDER BY id";
    let mut differences = Vec::new();
    for storage in ["plain", "masked_sliced"] {
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
                    storage == "masked_sliced",
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
                assert_eq!(concat_batches(&expected.schema(), &batches)?, expected);
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
                    "XXHASH64_NULL_STRUCT_OBSERVATION {}",
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

fn masked_nested_batch(batch: &RecordBatch) -> TestResult<RecordBatch> {
    let root = batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("expected root struct")?;
    let root = StructArray::new(
        root.fields().clone(),
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
        root.nulls().cloned(),
    );
    assert!(root.is_null(3));
    assert!(!root.column(0).is_null(3));
    assert!(!root.column(1).is_null(3));
    let DataType::List(item) = batch.column(2).data_type() else {
        return Err("expected struct list".into());
    };
    let DataType::Struct(fields) = item.data_type() else {
        return Err("expected struct elements".into());
    };
    let elements: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(777),
                Some(1),
                Some(333),
                Some(2),
                None,
                Some(555),
                Some(556),
                Some(-1),
                Some(1),
                Some(333),
                Some(2),
                Some(888),
            ])),
            Arc::new(StringArray::from(vec![
                Some("prefix"),
                Some("x"),
                Some("hidden-element"),
                None,
                None,
                Some("hidden-list"),
                Some("hidden-list2"),
                Some("z"),
                Some("x"),
                Some("hidden-element"),
                None,
                Some("suffix"),
            ])),
        ],
        Some(NullBuffer::from(vec![
            true, true, false, true, true, true, true, true, true, false, true, true,
        ])),
    ));
    let lists = ListArray::new(
        Arc::clone(item),
        OffsetBuffer::new(vec![0, 1, 4, 4, 5, 7, 8, 11, 12].into()),
        elements,
        Some(NullBuffer::from(vec![
            true, true, true, true, false, true, true, true,
        ])),
    )
    .slice(1, 6);
    assert_eq!(lists.value_offsets()[0], 1);
    assert!(lists.is_null(3));
    assert_eq!(lists.value(3).null_count(), 0);
    let row = lists.value(0);
    let row = row
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("expected struct row")?;
    assert!(row.is_null(1));
    assert!(!row.column(0).is_null(1));
    assert!(!row.column(1).is_null(1));
    let original = batch
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("expected struct of arrays")?;
    let arrays = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), None, Some(2)]),
        Some(vec![]),
        None,
        Some(vec![Some(9), Some(99)]),
        Some(vec![Some(-1)]),
        Some(vec![Some(1), None, Some(2)]),
    ]);
    let struct_array = StructArray::new(
        original.fields().clone(),
        vec![
            Arc::new(arrays),
            Arc::new(Int32Array::from(vec![
                Some(3),
                None,
                Some(0),
                Some(777),
                Some(2),
                Some(3),
            ])),
        ],
        original.nulls().cloned(),
    );
    assert!(struct_array.is_null(3));
    assert!(!struct_array.column(0).is_null(3));
    let original = batch
        .column(4)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("expected deep struct")?;
    let inner = original
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("expected inner struct")?;
    let inner = StructArray::new(
        inner.fields().clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(444),
                None,
                Some(999018),
                Some(-1),
                Some(1),
            ])),
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("hidden-inner"),
                Some(""),
                Some("hidden-deep"),
                Some("z"),
                Some("x"),
            ])),
        ],
        Some(NullBuffer::from(vec![true, false, true, true, true, true])),
    );
    assert!(inner.is_null(1));
    assert!(!inner.column(0).is_null(1));
    let deep = StructArray::new(
        original.fields().clone(),
        vec![
            Arc::new(inner),
            Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                None,
                Some(999019),
                Some(5),
                Some(1),
            ])),
        ],
        original.nulls().cloned(),
    );
    assert!(deep.is_null(3));
    assert!(!deep.column(0).is_null(3));
    Ok(RecordBatch::try_new(
        batch.schema(),
        vec![
            Arc::clone(batch.column(0)),
            Arc::new(root),
            Arc::new(lists),
            Arc::new(struct_array),
            Arc::new(deep),
        ],
    )?)
}

fn register_inputs(
    session: &SessionContext,
    inputs: &Value,
    batch_size: usize,
    partition_count: usize,
    masked: bool,
) -> TestResult {
    let input = &inputs["inputs"][0];
    let mut batch = spark_inputs::decode_recorded_batch(input)?;
    if masked {
        batch = masked_nested_batch(&batch)?;
    }
    let mut partitions = vec![Vec::new(); partition_count];
    for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
        partitions[chunk % partition_count]
            .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
    }
    session.register_table(
        "null_struct_inputs",
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
fn nullable_struct_storage_preserves_parent_masks_and_seeds() -> TestResult {
    let records = references()?;
    let recorded = |id: &str, column: usize| -> TestResult<Int64Array> {
        let case = records[1..]
            .iter()
            .find(|case| case["id"] == id && case["ansi"] == false)
            .ok_or("missing reference case")?;
        Ok(Int64Array::from(
            case["expected"]["rows"]
                .as_array()
                .ok_or("missing rows")?
                .iter()
                .map(|row| -> TestResult<i64> {
                    Ok(row[column].as_str().ok_or("missing BIGINT")?.parse()?)
                })
                .collect::<TestResult<Vec<_>>>()?,
        ))
    };
    let source =
        spark_inputs::decode_recorded_batch(&records[0]["corpora"][0]["metadata"]["inputs"][0])?;
    let batch = masked_nested_batch(&source)?;
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from_iter_values(0..6),
        Arc::clone(batch.column(1)),
    )?);
    assert_eq!(
        invoke_hash(vec![Arc::clone(&dictionary)])?,
        recorded("struct_columns", 1)?
    );
    assert_eq!(
        invoke_hash(vec![Arc::clone(batch.column(0)), Arc::clone(&dictionary)])?,
        recorded("seeded_columns", 1)?
    );
    assert_eq!(
        invoke_hash(vec![Arc::clone(&dictionary), Arc::clone(batch.column(0))])?,
        recorded("seeded_columns", 2)?
    );
    assert_eq!(
        invoke_hash(vec![dictionary.slice(1, 4)])?,
        recorded("struct_columns", 1)?.slice(1, 4)
    );
    let DataType::List(item) = batch.column(2).data_type() else {
        return Err("expected struct list".into());
    };
    let large =
        datafusion::arrow::compute::cast(batch.column(2), &DataType::LargeList(Arc::clone(item)))?;
    assert_eq!(invoke_hash(vec![large])?, recorded("struct_columns", 4)?);
    let lists = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or("expected lists")?;
    let values = datafusion::arrow::compute::concat(&[
        lists.value(0).as_ref(),
        lists.value(3).slice(0, 2).as_ref(),
        lists.value(0).slice(0, 1).as_ref(),
        lists.value(5).as_ref(),
    ])?;
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        Arc::clone(item),
        3,
        values,
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let hashes = recorded("struct_columns", 4)?;
    assert_eq!(
        invoke_hash(vec![fixed])?,
        Int64Array::from(vec![hashes.value(0), hashes.value(3), hashes.value(5)])
    );
    Ok(())
}
