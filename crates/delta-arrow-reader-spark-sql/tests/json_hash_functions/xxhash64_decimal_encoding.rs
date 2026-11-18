//! XXHASH64 adapts wide decimal inputs while retaining the upstream hash implementation.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Array, DictionaryArray, FixedSizeListArray, Int8Array, Int64Array,
    ListArray, StringArray, StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Int8Type};
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
        "../fixtures/xxhash64-decimal-encoding/requests.json"
    ))?;
    let records = include_str!("../fixtures/xxhash64-decimal-encoding/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["expected_record_count"], 16);
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    let definitions = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    assert_eq!(definitions.len(), 16);
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
async fn xxhash64_wide_decimals_match_spark_and_preserve_inputs() -> TestResult {
    let records = references()?;
    let inputs = &records[0]["corpora"][0]["metadata"];
    let native_sql = "SELECT 0 AS source,id,md5(label) AS digest,sha256(label) AS sha FROM decimal_inputs UNION ALL SELECT 1 AS source,id,md5(label) AS digest,sha256(label) AS sha FROM nested_decimal_inputs ORDER BY source,id";
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
                    "XXHASH64_DECIMAL_OBSERVATION {}",
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
    let DataType::List(item) = batch.column(2).data_type() else {
        return Err("expected decimal list".into());
    };
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![
            Some(999001),
            Some(7250),
            None,
            Some(-1000),
            None,
            Some(777128),
            Some(777129),
            Some(-129),
            Some(128),
            Some(7250),
            None,
            Some(-1000),
            Some(999002),
        ])
        .with_precision_and_scale(38, 3)?,
    );
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
    let original = batch
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("expected decimal struct")?;
    let structure = StructArray::new(
        original.fields().clone(),
        vec![
            Arc::new(
                Decimal128Array::from(vec![
                    Some(7250),
                    None,
                    None,
                    Some(999017),
                    Some(-129),
                    Some(7250),
                ])
                .with_precision_and_scale(38, 3)?,
            ),
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
    let mut columns = batch.columns().to_vec();
    columns[2] = Arc::new(lists);
    columns[3] = Arc::new(structure);
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn register_inputs(
    session: &SessionContext,
    inputs: &Value,
    batch_size: usize,
    partition_count: usize,
    masked: bool,
) -> TestResult {
    for input in inputs["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing input name")?;
        let mut batch = spark_inputs::decode_recorded_batch(input)?;
        if masked && name == "nested_decimal_inputs" {
            batch = masked_nested_batch(&batch)?;
        }
        let mut partitions = vec![Vec::new(); partition_count];
        for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[chunk % partition_count]
                .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        session.register_table(
            name,
            Arc::new(MemTable::try_new(batch.schema(), partitions)?),
        )?;
    }
    Ok(())
}

fn hash_array(array: ArrayRef) -> TestResult<Int64Array> {
    let function = Xxhash64WithSparkInputEncoding::default();
    let fields = vec![Arc::new(Field::new(
        "value",
        array.data_type().clone(),
        true,
    ))];
    let output = function.return_field_from_args(ReturnFieldArgs {
        arg_fields: &fields,
        scalar_arguments: &[None],
    })?;
    let length = array.len();
    let result = function
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(array)],
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
fn wide_decimal_storage_matches_recorded_spark_values() -> TestResult {
    let records = references()?;
    let case = records[1..]
        .iter()
        .find(|case| case["id"] == "registered_nested" && case["ansi"] == false)
        .ok_or("missing nested case")?;
    let expected_hash = |row: usize| -> TestResult<i64> {
        Ok(case["expected"]["rows"][row][1]
            .as_str()
            .ok_or("missing recorded hash")?
            .parse()?)
    };
    let batch =
        spark_inputs::decode_recorded_batch(&records[0]["corpora"][0]["metadata"]["inputs"][1])?;
    let masked = masked_nested_batch(&batch)?;
    let DataType::List(item) = masked.column(2).data_type() else {
        return Err("expected list item".into());
    };
    let large =
        datafusion::arrow::compute::cast(masked.column(2), &DataType::LargeList(Arc::clone(item)))?;
    assert_eq!(
        hash_array(large)?,
        Int64Array::from((0..6).map(expected_hash).collect::<TestResult<Vec<_>>>()?)
    );
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        Arc::clone(item),
        3,
        Arc::new(
            Decimal128Array::from(vec![
                Some(7250),
                None,
                Some(-1000),
                Some(999017),
                Some(999018),
                Some(999019),
                Some(7250),
                None,
                Some(-1000),
            ])
            .with_precision_and_scale(38, 3)?,
        ),
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    assert_eq!(
        hash_array(fixed)?,
        Int64Array::from(vec![
            expected_hash(0)?,
            expected_hash(3)?,
            expected_hash(5)?
        ])
    );
    let scalar_input =
        spark_inputs::decode_recorded_batch(&records[0]["corpora"][0]["metadata"]["inputs"][0])?;
    let values = Arc::clone(scalar_input.column(5));
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from_iter_values(0..11),
        values,
    )?);
    let scalar_case = records[1..]
        .iter()
        .find(|case| case["id"] == "decimal_columns" && case["ansi"] == false)
        .ok_or("missing decimal case")?;
    let hashes = scalar_case["expected"]["rows"]
        .as_array()
        .ok_or("missing decimal rows")?
        .iter()
        .map(|row| -> TestResult<i64> {
            Ok(row[4].as_str().ok_or("missing recorded BIGINT")?.parse()?)
        })
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(hash_array(dictionary)?, Int64Array::from(hashes));
    Ok(())
}
