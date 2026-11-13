//! NULL STRUCT parents mask stored children in every Spark field-access route.

use arrow::array::{Array, ArrayRef, AsArray, Int32Array, ListArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_executed_query_result, capture_native_query_observation, encode_value,
    remove_nullability_and_metadata, validate_batch,
};

const REQUEST: &str = include_str!("fixtures/null-struct-parent/requests.json");
const REFERENCE: &str = include_str!("fixtures/null-struct-parent/spark-results.jsonl");

fn case_key(case: &Value) -> TestResult<(String, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing ID")?.to_owned(),
        case["ansi"].as_bool().ok_or("missing ANSI")?,
    ))
}

fn references() -> TestResult<(Vec<Value>, Value)> {
    let request: Value = serde_json::from_str(REQUEST)?;
    let corpus = &request["corpora"][0];
    let mut records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let header = records.remove(0);
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(header["corpora"][0]["metadata"], corpus["metadata"]);
    assert_eq!(
        header["expected_record_count"],
        request["expected_record_count"]
    );
    let mut definitions = BTreeMap::new();
    for case in corpus["cases"].as_array().ok_or("missing source cases")? {
        assert!(definitions.insert(case_key(case)?, case).is_none());
    }
    for (id, _) in definitions.keys() {
        for ansi in [false, true] {
            assert!(
                definitions.contains_key(&(id.clone(), ansi)),
                "{id} ANSI={ansi}"
            );
        }
    }
    let mut seen = BTreeSet::new();
    for record in &records {
        let key = case_key(record)?;
        assert!(seen.insert(key.clone()), "duplicate reference {key:?}");
        let definition = definitions.get(&key).ok_or("unknown reference")?;
        for field in ["corpus", "sql", "ordered"] {
            assert_eq!(record[field], definition[field], "{key:?}: {field}");
        }
        assert_eq!(record["status"], "ok", "{key:?}");
        assert_eq!(
            record["effective_settings"]["spark.sql.ansi.enabled"],
            key.1.to_string()
        );
        assert_eq!(
            record["effective_settings"]["spark.sql.session.timeZone"],
            "UTC"
        );
        assert!(record["expected"]["rows"].is_array());
        assert!(record["schema_stages"]["executed"]["fields"].is_array());
    }
    assert_eq!(seen, definitions.into_keys().collect());
    assert_eq!(json!(seen.len()), request["expected_record_count"]);
    Ok((records, corpus["metadata"].clone()))
}

fn executed_types(schema: &Value) -> TestResult<Value> {
    let mut types = json!(
        schema["fields"]
            .as_array()
            .ok_or("missing executed fields")?
            .iter()
            .map(|field| field["type"].clone())
            .collect::<Vec<_>>()
    );
    remove_nullability_and_metadata(&mut types);
    Ok(types)
}

fn session(target_partitions: usize, output_batch: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(output_batch)
                        .with_target_partitions(target_partitions),
                ),
        )
        .build(),
    )
}

async fn assert_provider_inputs(
    session: &SessionContext,
    inputs: &Value,
    partitions: usize,
) -> TestResult {
    let state = session.state();
    for input in inputs["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing provider name")?;
        let scan = session
            .table_provider(name)
            .await?
            .scan(&state, None, &[], None)
            .await?;
        assert_eq!(
            scan.output_partitioning().partition_count(),
            partitions,
            "{name}"
        );
        let schema = scan.schema();
        let mut rows = Vec::new();
        for batch in collect(scan, session.task_ctx()).await? {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(json!(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?
                ));
            }
        }
        rows.sort_by_key(Value::to_string);
        let mut expected = input["rows"]
            .as_array()
            .ok_or("missing input rows")?
            .clone();
        expected.sort_by_key(Value::to_string);
        assert_eq!(rows, expected, "{name} source readback");
    }
    Ok(())
}

fn restore_struct_masks(logical: &StructArray, stored: &StructArray) -> TestResult<StructArray> {
    let columns = logical
        .columns()
        .iter()
        .zip(stored.columns())
        .map(
            |(logical, stored)| match (logical.as_struct_opt(), stored.as_struct_opt()) {
                (Some(logical), Some(stored)) => {
                    Ok(Arc::new(restore_struct_masks(logical, stored)?) as ArrayRef)
                }
                _ => Ok(stored.clone()),
            },
        )
        .collect::<TestResult<Vec<_>>>()?;
    Ok(StructArray::try_new(
        stored.fields().clone(),
        columns,
        logical.nulls().cloned(),
    )?)
}

fn hidden_batch(input: &Value) -> TestResult<RecordBatch> {
    let logical = spark_inputs::decode_recorded_batch(input)?;
    let mut storage = input.clone();
    for row in storage["rows"].as_array_mut().ok_or("missing rows")? {
        if row[1].is_null() {
            row[1] = json!([
                "31337",
                "hidden",
                ["919"],
                ["hidden", "4141"],
                [["hidden", "8282"]],
                null
            ]);
        } else if row[1][3].is_null() {
            row[1][3] = json!(["hidden_nested", "4242"]);
        }
        if let Some(records) = row[2].as_array_mut() {
            for record in records {
                if record.is_null() {
                    *record = json!(["5151", "hidden_record", ["hidden", "6161"]]);
                } else if record[2].is_null() {
                    record[2] = json!(["hidden_nested_record", "6262"]);
                }
            }
        }
    }
    let stored = spark_inputs::decode_recorded_batch(&storage)?;
    let parent = restore_struct_masks(logical.column(1).as_struct(), stored.column(1).as_struct())?;
    if logical.num_rows() > 0 {
        assert!(parent.is_null(1));
        assert_eq!(encode_value(parent.column(0).as_ref(), 1)?, json!("31337"));
    }
    let logical_records = logical.column(2).as_list::<i32>();
    let stored_records = stored.column(2).as_list::<i32>();
    let values = restore_struct_masks(
        logical_records.values().as_struct(),
        stored_records.values().as_struct(),
    )?;
    let DataType::List(field) = stored_records.data_type() else {
        return Err("expected LIST".into());
    };
    let records = ListArray::try_new(
        field.clone(),
        stored_records.offsets().clone(),
        Arc::new(values),
        logical_records.nulls().cloned(),
    )?;
    let batch = RecordBatch::try_new(
        logical.schema(),
        vec![
            logical.column(0).clone(),
            Arc::new(parent),
            Arc::new(records),
        ],
    )?;
    validate_batch(batch.schema().as_ref(), &batch)?;
    Ok(batch)
}

fn register_hidden_inputs(
    session: &SessionContext,
    inputs: &Value,
    input_batch: usize,
    partition_count: usize,
) -> TestResult {
    for input in inputs["inputs"].as_array().ok_or("missing inputs")? {
        let batch = hidden_batch(input)?;
        let mut partitions = vec![Vec::new(); partition_count];
        for (chunk, offset) in (0..batch.num_rows()).step_by(input_batch).enumerate() {
            partitions[chunk % partition_count]
                .push(batch.slice(offset, input_batch.min(batch.num_rows() - offset)));
        }
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(batch.schema(), partitions)?),
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn null_struct_parents_match_spark_for_logical_and_hidden_storage() -> TestResult {
    let (cases, inputs) = references()?;
    let mut differences = Vec::new();
    for (partitions, input_batch, target_partitions, output_batch) in
        [(1, 1, 1, 1), (3, 2, 4, 3), (2, 32, 2, 32)]
    {
        for hidden in [false, true] {
            let session = session(target_partitions, output_batch);
            if hidden {
                register_hidden_inputs(&session, &inputs, input_batch, partitions)?;
            } else {
                spark_inputs::register_recorded_inputs_with_partition_count(
                    &session,
                    &inputs,
                    input_batch,
                    partitions,
                )?;
            }
            assert_provider_inputs(&session, &inputs, partitions).await?;
            for case in &cases {
                let actual = capture_executed_query_result(&session, case).await;
                let mut rows = case["expected"]["rows"].clone();
                if case["ordered"] != true {
                    rows.as_array_mut()
                        .ok_or("missing rows")?
                        .sort_by_key(Value::to_string);
                }
                let matches = match &actual {
                    Ok(actual) => {
                        actual["rows"] == rows
                            && executed_types(&actual["schema"])?
                                == executed_types(&case["schema_stages"]["executed"])?
                    }
                    Err(_) => false,
                };
                println!(
                    "NULL_STRUCT_OBSERVATION {}",
                    json!({"id":case["id"],"ansi":case["ansi"],"hidden":hidden,
                    "partitions":partitions,"input_batch":input_batch,"target_partitions":target_partitions,"output_batch":output_batch,
                    "matches":matches,"actual":actual.as_ref().map_err(ToString::to_string)})
                );
                if !matches {
                    differences.push(format!("{} ANSI={} hidden={hidden} layout=({partitions},{input_batch},{target_partitions},{output_batch}): expected rows={rows} schema={}, got {actual:?}",case["id"],case["ansi"],case["schema_stages"]["executed"]));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn nullable_effectful_parent_is_evaluated_once_per_source_row() -> TestResult {
    let (_, inputs) = references()?;
    for output_batch in [1, 32] {
        let session = session(2, output_batch);
        spark_inputs::register_recorded_inputs(&session, &inputs, 2)?;
        let rows = Arc::new(AtomicUsize::new(0));
        let counter = rows.clone();
        let fields: arrow::datatypes::Fields =
            vec![Field::new("code", DataType::Int32, true)].into();
        let result_type = DataType::Struct(fields.clone());
        session.register_udf(create_udf(
            "next_parent",
            vec![DataType::Int32],
            result_type,
            Volatility::Volatile,
            Arc::new(move |arguments| {
                let arrays = ColumnarValue::values_to_arrays(arguments)?;
                let ids = arrays[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Internal("expected INT".into())
                    })?;
                counter.fetch_add(ids.len(), Ordering::SeqCst);
                let values = Int32Array::from_iter_values(ids.values().iter().map(|id| id + 100));
                Ok(ColumnarValue::Array(Arc::new(StructArray::try_new(
                    fields.clone(),
                    vec![Arc::new(values)],
                    Some(NullBuffer::from(
                        ids.values().iter().map(|id| *id != 1).collect::<Vec<_>>(),
                    )),
                )?)))
            }),
        ));
        for ansi in [false, true] {
            for shared in [false, true] {
                rows.store(0, Ordering::SeqCst);
                let sql = if shared {
                    "SELECT id, p.code, p.code AS again FROM (SELECT id, next_parent(id) AS p FROM parents) q ORDER BY id"
                } else {
                    "SELECT id, next_parent(id).code AS code FROM parents ORDER BY id"
                };
                let query = plan_query(
                    &session,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                assert_eq!(
                    rows.load(Ordering::SeqCst),
                    0,
                    "producer ran during planning"
                );
                let schema = physical.schema();
                let mut actual = Vec::new();
                for batch in collect(physical, session.task_ctx()).await? {
                    validate_batch(schema.as_ref(), &batch)?;
                    for row in 0..batch.num_rows() {
                        actual.push(json!(
                            batch
                                .columns()
                                .iter()
                                .map(|column| encode_value(column.as_ref(), row))
                                .collect::<TestResult<Vec<_>>>()?
                        ));
                    }
                }
                let expected = [
                    ("0", Some("100")),
                    ("0", Some("100")),
                    ("1", None),
                    ("2", Some("102")),
                    ("3", Some("103")),
                ]
                .into_iter()
                .map(|(id, code)| {
                    if shared {
                        json!([id, code, code])
                    } else {
                        json!([id, code])
                    }
                })
                .collect::<Vec<_>>();
                assert_eq!(actual, expected, "ANSI={ansi} shared={shared}");
                assert_eq!(
                    rows.load(Ordering::SeqCst),
                    inputs["inputs"][0]["rows"]
                        .as_array()
                        .ok_or("missing input rows")?
                        .len(),
                    "ANSI={ansi} shared={shared}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn progressive_dot_and_bracket_access_grows_linearly() -> TestResult {
    for syntax in ["dot", "bracket", "mixed"] {
        let mut sizes = Vec::new();
        for depth in [2, 4, 8] {
            let mut data_type = json!("integer");
            let mut value = json!("9");
            for _ in 0..depth {
                data_type = json!({"type":"struct","fields":[{"name":"n","type":data_type,"nullable":true,"metadata":{}}]});
                value = json!([value]);
            }
            let inputs = json!({"inputs":[{"name":"chains","schema":{"type":"struct","fields":[{"name":"s","type":data_type,"nullable":true,"metadata":{}}]},"rows":[[null],[value]]}]});
            let session = session(1, 32);
            spark_inputs::register_recorded_inputs(&session, &inputs, 1)?;
            let path = (0..depth).fold("s".to_owned(), |mut path, index| {
                path.push_str(if syntax == "dot" || syntax == "mixed" && index % 2 == 0 {
                    ".n"
                } else {
                    "['n']"
                });
                path
            });
            let sql = format!("SELECT {path} AS value FROM chains ORDER BY value NULLS FIRST");
            let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
            let mut nodes = 0;
            query.logical_plan.apply(|plan| {
                for expr in plan.expressions() {
                    expr.apply(|_| {
                        nodes += 1;
                        Ok::<_, datafusion::common::DataFusionError>(TreeNodeRecursion::Continue)
                    })?;
                }
                Ok::<_, datafusion::common::DataFusionError>(TreeNodeRecursion::Continue)
            })?;
            sizes.push((depth, nodes));
            let actual = capture_executed_query_result(
                &session,
                &json!({"sql":sql,"ansi":false,"ordered":true}),
            )
            .await?;
            assert_eq!(
                actual["rows"],
                json!([[null], ["9"]]),
                "{syntax} depth={depth}"
            );
        }
        let [(d1, n1), (d2, n2), (d3, n3)] = sizes.as_slice() else {
            return Err("missing chain sizes".into());
        };
        assert!(*n2 > *n1 && *n3 > *n2);
        assert_eq!(
            (n2 - n1) * (d3 - d2),
            (n3 - n2) * (d2 - d1),
            "{syntax}: {sizes:?}"
        );
        println!(
            "NULL_STRUCT_GROWTH {}",
            json!({"syntax":syntax,"sizes":sizes})
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_field_access_and_invalid_field_validation_are_preserved() -> TestResult {
    let (_, inputs) = references()?;
    let native = SessionContext::new();
    let configured = session(2, 32);
    for session in [&native, &configured] {
        register_hidden_inputs(session, &inputs, 2, 2)?;
    }
    let sql = "SELECT id, s['code'] AS code FROM parents ORDER BY id";
    let expected = capture_native_query_observation(&native, sql).await?;
    assert!(
        expected["rows"]
            .as_array()
            .ok_or("missing native rows")?
            .contains(&json!(["1", "31337"]))
    );
    assert_eq!(
        capture_native_query_observation(&configured, sql).await?,
        expected
    );
    for ansi_mode in [false, true] {
        for sql in [
            "SELECT s.missing FROM parents WHERE id = 1",
            "SELECT s['missing'] FROM parents WHERE id = 1",
        ] {
            match plan_query(
                &configured,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await
            {
                Err(error) => {
                    let error = error.to_string();
                    assert!(
                        error.contains("missing") && error.contains("FIELD_NOT_FOUND"),
                        "{error}"
                    );
                }
                Ok(_) => return Err(format!("invalid field admitted: {sql}").into()),
            }
        }
    }
    Ok(())
}

#[test]
fn parent_masks_preserve_sliced_dictionary_children_and_runtime_validation() -> TestResult {
    use arrow::array::{DictionaryArray, Int8Array, StringArray};
    use arrow::datatypes::{FieldRef, Int8Type};
    use datafusion::common::ScalarValue;
    use datafusion::functions::core::getfield::GetFieldFunc;
    use datafusion::logical_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
    use sail_function::scalar::get_field_with_parent_nulls::GetFieldWithParentNulls;

    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None, Some(2)]),
        Arc::new(StringArray::from(vec!["visible", "hidden", "tail"])),
    )?);
    let child_field = Arc::new(
        Field::new("payload", dictionary.data_type().clone(), true).with_metadata(
            [("source".to_owned(), "kept".to_owned())]
                .into_iter()
                .collect(),
        ),
    );
    let parent: ArrayRef = Arc::new(StructArray::try_new(
        vec![child_field].into(),
        vec![dictionary],
        Some(NullBuffer::from(vec![true, false, true, true])),
    )?);
    let invoke = |function: &dyn ScalarUDFImpl,
                  parent: ArrayRef,
                  name: &str|
     -> TestResult<(FieldRef, ArrayRef)> {
        let key = ScalarValue::Utf8(Some(name.to_owned()));
        let fields = vec![
            Arc::new(Field::new("parent", parent.data_type().clone(), true)),
            Arc::new(Field::new("field", DataType::Utf8, false)),
        ];
        let field = function.return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &[None, Some(&key)],
        })?;
        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(parent.clone()),
                    ColumnarValue::Scalar(key),
                ],
                arg_fields: fields,
                number_rows: parent.len(),
                return_field: field.clone(),
                config_options: Arc::default(),
            })?
            .into_array(parent.len())?;
        result.to_data().validate_full()?;
        Ok((field, result))
    };
    let masked = GetFieldWithParentNulls::new();
    let native = GetFieldFunc::new();
    let expected = [json!("visible"), Value::Null, Value::Null, json!("tail")];
    for (offset, length) in [(0, 4), (1, 2), (2, 2)] {
        let parent = parent.slice(offset, length);
        let (native_field, native_values) = invoke(&native, parent.clone(), "payload")?;
        let (field, values) = invoke(&masked, parent, "payload")?;
        assert_eq!(field, native_field);
        assert_eq!(values.data_type(), native_values.data_type());
        let source = native_values.as_dictionary::<Int8Type>().values().to_data();
        let result = values.as_dictionary::<Int8Type>().values().to_data();
        assert!(
            source
                .buffers()
                .iter()
                .zip(result.buffers())
                .all(|(source, result)| source.ptr_eq(result))
        );
        let decoded = arrow::compute::cast(&values, &DataType::Utf8)?;
        assert_eq!(
            (0..length)
                .map(|row| encode_value(decoded.as_ref(), row))
                .collect::<TestResult<Vec<_>>>()?,
            expected[offset..offset + length]
        );
    }
    let struct_values: ArrayRef = Arc::new(StructArray::try_new(
        vec![Field::new("payload", DataType::Utf8, true)].into(),
        vec![Arc::new(StringArray::from(vec![
            Some("visible"),
            Some("hidden"),
            None,
            Some("tail"),
        ]))],
        parent.nulls().cloned(),
    )?);
    let encoded_parent: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 1, 2, 3]),
        struct_values,
    )?);
    for (offset, length) in [(0, 4), (1, 2)] {
        let encoded_parent = encoded_parent.slice(offset, length);
        let (_, native_values) = invoke(&native, encoded_parent.clone(), "payload")?;
        let (_, values) = invoke(&masked, encoded_parent, "payload")?;
        assert_eq!(values.data_type(), native_values.data_type());
        let decoded = arrow::compute::cast(&values, &DataType::Utf8)?;
        assert_eq!(
            (0..length)
                .map(|row| encode_value(decoded.as_ref(), row))
                .collect::<TestResult<Vec<_>>>()?,
            expected[offset..offset + length]
        );
    }
    let parent = parent.as_struct();
    let all_null: ArrayRef = Arc::new(StructArray::try_new(
        parent.fields().clone(),
        parent.columns().to_vec(),
        Some(NullBuffer::new_null(parent.len())),
    )?);
    for function in [&native as &dyn ScalarUDFImpl, &masked as &dyn ScalarUDFImpl] {
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(all_null.clone()),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("missing".into()))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("parent", all_null.data_type().clone(), true)),
                Arc::new(Field::new("field", DataType::Utf8, false)),
            ],
            number_rows: all_null.len(),
            return_field: Arc::new(Field::new("missing", DataType::Utf8, true)),
            config_options: Arc::default(),
        });
        match result {
            Err(error) => assert!(
                error
                    .to_string()
                    .contains("Field missing not found in struct"),
                "{error}"
            ),
            Ok(_) => return Err("an all-NULL parent bypassed invalid-field validation".into()),
        }
    }
    Ok(())
}

#[tokio::test]
async fn non_bitmap_children_preserve_the_prior_guarded_results() -> TestResult {
    use arrow::array::{RunArray, UnionArray};
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::{Int32Type, Schema, UnionFields};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{col, lit, when};

    let runs: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![2, 4]),
        &Int32Array::from(vec![7, 10]),
    )?);
    let fields = UnionFields::try_new(vec![0], vec![Field::new("value", DataType::Int32, true)])?;
    let values: ArrayRef = Arc::new(Int32Array::from(vec![7, 8, 9, 10]));
    let sparse: ArrayRef = Arc::new(UnionArray::try_new(
        fields.clone(),
        ScalarBuffer::from(vec![0i8; 4]),
        None,
        vec![values.clone()],
    )?);
    let dense: ArrayRef = Arc::new(UnionArray::try_new(
        fields,
        ScalarBuffer::from(vec![0i8; 4]),
        Some(ScalarBuffer::from(vec![0i32, 1, 2, 3])),
        vec![values],
    )?);
    for child in [runs, sparse, dense] {
        let data_type = child.data_type().clone();
        let parent = StructArray::try_new(
            vec![Field::new("leaf", data_type.clone(), true)].into(),
            vec![child],
            Some(NullBuffer::from(vec![true, false, false, true])),
        )?;
        for (offset, length, run_count) in [(0, 4, 3), (1, 2, 1), (2, 2, 2)] {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "s",
                    parent.data_type().clone(),
                    true,
                )])),
                vec![Arc::new(parent.slice(offset, length))],
            )?;
            validate_batch(batch.schema().as_ref(), &batch)?;
            let session = session(1, 32);
            session.register_table(
                "layouts",
                Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
            )?;
            let legacy = when(col("s").is_null(), lit(ScalarValue::try_from(&data_type)?))
                .otherwise(
                    datafusion::functions::core::get_field().call(vec![col("s"), lit("leaf")]),
                )?;
            let expected = session
                .table("layouts")
                .await?
                .select(vec![legacy.alias("value")])?
                .collect()
                .await?;
            assert_eq!(expected.len(), 1);
            let expected = expected[0].column(0);
            for ansi_mode in [false, true] {
                let sql = "SELECT s.leaf AS value FROM layouts";
                let query = plan_query(
                    &session,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                let schema = physical.schema();
                let actual = collect(physical, session.task_ctx()).await?;
                assert_eq!(actual.len(), 1);
                validate_batch(schema.as_ref(), &actual[0])?;
                let actual = actual[0].column(0);
                assert_eq!(actual.data_type(), expected.data_type());
                assert_eq!(actual.logical_nulls(), expected.logical_nulls());
                assert_eq!(
                    actual.to_data(),
                    expected.to_data(),
                    "ANSI={ansi_mode}: {sql}"
                );
                if let Some(runs) = actual.as_any().downcast_ref::<RunArray<Int32Type>>() {
                    assert_eq!(
                        runs.values().len(),
                        run_count,
                        "contiguous NULL rows must remain one encoded run"
                    );
                }
            }
        }
    }
    Ok(())
}
