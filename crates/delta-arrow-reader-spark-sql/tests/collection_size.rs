//! SIZE/CARDINALITY follow the selected NULL rule; ARRAY_SIZE remains null-on-null.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{
    Array, ArrayRef, AsArray, Int32Builder, LargeListArray, ListArray, MapBuilder, NullArray,
    StringBuilder,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int32Type};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn create_session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

async fn execute_reference_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let options = SparkSqlOptions {
        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        legacy_size_of_null: case["legacy_size_of_null"]
            .as_bool()
            .ok_or("missing legacy mode")?,
        ..SparkSqlOptions::default()
    };
    let query = match plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &options,
    )
    .await
    {
        Ok(query) => query,
        Err(error) => return Ok(json!({"stage":"planning", "error":error.to_string()})),
    };
    let logical_nullable = query.logical_plan.schema().field(0).is_nullable();
    let plan = query.create_physical_plan(session).await?;
    let schema = plan.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(query.column_names, ["r"]);
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        let sizes = batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        if sizes.null_count() > 0 {
            assert!(logical_nullable && schema.field(0).is_nullable());
        }
        rows.extend(sizes.iter().map(|size| vec![size]));
    }
    Ok(json!({"status":"ok", "rows":rows}))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_spark_collection_sizes() -> TestResult {
    let records = include_str!("fixtures/collection-size-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len(), 289);
    assert_eq!(records[0]["default_legacy_size_of_null"], "true");
    assert!(SparkSqlOptions::default().legacy_size_of_null);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = create_session(batch_size);
        for case in &records[1..] {
            let actual = execute_reference_query(&session, case).await;
            let expected = &case["expected"];
            let matches = match &actual {
                Ok(value) if expected["status"] == "ok" => {
                    assert_eq!(expected["schema"]["fields"][0]["type"], "integer");
                    value["status"] == "ok" && value["rows"] == expected["rows"]
                }
                Ok(value) if value["stage"] == "planning" => {
                    let error = value["error"].as_str().ok_or("missing planning error")?;
                    match expected["condition"].as_str() {
                        Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") => {
                            error.contains("size expects List or Map")
                                || (error.contains("array_length")
                                    && error.contains("coercion from"))
                        }
                        Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION") => {
                            error.contains("one value expected")
                        }
                        _ => false,
                    }
                }
                Err(error) if expected["condition"] == "CAST_INVALID_INPUT" => error
                    .to_string()
                    .contains("Cannot cast string 'bad' to value of Int32 type"),
                _ => false,
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} legacy={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"], case["legacy_size_of_null"]
                ));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn collection_size_keeps_arrow_representations_and_single_evaluation() -> TestResult {
    let session = create_session(32);
    let arrays = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), None]),
        None,
        Some(vec![Some(3), Some(4)]),
    ]);
    let element = Arc::new(Field::new_list_field(DataType::Int32, true));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut inputs = Vec::new();
    for data_type in [
        DataType::List(element.clone()),
        DataType::LargeList(element.clone()),
        DataType::ListView(element.clone()),
        DataType::LargeListView(element.clone()),
        DataType::FixedSizeList(element.clone(), 2),
    ] {
        inputs.push((
            arrow::compute::cast(&arrays, &data_type)?,
            [Some(2), None, Some(2)],
        ));
    }
    let mut maps = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for valid in [true, false, true] {
        if valid {
            maps.keys().append_value("a");
            maps.values().append_value(1);
            maps.keys().append_value("b");
            maps.values().append_null();
        }
        maps.append(valid)?;
    }
    inputs.push((Arc::new(maps.finish()), [Some(2), None, Some(2)]));
    inputs.push((Arc::new(NullArray::new(3)), [None, None, None]));
    for (input, expected) in inputs {
        let data_type = input.data_type();
        let counter = calls.clone();
        session.register_udf(create_udf(
            "count_collection",
            vec![data_type.clone()],
            data_type.clone(),
            Volatility::Volatile,
            Arc::new(move |arguments| {
                let values = ColumnarValue::values_to_arrays(arguments)?;
                counter.fetch_add(values[0].len(), Ordering::SeqCst);
                Ok(ColumnarValue::Array(values[0].clone()))
            }),
        ));
        for (offset, len) in [(0, 3), (1, 2), (0, 0)] {
            session.deregister_table("collections")?;
            session.register_batch(
                "collections",
                RecordBatch::try_from_iter([("a", input.slice(offset, len))])?,
            )?;
            for ansi_mode in [true, false] {
                for legacy_size_of_null in [true, false] {
                    let options = SparkSqlOptions {
                        ansi_mode,
                        legacy_size_of_null,
                        ..SparkSqlOptions::default()
                    };
                    for function in ["SIZE", "CARDINALITY", "ARRAY_SIZE"] {
                        if function == "ARRAY_SIZE" && matches!(data_type, DataType::Map(..)) {
                            continue;
                        }
                        calls.store(0, Ordering::SeqCst);
                        let query = plan_query(
                            &session,
                            &format!(
                                "SELECT {function}(count_collection(a)) AS r FROM collections"
                            ),
                            &options,
                        )
                        .await?;
                        let plan = query.create_physical_plan(&session).await?;
                        assert_eq!(
                            calls.load(Ordering::SeqCst),
                            0,
                            "planning evaluated the input"
                        );
                        let batches = collect(plan, session.task_ctx()).await?;
                        let values = batches
                            .iter()
                            .flat_map(|b| b.column(0).as_primitive::<Int32Type>().iter())
                            .collect::<Vec<_>>();
                        let null_size =
                            if !ansi_mode && legacy_size_of_null && function != "ARRAY_SIZE" {
                                Some(-1)
                            } else {
                                None
                            };
                        let expected = expected.map(|size| size.or(null_size));
                        assert_eq!(values, expected[offset..offset + len]);
                        assert_eq!(
                            calls.load(Ordering::SeqCst),
                            len,
                            "{function} {data_type:?} ANSI={ansi_mode} legacy={legacy_size_of_null}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn collection_size_rejects_lengths_outside_int_range() -> TestResult {
    let session = create_session(32);
    let length = i64::from(i32::MAX) + 1;
    // NullArray stores only a length, so this boundary needs no huge value buffer.
    let arrays = LargeListArray::try_new(
        Arc::new(Field::new_list_field(DataType::Null, true)),
        OffsetBuffer::new(vec![0, length].into()),
        Arc::new(NullArray::new(usize::try_from(length)?)),
        None,
    )?;
    session.register_batch(
        "large_collection",
        RecordBatch::try_from_iter([("a", Arc::new(arrays) as ArrayRef)])?,
    )?;
    for ansi_mode in [true, false] {
        for legacy_size_of_null in [true, false] {
            let options = SparkSqlOptions {
                ansi_mode,
                legacy_size_of_null,
                ..SparkSqlOptions::default()
            };
            for function in ["SIZE", "CARDINALITY", "ARRAY_SIZE"] {
                let query = plan_query(
                    &session,
                    &format!("SELECT {function}(a) AS r FROM large_collection"),
                    &options,
                )
                .await?;
                let plan = query.create_physical_plan(&session).await?;
                let error = collect(plan, session.task_ctx())
                    .await
                    .err()
                    .ok_or("oversized collection was accepted")?
                    .to_string();
                assert!(
                    error.contains("2147483648") && error.contains("Int32"),
                    "{error}"
                );
            }
        }
    }
    Ok(())
}
