//! View CONCAT retains storage, NULL selection and the accepted fallback paths.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, DictionaryArray, Int32Array, ListArray, StringViewArray,
};
use arrow::compute::{cast, concat};
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;
use datafusion::common::exec_err;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn session() -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    )
}

async fn query(session: &SessionContext, sql: &str, ansi: bool) -> TestResult<Vec<RecordBatch>> {
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode: ansi,
            ..Default::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    Ok(collect(physical, session.task_ctx()).await?)
}

#[tokio::test]
async fn concat_row_path_preserves_storage_and_scalar_column_forms() -> TestResult {
    let inputs: Vec<Vec<Option<String>>> = serde_json::from_str(include_str!(
        "../vendor/sail/crates/sail-function/tests/fixtures/concat-row-cost-1388-inputs.json"
    ))?;
    let inputs = inputs
        .into_iter()
        .filter(|row| row.len() == 3)
        .collect::<Vec<_>>();
    let literals = ["literal-0", "literal-1", "literal-2"];
    for storage in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for dictionary in [false, true] {
            let session = session();
            let mut columns = vec![(
                "k",
                Arc::new(Int32Array::from_iter_values(0..inputs.len() as i32)) as ArrayRef,
            )];
            for (index, name) in ["a", "b", "c"].into_iter().enumerate() {
                let values = cast(
                    &StringViewArray::from(
                        inputs
                            .iter()
                            .map(|row| row[index].as_deref())
                            .collect::<Vec<_>>(),
                    ),
                    &storage,
                )?;
                let values: ArrayRef = if dictionary {
                    Arc::new(DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from_iter_values(0..inputs.len() as i32),
                        values,
                    )?)
                } else {
                    values
                };
                columns.push((name, values));
            }
            session.register_batch(
                "inputs",
                RecordBatch::try_from_iter_with_nullable(columns.into_iter().map(
                    |(name, array)| {
                        let nullable = array.logical_null_count() > 0;
                        (name, array, nullable)
                    },
                ))?,
            )?;
            for ansi in [false, true] {
                for mask in 0..8 {
                    let args = ["a", "b", "c"]
                        .into_iter()
                        .enumerate()
                        .map(|(index, name)| {
                            if mask & (1 << index) == 0 {
                                format!("'{}'", literals[index])
                            } else {
                                name.to_string()
                            }
                        })
                        .collect::<Vec<_>>();
                    let sql = format!(
                        "SELECT k,concat({}) AS r FROM inputs ORDER BY k",
                        args.join(",")
                    );
                    let expected = inputs
                        .iter()
                        .map(|row| {
                            (0..3)
                                .map(|index| {
                                    if mask & (1 << index) == 0 {
                                        Some(literals[index])
                                    } else {
                                        row[index].as_deref()
                                    }
                                })
                                .collect::<Option<Vec<_>>>()
                                .map(|parts| parts.concat())
                        })
                        .collect::<Vec<_>>();
                    let expected_type = if mask == 0 {
                        DataType::Utf8
                    } else {
                        storage.clone()
                    };
                    let expected = cast(&StringViewArray::from(expected), &expected_type)?;
                    let batches = query(&session, &sql, ansi).await.map_err(|error| format!("{sql}, storage={storage}, dictionary={dictionary}, ANSI={ansi}: {error}"))?;
                    for batch in &batches {
                        assert_eq!(
                            batch.schema().field(1).data_type(),
                            &expected_type,
                            "{sql}, storage={storage}, dictionary={dictionary}, ANSI={ansi}"
                        );
                    }
                    let columns = batches
                        .iter()
                        .map(|batch| batch.column(1).as_ref())
                        .collect::<Vec<_>>();
                    assert_eq!(
                        concat(&columns)?.as_ref(),
                        expected.as_ref(),
                        "{sql}, storage={storage}, dictionary={dictionary}, ANSI={ansi}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn concat_view_path_preserves_selected_effects_and_errors() -> TestResult {
    let session = session();
    session.register_batch(
        "inputs",
        RecordBatch::try_from_iter(vec![
            (
                "a",
                Arc::new(StringViewArray::from(vec![
                    Some("abcdefghijklmnopqrstuvwxyz"),
                    None,
                    Some("left"),
                ])) as ArrayRef,
            ),
            (
                "b",
                Arc::new(StringViewArray::from(vec![
                    Some("middle"),
                    Some("unused"),
                    None,
                ])) as ArrayRef,
            ),
            (
                "c",
                Arc::new(StringViewArray::from(vec![
                    Some("tail"),
                    Some("unused"),
                    Some("unused"),
                ])) as ArrayRef,
            ),
        ])?,
    )?;
    let calls = [
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    ];
    for (index, name) in ["observed_a", "observed_b", "observed_c"]
        .into_iter()
        .enumerate()
    {
        let seen = calls[index].clone();
        session.register_udf(create_udf(
            name,
            vec![DataType::Utf8View],
            DataType::Utf8View,
            Volatility::Volatile,
            Arc::new(move |args| {
                seen.fetch_add(
                    match &args[0] {
                        ColumnarValue::Array(array) => array.len(),
                        ColumnarValue::Scalar(_) => 1,
                    },
                    Ordering::SeqCst,
                );
                Ok(args[0].clone())
            }),
        ));
    }
    session.register_udf(create_udf(
        "failed_concat_operand",
        vec![DataType::Utf8View],
        DataType::Utf8View,
        Volatility::Volatile,
        Arc::new(|_| exec_err!("CONCAT reached operand failure")),
    ));
    for ansi in [false, true] {
        for calls in &calls {
            calls.store(0, Ordering::SeqCst);
        }
        let planned = plan_query(
            &session,
            "SELECT concat(observed_a(a),observed_b(b),observed_c(c)) AS r FROM inputs",
            &SparkSqlOptions {
                ansi_mode: ansi,
                ..Default::default()
            },
        )
        .await?;
        let physical = planned.create_physical_plan(&session).await?;
        assert!(
            calls.iter().all(|calls| calls.load(Ordering::SeqCst) == 0),
            "planning evaluated a volatile CONCAT operand"
        );
        let batches = collect(physical, session.task_ctx()).await?;
        let arrays = batches
            .iter()
            .map(|batch| batch.column(0).as_ref())
            .collect::<Vec<_>>();
        assert_eq!(
            concat(&arrays)?.as_ref(),
            &StringViewArray::from(vec![
                Some("abcdefghijklmnopqrstuvwxyzmiddletail"),
                None,
                None
            ]) as &dyn Array
        );
        assert_eq!(
            calls.each_ref().map(|calls| calls.load(Ordering::SeqCst)),
            [3, 2, 1],
            "ANSI={ansi}"
        );
        query(
            &session,
            "SELECT concat(a,failed_concat_operand(b)) FROM inputs WHERE a IS NULL",
            ansi,
        )
        .await?;
        let error = query(
            &session,
            "SELECT concat(a,failed_concat_operand(b)) FROM inputs WHERE a IS NOT NULL",
            ansi,
        )
        .await
        .err()
        .ok_or("reached CONCAT operand did not fail")?;
        assert!(
            error.to_string().contains("CONCAT reached operand failure"),
            "{error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn concat_binary_and_list_keep_accepted_rows() -> TestResult {
    let bytes = BinaryArray::from(vec![
        Some(b"abcdefghijklmnop".as_slice()),
        None,
        Some([255_u8; 16].as_slice()),
    ]);
    for storage in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(16),
    ] {
        let session = session();
        session.register_batch(
            "inputs",
            RecordBatch::try_from_iter(vec![("b", cast(&bytes, &storage)?)])?,
        )?;
        let expected =
            BinaryArray::from_iter(bytes.iter().map(|value| value.map(|value| value.repeat(2))));
        for ansi in [false, true] {
            let batches = query(&session, "SELECT concat(b,b) AS r FROM inputs", ansi).await?;
            let expected_type = if storage == DataType::LargeBinary {
                DataType::LargeBinary
            } else {
                DataType::Binary
            };
            for batch in &batches {
                assert_eq!(batch.schema().field(0).data_type(), &expected_type);
            }
            let arrays = batches
                .iter()
                .map(|batch| batch.column(0).as_ref())
                .collect::<Vec<_>>();
            assert_eq!(
                cast(concat(&arrays)?.as_ref(), &DataType::Binary)?.as_binary::<i32>(),
                &expected,
                "storage={storage}, ANSI={ansi}"
            );
        }
    }
    let session = session();
    session.register_batch(
        "inputs",
        RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                Some(vec![Some(1), None]),
                None,
                Some(vec![]),
            ])) as ArrayRef,
        )])?,
    )?;
    let expected = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), None, Some(1), None]),
        None,
        Some(vec![]),
    ]);
    for ansi in [false, true] {
        let batches = query(&session, "SELECT concat(v,v) AS r FROM inputs", ansi).await?;
        let arrays = batches
            .iter()
            .map(|batch| batch.column(0).as_ref())
            .collect::<Vec<_>>();
        assert_eq!(
            concat(&arrays)?.as_ref(),
            &expected as &dyn Array,
            "ANSI={ansi}"
        );
    }
    Ok(())
}
