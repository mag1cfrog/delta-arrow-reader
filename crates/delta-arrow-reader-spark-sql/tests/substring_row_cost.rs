//! The bounded view row path preserves the existing kernel and resolver storage.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, StringViewArray};
use arrow::compute::{cast, concat};
use arrow::datatypes::{DataType, Field, Int32Type};
use arrow::record_batch::RecordBatch;
use datafusion::common::{ScalarValue, config::ConfigOptions, exec_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Volatility, create_udf,
};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::string::spark_substring::SparkSubstring;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

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
async fn substring_row_path_preserves_resolved_storage_and_bounds() -> TestResult {
    let inputs: Vec<(Option<String>, Option<i64>, Option<i64>)> =
        serde_json::from_str(include_str!(
            "../vendor/sail/crates/sail-function/tests/fixtures/substring-row-cost-1388-inputs.json"
        ))?;
    // Spark's existing integer coercion is unchanged; the unit oracle also covers
    // direct Int64 inputs beyond the public Spark INT range.
    let inputs = inputs
        .into_iter()
        .filter(|(_, position, length)| {
            [position, length]
                .into_iter()
                .all(|value| value.is_none_or(|value| i32::try_from(value).is_ok()))
        })
        .collect::<Vec<_>>();
    let values = StringViewArray::from(
        inputs
            .iter()
            .map(|row| row.0.as_deref())
            .collect::<Vec<_>>(),
    );
    let positions: ArrayRef = Arc::new(Int64Array::from(
        inputs.iter().map(|row| row.1).collect::<Vec<_>>(),
    ));
    let lengths: ArrayRef = Arc::new(Int64Array::from(
        inputs.iter().map(|row| row.2).collect::<Vec<_>>(),
    ));
    for storage in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let decoded = cast(&values, &storage)?;
        for dictionary in [false, true] {
            let source: ArrayRef = if dictionary {
                Arc::new(DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from_iter_values(0..values.len() as i32),
                    decoded.clone(),
                )?)
            } else {
                decoded.clone()
            };
            let session = SessionContext::new_with_state(
                configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
            );
            session.register_batch(
                "inputs",
                RecordBatch::try_from_iter(vec![
                    (
                        "k",
                        Arc::new(Int32Array::from_iter_values(0..values.len() as i32)) as ArrayRef,
                    ),
                    ("s", source),
                    ("p", positions.clone()),
                    ("n", lengths.clone()),
                ])?,
            )?;
            for ansi in [false, true] {
                for (position, length) in [
                    ("1", "12"),
                    ("p", "12"),
                    ("1", "n"),
                    ("p", "n"),
                    ("1", "2147483647"),
                ] {
                    let bounds = |name: &str, column: &ArrayRef| -> TestResult<ColumnarValue> {
                        Ok(match name.parse::<i64>() {
                            Ok(value) => ColumnarValue::Scalar(ScalarValue::Int64(Some(value))),
                            Err(_) => ColumnarValue::Array(column.clone()),
                        })
                    };
                    let args = vec![
                        ColumnarValue::Array(decoded.clone()),
                        bounds(position, &positions)?,
                        bounds(length, &lengths)?,
                    ];
                    // Array bounds always select the old registry fallback. Its direct
                    // comparison with the registry entry point lives in sail-function.
                    let args = args
                        .into_iter()
                        .map(|value| Ok(ColumnarValue::Array(value.into_array(values.len())?)))
                        .collect::<datafusion::common::Result<Vec<_>>>()?;
                    let expected = ScalarUDFImpl::invoke_with_args(
                        &SparkSubstring::new(),
                        ScalarFunctionArgs {
                            arg_fields: args
                                .iter()
                                .map(|value| Arc::new(Field::new("", value.data_type(), true)))
                                .collect(),
                            args,
                            number_rows: values.len(),
                            return_field: Arc::new(Field::new("r", storage.clone(), true)),
                            config_options: Arc::new(ConfigOptions::default()),
                        },
                    )?
                    .into_array(values.len())?;
                    let expression = if length == "2147483647" {
                        format!("substring(s,{position})")
                    } else {
                        format!("substring(s,{position},{length})")
                    };
                    let batches = query(
                        &session,
                        &format!("SELECT k,{expression} AS r FROM inputs ORDER BY k"),
                        ansi,
                    )
                    .await?;
                    for batch in &batches {
                        assert_eq!(
                            batch.schema().field(1).data_type(),
                            &storage,
                            "dictionary={dictionary}, ANSI={ansi}"
                        );
                        assert_eq!(batch.schema().field(1).name(), "r");
                    }
                    let columns = batches
                        .iter()
                        .map(|batch| batch.column(1).as_ref())
                        .collect::<Vec<_>>();
                    let actual = concat(&columns)?;
                    assert_eq!(
                        actual.as_ref(),
                        expected.as_ref(),
                        "{expression}, {storage}, dictionary={dictionary}, ANSI={ansi}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn substring_view_path_preserves_effect_and_error_boundaries() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_batch(
        "inputs",
        RecordBatch::try_from_iter(vec![(
            "s",
            Arc::new(StringViewArray::from(vec![
                Some("abcdefghijklmnopqrstuvwxyz"),
                None,
                Some("\u{e9}\u{4e2d}\u{1f600}abcdefghijklmnopqrstuvwxyz"),
            ])) as ArrayRef,
        )])?,
    )?;
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    session.register_udf(create_udf(
        "observed_view",
        vec![DataType::Utf8View],
        DataType::Utf8View,
        Volatility::Volatile,
        Arc::new(move |args| {
            seen.fetch_add(
                match &args[0] {
                    ColumnarValue::Array(values) => values.len(),
                    ColumnarValue::Scalar(_) => 1,
                },
                Ordering::SeqCst,
            );
            Ok(args[0].clone())
        }),
    ));
    session.register_udf(create_udf(
        "failed_view",
        vec![],
        DataType::Utf8View,
        Volatility::Volatile,
        Arc::new(|_| exec_err!("substring reached source failure")),
    ));
    for ansi in [false, true] {
        calls.store(0, Ordering::SeqCst);
        let sql = "SELECT substring(observed_view(s),1,12) AS r FROM inputs";
        let planned = plan_query(
            &session,
            sql,
            &SparkSqlOptions {
                ansi_mode: ansi,
                ..Default::default()
            },
        )
        .await?;
        let physical = planned.create_physical_plan(&session).await?;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "planning evaluated a volatile argument"
        );
        let batches = collect(physical, session.task_ctx()).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        calls.store(0, Ordering::SeqCst);
        for sql in [
            "SELECT substring(observed_view(s),1,CAST(NULL AS INT)) AS r FROM inputs",
            "SELECT substring(failed_view(),1,CAST(NULL AS INT)) AS r FROM inputs",
            "SELECT CASE WHEN false THEN substring(failed_view(),1,12) ELSE s END AS r FROM inputs",
            "SELECT substring(observed_view(s),1,12) AS r FROM inputs WHERE false",
        ] {
            query(&session, sql, ansi).await?;
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "NULL or dead input evaluated an unused argument"
        );
        assert!(
            query(
                &session,
                "SELECT substring(failed_view(),1,12) AS r FROM inputs",
                ansi
            )
            .await
            .err()
            .is_some_and(|error| error
                .to_string()
                .contains("substring reached source failure"))
        );
    }
    Ok(())
}
