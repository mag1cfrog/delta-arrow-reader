//! The ordinary DATE row path preserves CAST/TRY_CAST modes and string storage.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, DictionaryArray, Int32Array, LargeStringArray, StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Date32Type, Int32Type};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_sql_analyzer::parser::parse_date;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn date_rows(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
) -> TestResult<Vec<Option<i32>>> {
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        },
    )
    .await?;
    let plan = query.create_physical_plan(session).await?;
    assert_eq!(plan.schema().field(1).data_type(), &DataType::Date32);
    assert_eq!(plan.schema().field(1).name(), "r");
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        rows.extend(batch.column(1).as_primitive::<Date32Type>().iter());
    }
    Ok(rows)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date_row_path_preserves_cast_modes_and_storage() -> TestResult {
    for values in [
        vec![
            Some("2024-02-29"),
            Some("2024"),
            Some("2024-2-9"),
            Some("+12345-01-02"),
            Some("2024-02-29Tignored"),
            None,
        ],
        vec![Some("2024-02-30"), None],
        vec![Some("20240229"), None],
    ] {
        let expected = values
            .iter()
            .map(|value| {
                value.and_then(|value| {
                    parse_date(value)
                        .and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?)))
                        .ok()
                })
            })
            .collect::<Vec<_>>();
        let invalid_value = values
            .iter()
            .zip(&expected)
            .find_map(|(value, date)| if date.is_none() { *value } else { None });
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(values.clone())),
            Arc::new(LargeStringArray::from(values.clone())),
            Arc::new(StringViewArray::from(values.clone())),
            Arc::new(DictionaryArray::<Int32Type>::from_iter(values.clone())),
        ];
        for array in arrays {
            for batch_rows in [1, 32] {
                let session = SessionContext::new_with_state(
                    configure_spark_sql(
                        SessionStateBuilder::new()
                            .with_default_features()
                            .with_config(
                                SessionConfig::new()
                                    .with_target_partitions(2)
                                    .with_batch_size(batch_rows),
                            ),
                    )
                    .build(),
                );
                session.register_batch(
                    "dates",
                    RecordBatch::try_from_iter(vec![
                        (
                            "k",
                            Arc::new(Int32Array::from_iter_values(0..array.len() as i32))
                                as ArrayRef,
                        ),
                        ("v", array.clone()),
                    ])?,
                )?;
                for ansi in [false, true] {
                    for cast in ["CAST", "TRY_CAST"] {
                        let actual = date_rows(
                            &session,
                            &format!("SELECT k,{cast}(v AS DATE) AS r FROM dates ORDER BY k"),
                            ansi,
                        )
                        .await;
                        if cast == "CAST"
                            && ansi
                            && let Some(invalid_value) = invalid_value
                        {
                            let error =
                                actual.err().ok_or("strict CAST accepted an invalid date")?;
                            assert!(error.to_string().contains("[CAST_INVALID_INPUT]"));
                            assert!(error.to_string().contains(invalid_value), "{error}");
                        } else {
                            assert_eq!(
                                actual?,
                                expected,
                                "{cast}, ANSI={ansi}, {:?}",
                                array.data_type()
                            );
                        }
                    }
                }
            }
        }
    }
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for ansi in [false, true] {
        for cast in ["CAST", "TRY_CAST"] {
            assert_eq!(
                date_rows(
                    &session,
                    &format!("SELECT 0 AS k,{cast}('2024-02-29' AS DATE) AS r"),
                    ansi,
                )
                .await?,
                [Some(19782)]
            );
            let invalid = date_rows(
                &session,
                &format!("SELECT 0 AS k,{cast}('2024-02-30' AS DATE) AS r"),
                ansi,
            )
            .await;
            if cast == "CAST" && ansi {
                assert!(
                    invalid
                        .err()
                        .is_some_and(|error| error.to_string().contains("[CAST_INVALID_INPUT]"))
                );
            } else {
                assert_eq!(invalid?, [None]);
            }
        }
    }
    Ok(())
}
