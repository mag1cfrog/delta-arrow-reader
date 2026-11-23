//! The literal second-position path agrees with the original dynamic position.

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, ListArray, RecordBatch};
use arrow::datatypes::Int64Type;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

#[tokio::test]
async fn literal_second_position_matches_dynamic_position_for_bigint_lists()
-> Result<(), Box<dyn std::error::Error>> {
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(90)]),
        None,
        Some(vec![]),
        Some(vec![None]),
        Some(vec![Some(1), None, Some(3)]),
        Some(vec![Some(i64::MIN), Some(i64::MAX)]),
        Some(vec![Some(91)]),
    ]);
    let batch = RecordBatch::try_from_iter([
        ("arr", Arc::new(lists.slice(1, 5)) as Arc<dyn Array>),
        (
            "slot",
            Arc::new(Int32Array::from(vec![2; 5])) as Arc<dyn Array>,
        ),
        (
            "value",
            Arc::new(Int64Array::from(vec![
                Some(6),
                None,
                Some(7),
                None,
                Some(8),
            ])) as Arc<dyn Array>,
        ),
    ])?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_batch("inputs", batch)?;
    for ansi_mode in [false, true] {
        let query = plan_query(
            &session,
            "SELECT array_insert(arr, 2, value) AS literal, \
             array_insert(arr, slot, value) AS dynamic, \
             array_insert(arr, 1, value) AS first FROM inputs",
            &SparkSqlOptions {
                ansi_mode,
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        let plan = query.create_physical_plan(&session).await?;
        let plan_text = displayable(plan.as_ref()).indent(true).to_string();
        assert_eq!(
            plan_text
                .matches("spark_insert_bigint_at_second_position(")
                .count(),
            1
        );
        let batches = collect(plan, session.task_ctx()).await?;
        let mut rows = 0;
        for batch in batches {
            assert_eq!(batch.column(0).data_type(), batch.column(1).data_type());
            assert_eq!(batch.column(0).as_ref(), batch.column(1).as_ref());
            batch.column(0).to_data().validate_full()?;
            rows += batch.num_rows();
        }
        assert_eq!(rows, 5);
    }
    Ok(())
}

#[tokio::test]
async fn literal_second_position_preserves_derived_value_failure()
-> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::StringArray;

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_batch(
        "bad_input",
        RecordBatch::try_from_iter([
            (
                "arr",
                Arc::new(ListArray::from_iter_primitive::<
                    Int64Type,
                    Vec<Option<i64>>,
                    _,
                >([None])) as Arc<dyn Array>,
            ),
            (
                "slot",
                Arc::new(Int32Array::from(vec![2])) as Arc<dyn Array>,
            ),
            (
                "raw",
                Arc::new(StringArray::from(vec!["not an integer"])) as Arc<dyn Array>,
            ),
        ])?,
    )?;
    for ansi_mode in [false, true] {
        for position in ["2", "slot"] {
            let sql = format!(
                "SELECT array_insert(arr, {position}, value) FROM \
                 (SELECT arr, slot, CAST(raw AS BIGINT) AS value FROM bad_input) AS derived"
            );
            let query = plan_query(
                &session,
                &sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let plan = query.create_physical_plan(&session).await?;
            let result = collect(plan, session.task_ctx()).await;
            if ansi_mode {
                assert!(result.is_err(), "{sql}");
            } else {
                let batches = result?;
                assert_eq!(batches[0].num_rows(), 1, "{sql}");
                assert!(batches[0].column(0).is_null(0), "{sql}");
            }
        }
    }
    Ok(())
}
