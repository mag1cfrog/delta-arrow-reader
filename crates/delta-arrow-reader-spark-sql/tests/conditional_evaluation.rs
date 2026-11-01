//! Conditional arguments run once on the rows that need them.

use std::error::Error;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::{ArrayRef, AsArray, DictionaryArray, Int8Array, Int32Array};
use arrow::datatypes::{DataType, Int8Type, Int32Type};
use arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::execution::{FunctionRegistry, SessionStateBuilder};
use datafusion::logical_expr::{ColumnarValue, Volatility, col, create_udf, lit};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy, Debug)]
enum SqlFrontend {
    Spark { ansi_mode: bool },
    DataFusion,
}

fn create_session(frontend: SqlFrontend) -> SessionContext {
    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_target_partitions(1));
    let builder = match frontend {
        SqlFrontend::Spark { .. } => configure_spark_sql(builder),
        SqlFrontend::DataFusion => builder,
    };
    SessionContext::new_with_state(builder.build())
}

async fn execute_query(
    session: &SessionContext,
    frontend: SqlFrontend,
    sql: &str,
    evaluated_rows: &[Arc<AtomicUsize>; 3],
) -> TestResult<Vec<Option<i32>>> {
    let plan = match frontend {
        SqlFrontend::Spark { ansi_mode } => {
            let query = plan_query(
                session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    session_timezone: "America/Phoenix".into(),
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            query.create_physical_plan(session).await?
        }
        SqlFrontend::DataFusion => session.sql(sql).await?.create_physical_plan().await?,
    };
    assert_eq!(
        evaluated_rows.each_ref().map(|n| n.load(Ordering::SeqCst)),
        [0; 3],
        "planning evaluated a volatile argument"
    );
    let batches = collect(plan, session.task_ctx()).await?;
    let mut result = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        let value_type = match column.data_type() {
            DataType::Dictionary(_, value_type) => value_type.as_ref(),
            data_type => data_type,
        };
        assert_eq!(value_type, &DataType::Int32);
        // Dictionary encoding preserves the logical INT values counted here.
        let values = arrow::compute::cast(column, &DataType::Int32)?;
        result.extend(values.as_primitive::<Int32Type>().iter());
    }
    Ok(result)
}

#[tokio::test]
async fn conditional_arguments_run_once_only_where_needed() -> TestResult {
    for frontend in [
        SqlFrontend::DataFusion,
        SqlFrontend::Spark { ansi_mode: true },
        SqlFrontend::Spark { ansi_mode: false },
    ] {
        let session = create_session(frontend);
        let input = RecordBatch::try_from_iter([
            (
                "a",
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None])) as ArrayRef,
            ),
            (
                "b",
                Arc::new(Int32Array::from(vec![Some(9), Some(2), Some(9), None])) as ArrayRef,
            ),
            (
                "c",
                Arc::new(Int32Array::from(vec![None, None, None, Some(4)])) as ArrayRef,
            ),
            (
                "present",
                Arc::new(Int32Array::from(vec![5, 6, 7, 8])) as ArrayRef,
            ),
            (
                "dictionary",
                Arc::new(DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![Some(0), Some(1), Some(0), None]),
                    Arc::new(Int32Array::from(vec![None, Some(1)])),
                )?) as ArrayRef,
            ),
            (
                "dictionary_all_null",
                Arc::new(DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![0; 4]),
                    Arc::new(Int32Array::from(vec![None])),
                )?) as ArrayRef,
            ),
        ])?;
        assert!(!input.schema().field_with_name("present")?.is_nullable());
        session.register_batch("inputs", input.clone())?;
        let evaluated_rows = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
        for (name, count) in ["count_first", "count_second", "count_third"]
            .into_iter()
            .zip(&evaluated_rows)
        {
            let count = count.clone();
            session.register_udf(create_udf(
                name,
                vec![DataType::Int32],
                DataType::Int32,
                Volatility::Volatile,
                Arc::new(move |arguments| {
                    let arrays = ColumnarValue::values_to_arrays(arguments)?;
                    assert!(!arrays[0].is_empty(), "evaluated an empty input batch");
                    count.fetch_add(arrays[0].len(), Ordering::SeqCst);
                    Ok(ColumnarValue::Array(arrays[0].clone()))
                }),
            ));
        }
        session.register_udf(
            datafusion::functions::core::coalesce()
                .as_ref()
                .clone()
                .with_aliases(["fallback_alias"]),
        );
        let mut cases = vec![
            (
                "COALESCE(count_first(a),count_second(b),count_third(c))".to_string(),
                [4, 2, 1],
                vec![Some(1), Some(2), Some(3), Some(4)],
            ),
            (
                "COALESCE(CAST(7 AS INT),count_first(a))".to_string(),
                [0, 0, 0],
                vec![Some(7); 4],
            ),
            (
                "COALESCE(NULL,count_first(a),count_second(b),CAST(7 AS INT))".to_string(),
                [4, 2, 0],
                vec![Some(1), Some(2), Some(3), Some(7)],
            ),
            (
                "COALESCE(count_first(CAST(1 AS INT)),CAST('bad' AS INT))".to_string(),
                [1, 0, 0],
                vec![Some(1); 4],
            ),
            (
                "COALESCE(count_first(NULL),count_second(b),CAST(7 AS INT))".to_string(),
                [1, 4, 0],
                vec![Some(9), Some(2), Some(9), Some(7)],
            ),
            (
                "CASE WHEN count_first(a) IS NOT NULL THEN count_first(a) ELSE count_second(b) END"
                    .to_string(),
                [6, 2, 0],
                vec![Some(1), Some(2), Some(3), None],
            ),
        ];
        for function in ["COALESCE", "IFNULL", "NVL", "fallback_alias"] {
            cases.push((
                format!("{function}(count_first(a),count_second(b))"),
                [4, 2, 0],
                vec![Some(1), Some(2), Some(3), None],
            ));
            cases.push((
                format!("{function}(count_first(present),count_second(b))"),
                [4, 0, 0],
                vec![Some(5), Some(6), Some(7), Some(8)],
            ));
            cases.push((
                format!("{function}(present,CAST('bad' AS INT))"),
                [0, 0, 0],
                vec![Some(5), Some(6), Some(7), Some(8)],
            ));
        }
        if matches!(frontend, SqlFrontend::Spark { .. }) {
            cases.extend([
                (
                    "CAST(TRY_CAST(CAST(count_first(a) AS STRING) AS BOOLEAN) AS INT)".to_string(),
                    [4, 0, 0],
                    vec![Some(1), None, None, None],
                ),
                (
                    "NULLIF(count_first(a),count_second(b))".to_string(),
                    [4, 2, 0],
                    vec![Some(1), None, Some(3), None],
                ),
                (
                    "NULLIF(count_first(a),CAST(NULL AS DOUBLE))".to_string(),
                    [4, 0, 0],
                    vec![Some(1), None, Some(3), None],
                ),
                (
                    "NULLIF(count_first(present),NULLIF(1,1))".to_string(),
                    [4, 0, 0],
                    vec![Some(5), Some(6), Some(7), Some(8)],
                ),
                (
                    "CAST(NULLIF(CONCAT('bad',CAST(count_first(a) AS STRING)),CAST(NULL AS INT)) IS NOT NULL AS INT)"
                        .to_string(),
                    [4, 0, 0],
                    vec![Some(1), Some(0), Some(1), Some(0)],
                ),
                (
                    "NULLIF(count_first(a),CAST(count_second(b) AS DOUBLE))".to_string(),
                    [4, 2, 0],
                    vec![Some(1), None, Some(3), None],
                ),
                (
                    "NULLIF(count_first(a),CAST(count_second(b) AS FLOAT))".to_string(),
                    [4, 2, 0],
                    vec![Some(1), None, Some(3), None],
                ),
                (
                    "CAST(NULLIF(CAST(count_first(a) AS STRING),count_second(b)) AS INT)"
                        .to_string(),
                    [4, 2, 0],
                    vec![Some(1), None, Some(3), None],
                ),
                (
                    "NULLIF(count_first(a),CAST(count_second(b) AS STRING))".to_string(),
                    [4, 2, 0],
                    vec![Some(1), None, Some(3), None],
                ),
                (
                    "CAST(CAST(count_first(a) AS STRING)=count_second(b) AS INT)".to_string(),
                    [4, 4, 0],
                    vec![Some(0), None, Some(0), None],
                ),
                // DATEDIFF's result-width gap is separate (#152); counters use bounded INT days.
                (
                    "CAST(DATEDIFF(NULLIF(date_add(DATE '2026-01-01',count_first(a)),CAST(date_add(DATE '2026-01-01',count_second(b)) AS TIMESTAMP)),DATE '2026-01-01') AS INT)".to_string(),
                    [4, 2, 0],
                    vec![Some(1), None, Some(3), None],
                ),
                (
                    "CAST(DATEDIFF(CAST(NULLIF(CAST(date_add(DATE '2026-01-01',count_first(a)) AS TIMESTAMP_NTZ),CAST(date_add(DATE '2026-01-01',count_second(b)) AS TIMESTAMP)) AS DATE),DATE '2026-01-01') AS INT)".to_string(),
                    [4, 2, 0],
                    vec![Some(1), None, Some(3), None],
                ),
                (
                    "CAST(date_add(DATE '2026-01-01',count_first(a))=CAST(date_add(DATE '2026-01-01',count_second(b)) AS TIMESTAMP) AS INT)".to_string(),
                    [4, 4, 0],
                    vec![Some(0), None, Some(0), None],
                ),
                (
                    "NULLIF(count_first(a),CAST(count_second(b) AS DECIMAL(10,2)))".to_string(),
                    [4, 2, 0],
                    vec![Some(1), None, Some(3), None],
                ),
                (
                    "NULLIF(count_first(CAST(NULL AS INT)),CAST(count_second(b) AS DOUBLE))"
                        .to_string(),
                    [1, 0, 0],
                    vec![None; 4],
                ),
                (
                    "NULLIF(count_first(a),count_second(a))".to_string(),
                    [4, 2, 0],
                    vec![None; 4],
                ),
                (
                    "NULLIF(count_first(present),count_second(b))".to_string(),
                    [4, 4, 0],
                    vec![Some(5), Some(6), Some(7), Some(8)],
                ),
                (
                    "NULLIF(count_first(CAST(NULL AS INT)),count_second(b))".to_string(),
                    [1, 0, 0],
                    vec![None; 4],
                ),
                (
                    "NULLIF(count_first(CAST(1 AS INT)),count_second(b))".to_string(),
                    [1, 4, 0],
                    vec![Some(1); 4],
                ),
                (
                    "NULLIF(dictionary,count_second(b))".to_string(),
                    [0, 1, 0],
                    vec![None, Some(1), None, None],
                ),
                (
                    "NULLIF(dictionary_all_null,count_second(b))".to_string(),
                    [0, 0, 0],
                    vec![None; 4],
                ),
            ]);
        }
        for (expression, expected_evaluated_rows, expected_rows) in cases {
            for counter in &evaluated_rows {
                counter.store(0, Ordering::SeqCst);
            }
            let sql = format!("SELECT {expression} AS r FROM inputs");
            assert_eq!(
                execute_query(&session, frontend, &sql, &evaluated_rows).await?,
                expected_rows,
                "{frontend:?}: {sql}"
            );
            assert_eq!(
                evaluated_rows.each_ref().map(|n| n.load(Ordering::SeqCst)),
                expected_evaluated_rows,
                "{frontend:?}: {sql}"
            );
            for counter in &evaluated_rows {
                counter.store(0, Ordering::SeqCst);
            }
            let sql = format!("SELECT {expression} AS r FROM inputs WHERE false");
            assert!(
                execute_query(&session, frontend, &sql, &evaluated_rows)
                    .await?
                    .is_empty()
            );
            assert_eq!(
                evaluated_rows.each_ref().map(|n| n.load(Ordering::SeqCst)),
                [0; 3]
            );
        }
        let schema = DFSchema::try_from(input.schema())?;
        for function in ["coalesce", "ifnull", "nvl", "fallback_alias"] {
            // Evaluate directly so an empty relation cannot prune the expression.
            for first in [col("a"), lit(1_i32)] {
                let expression = session.udf(function)?.call(vec![
                    session.udf("count_first")?.call(vec![first]),
                    session.udf("count_second")?.call(vec![col("b")]),
                ]);
                let physical = session.create_physical_expr(expression, &schema)?;
                let result = physical.evaluate(&input.slice(0, 0))?.into_array(0)?;
                assert_eq!(result.len(), 0);
                assert_eq!(result.data_type(), &DataType::Int32);
                assert_eq!(
                    evaluated_rows.each_ref().map(|n| n.load(Ordering::SeqCst)),
                    [0; 3]
                );
            }
            if matches!(frontend, SqlFrontend::DataFusion) {
                let sql = format!("SELECT {function}(a,CAST('bad' AS INT)) AS r FROM inputs");
                let error = execute_query(&session, frontend, &sql, &evaluated_rows)
                    .await
                    .expect_err("selected invalid fallback must fail");
                assert!(error.to_string().contains("Cannot cast string 'bad'"));
            }
        }
    }
    Ok(())
}
