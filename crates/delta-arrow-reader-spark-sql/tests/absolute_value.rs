//! Invalid ABS calls must remain planning errors even inside unused expressions.

use std::error::Error;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Decimal128Type, Float64Type, Int32Type};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn abs_rejects_wrong_arity_before_unused_subqueries_are_removed() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        };
        for (arguments, count) in [("", 0), ("1,2", 2)] {
            for sql in [
                format!("SELECT abs({arguments}) AS r"),
                format!(
                    "SELECT EXISTS(SELECT id FROM range(1) WHERE false AND (SELECT abs({arguments}) FROM range(1))>0) AS r"
                ),
            ] {
                let result = plan_query(&session, &sql, &options).await;
                let Err(error) = result else {
                    return Err(format!("Invalid ABS reached physical planning: {sql}").into());
                };
                assert!(
                    error.to_string().contains(&format!(
                        "Spark `abs` function requires 1 argument, got {count}"
                    )),
                    "{sql}: {error}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn abs_valid_calls_keep_integer_float_decimal_and_null_results() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for ansi_mode in [false, true] {
        let query = plan_query(
            &session,
            "SELECT abs(-7) AS i,abs(CAST(-1.5 AS DOUBLE)) AS f,abs(CAST(-1.25 AS DECIMAL(10,2))) AS d,abs(CAST(NULL AS INT)) AS n",
            &SparkSqlOptions { ansi_mode, ..SparkSqlOptions::default() },
        ).await?;
        let physical = query.create_physical_plan(&session).await?;
        let batches = collect(physical, session.task_ctx()).await?;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["i", "f", "d", "n"]
        );
        assert_eq!(batch.column(0).as_primitive::<Int32Type>().value(0), 7);
        assert_eq!(batch.column(1).as_primitive::<Float64Type>().value(0), 1.5);
        let decimal = batch.column(2).as_primitive::<Decimal128Type>();
        assert_eq!(decimal.precision(), 10);
        assert_eq!(decimal.scale(), 2);
        assert_eq!(decimal.value(0), 125);
        assert!(batch.column(3).is_null(0));
    }
    Ok(())
}
