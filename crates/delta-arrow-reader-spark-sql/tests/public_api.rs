//! Host session, result naming and execution contracts of the public API.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn session() -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(2)),
        )
        .build(),
    )
}

#[tokio::test]
async fn native_cast_functions_lower_before_local_preparation() -> TestResult {
    let session = session();
    for expression in [
        "arrow_cast(v,'Int32')",
        "arrow_try_cast(v,'Int32')",
        "cast_to_type(v,CAST(0 AS INT))",
        "try_cast_to_type(v,CAST(0 AS INT))",
    ] {
        let batches = session
            .sql(&format!(
                "SELECT CAST(v AS INT) AS x,{expression} AS y FROM (VALUES ('1'),(NULL)) t(v)"
            ))
            .await?
            .collect()
            .await?;
        let mut rows = Vec::new();
        for batch in batches {
            assert_eq!(batch.schema().field(0).name(), "x");
            assert_eq!(batch.schema().field(1).name(), "y");
            assert!(
                batch
                    .columns()
                    .iter()
                    .all(|column| column.data_type() == &DataType::Int32)
            );
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            if column.is_null(row) {
                                Ok(None)
                            } else {
                                array_value_to_string(column.as_ref(), row).map(Some)
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
        }
        assert_eq!(
            rows,
            [vec![Some("1".into()); 2], vec![None; 2]],
            "{expression}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn reject_ragged_values_before_column_coercion() -> TestResult {
    let session = session();
    for ansi_mode in [true, false] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        };
        for (sql, actual, expected) in [
            ("VALUES (0, MAP('k',1)), (1)", 1, 2),
            ("VALUES (0), (1, MAP('k',1))", 2, 1),
            ("VALUES (MAP('k',1)), (MAP('k',2), 0)", 2, 1),
            ("VALUES (MAP('k',1), 0), (MAP('k',2))", 1, 2),
            ("VALUES (0, CAST('NaN' AS FLOAT)), (1)", 1, 2),
            ("VALUES (0), (1, CAST('NaN' AS FLOAT))", 2, 1),
            ("VALUES (0, CAST(1 AS DECIMAL(10,2))), (1)", 1, 2),
            ("VALUES (0), (1, CAST(1 AS DECIMAL(10,2)))", 2, 1),
            ("VALUES (0, 1), (2)", 1, 2),
            ("VALUES (0), (1, 2)", 2, 1),
        ] {
            let error = plan_query(&session, sql, &options).await.expect_err(sql);
            assert!(error.to_string().contains(&format!(
                "Inconsistent data length across values list: got {actual} values in row 1 but expected {expected}"
            )), "{sql}: {error}");
        }
        let query = plan_query(&session,
            "SELECT id AS row_id, m['k'] AS value FROM VALUES (0, MAP('k',1)), (1, MAP('k',NULL)) t(id,m) ORDER BY id",
            &options,
        ).await?;
        assert_eq!(query.column_names, ["row_id", "value"]);
        let physical = query.create_physical_plan(&session).await?;
        assert_eq!(physical.schema().field(1).data_type(), &DataType::Int32);
        assert!(query.logical_plan.schema().field(1).is_nullable());
        assert!(physical.schema().field(1).is_nullable());
        let mut rows = Vec::new();
        for batch in collect(physical, session.task_ctx()).await? {
            for row in 0..batch.num_rows() {
                rows.push((
                    array_value_to_string(batch.column(0).as_ref(), row)?,
                    if batch.column(1).is_null(row) {
                        None
                    } else {
                        Some(array_value_to_string(batch.column(1).as_ref(), row)?)
                    },
                ));
            }
        }
        assert_eq!(rows, [("0".into(), Some("1".into())), ("1".into(), None)]);
    }
    Ok(())
}

#[tokio::test]
async fn preserve_host_resources_output_names_and_order() -> TestResult {
    let runtime = Arc::new(RuntimeEnv::default());
    let state = configure_spark_sql(
        SessionStateBuilder::new()
            .with_default_features()
            .with_runtime_env(runtime.clone())
            .with_config(
                SessionConfig::new()
                    .with_batch_size(32)
                    .with_target_partitions(5),
            ),
    )
    .build();
    let session = SessionContext::new_with_state(state);
    let source = session
        .sql("SELECT * FROM (VALUES (1), (3), (2)) AS values_table(id)")
        .await?;
    session.register_table("registered", source.into_view())?;
    let query = plan_query(
        &session,
        "SELECT CAST(id AS BIGINT) AS row_id FROM registered ORDER BY id DESC",
        &SparkSqlOptions::default(),
    )
    .await?;
    assert_eq!(query.column_names, ["row_id"]);
    let physical = query.create_physical_plan(&session).await?;
    assert_eq!(physical.schema().field(0).name(), "row_id");
    assert!(Arc::ptr_eq(&session.runtime_env(), &runtime));
    assert_eq!(session.copied_config().batch_size(), 32);
    assert_eq!(session.copied_config().target_partitions(), 5);
    for _ in 0..2 {
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("expected BIGINT output")?;
            rows.extend(ids.values().iter().copied());
        }
        assert_eq!(rows, [3, 2, 1]);
    }
    let count = session
        .sql("SELECT COUNT(*) FROM registered")
        .await?
        .collect()
        .await?;
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].num_rows(), 1);
    assert_eq!(array_value_to_string(count[0].column(0).as_ref(), 0)?, "3");
    Ok(())
}

#[tokio::test]
async fn preserve_hidden_sort_keys_below_scalar_subqueries() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(1)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    );
    let query = plan_query(
        &session,
        "SELECT CAST(v + (SELECT max(x) FROM VALUES (7),(9) AS u(x)) AS DECIMAL(10,0)) AS r FROM VALUES (3,30),(CAST(NULL AS INT),0),(1,10),(2,20) AS t(k,v) ORDER BY k ASC NULLS FIRST",
        &SparkSqlOptions::default(),
    ).await?;
    let physical = query.create_physical_plan(&session).await?;
    let batches = collect(physical, session.task_ctx()).await?;
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(array_value_to_string(batch.column(0).as_ref(), row)?);
        }
    }
    assert_eq!(rows, ["9", "19", "29", "39"]);
    Ok(())
}

#[tokio::test]
async fn reject_correlated_scalar_grouping_without_single_row_guarantee() -> TestResult {
    let session = session();
    for sql in [
        "SELECT (SELECT count(*) FROM VALUES (0,1),(0,2) t(id,g) WHERE id=o.k GROUP BY g) AS r FROM VALUES (0) o(k)",
        "SELECT (SELECT count(*) FROM VALUES (1,10),(2,10) i(k,v) WHERE (i.k=o.k AND false) OR i.v=o.v GROUP BY i.k) AS r FROM VALUES (1,10) o(k,v)",
    ] {
        let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
        let error = query
            .create_physical_plan(&session)
            .await
            .err()
            .ok_or("accepted a potentially multi-row correlated scalar subquery")?;
        assert!(
            error.to_string().contains("non-correlated columns"),
            "{sql}: {error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn volatile_functions_run_only_during_execution() -> TestResult {
    let session = session();
    let invocations = Arc::new(AtomicUsize::new(0));
    let counter = invocations.clone();
    session.register_udf(create_udf(
        "next_value",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |arguments| {
            let arrays = ColumnarValue::values_to_arrays(arguments)?;
            let values =
                (0..arrays[0].len()).map(|_| counter.fetch_add(1, Ordering::SeqCst) as i64);
            Ok(ColumnarValue::Array(Arc::new(
                Int64Array::from_iter_values(values),
            )))
        }),
    ));
    for data_type in ["INT", "DATE", "BOOLEAN"] {
        let query = plan_query(
            &session,
            &format!(
                "SELECT v=CAST(NULL AS {data_type}) AS compared, next_value(v) AS sequence FROM VALUES ('bad') t(v)"
            ),
            &SparkSqlOptions::default(),
        )
        .await?;
        let error = query
            .create_physical_plan(&session)
            .await
            .expect_err("a volatile sibling must not hide a deterministic cast failure");
        assert!(
            error.to_string().contains("CAST_INVALID_INPUT")
                || error.to_string().contains("Cannot cast string 'bad'"),
            "{error}"
        );
        assert_eq!(invocations.load(Ordering::SeqCst), 0);
    }
    let query = plan_query(
        &session,
        "SELECT CAST(v AS INT) AS parsed, next_value(v) AS sequence FROM VALUES ('7'), ('9') t(v)",
        &SparkSqlOptions::default(),
    )
    .await?;
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    let physical = query.create_physical_plan(&session).await?;
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        0,
        "planning must not consume volatile values"
    );
    let batches = collect(physical, session.task_ctx()).await?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn reject_writes_external_sources_and_multiple_statements() -> TestResult {
    let session = session();
    let existing = session.sql("SELECT 7 AS id").await?;
    session.register_table("existing", existing.into_view())?;
    for sql in [
        "CREATE TABLE created (id INT)",
        "DROP TABLE existing",
        "INSERT INTO existing VALUES (1)",
        "SET spark.sql.ansi.enabled=false",
    ] {
        let error = plan_query(&session, sql, &SparkSqlOptions::default())
            .await
            .err()
            .ok_or("write command was accepted")?;
        assert!(
            error
                .to_string()
                .contains("only read-only queries are supported"),
            "{error}"
        );
    }
    for sql in [
        "SELECT * FROM parquet.`/unavailable/file.parquet`",
        "SELECT 1; SELECT 2",
    ] {
        assert!(
            plan_query(&session, sql, &SparkSqlOptions::default())
                .await
                .is_err(),
            "accepted unsupported statement: {sql}"
        );
    }
    assert!(!session.table_exist("created")?);
    let unchanged = session
        .sql("SELECT id FROM existing")
        .await?
        .collect()
        .await?;
    assert_eq!(unchanged.len(), 1);
    assert_eq!(unchanged[0].num_rows(), 1);
    assert_eq!(
        array_value_to_string(unchanged[0].column(0).as_ref(), 0)?,
        "7"
    );
    Ok(())
}
