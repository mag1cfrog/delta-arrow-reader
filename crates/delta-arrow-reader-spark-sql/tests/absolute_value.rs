//! Invalid ABS calls must remain planning errors even inside unused expressions.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, AsArray, Float32Array};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Decimal128Type, Field, Float64Type, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
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
                assert!(
                    error
                        .to_string()
                        .contains("[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]")
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn abs_rejects_invalid_types_before_dead_case_branches_are_removed() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for ansi_mode in [false, true] {
        for sql in [
            "SELECT abs(true) AS r",
            "SELECT CASE WHEN true THEN 7 ELSE abs(true) END AS r",
        ] {
            let Err(error) = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await
            else {
                return Err(format!("Invalid ABS input reached physical planning: {sql}").into());
            };
            assert!(
                error
                    .to_string()
                    .contains("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]"),
                "{sql}: {error}"
            );
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

#[tokio::test]
async fn abs_admitted_numeric_providers_keep_declared_fields() -> TestResult {
    // These Arrow provider types test an existing contract, not new Spark type parity.
    let data_types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal32(6, 2),
        DataType::Decimal64(12, 2),
        DataType::Decimal128(20, 2),
        DataType::Decimal256(38, 2),
    ];
    let names = (0..data_types.len())
        .map(|i| format!("r{i}"))
        .collect::<Vec<_>>();
    let sql = format!(
        "SELECT {} FROM admitted_abs",
        (0..data_types.len())
            .map(|i| format!("abs(v{i}) AS r{i}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    for nullable in [false, true] {
        let values = Float32Array::from(vec![Some(-1.25), (!nullable).then_some(0.0), Some(1.25)]);
        let absolute_values =
            Float32Array::from(vec![Some(1.25), (!nullable).then_some(0.0), Some(1.25)]);
        let fields = data_types
            .iter()
            .enumerate()
            .map(|(i, data_type)| {
                Field::new(format!("v{i}"), data_type.clone(), nullable)
                    .with_metadata([("source".into(), "discard".into())].into())
            })
            .collect::<Vec<_>>();
        let columns = data_types
            .iter()
            .map(|data_type| {
                cast(
                    if data_type.is_unsigned_integer() {
                        &absolute_values
                    } else {
                        &values
                    },
                    data_type,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        let expected_schema = Arc::new(Schema::new(
            data_types
                .iter()
                .zip(&names)
                .map(|(data_type, name)| Field::new(name, data_type.clone(), nullable))
                .collect::<Vec<_>>(),
        ));
        let expected = RecordBatch::try_new(
            expected_schema.clone(),
            data_types
                .iter()
                .map(|data_type| cast(&absolute_values, data_type))
                .collect::<Result<Vec<_>, _>>()?,
        )?;
        for empty in [false, true] {
            let session = SessionContext::new_with_state(
                configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
            );
            let native = SessionContext::new();
            let rows = if empty { 0 } else { input.num_rows() };
            for context in [&session, &native] {
                context.register_batch("admitted_abs", input.slice(0, rows))?;
            }
            for ansi_mode in [false, true] {
                let query = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                assert_eq!(query.column_names, names);
                for (actual, expected) in query
                    .logical_plan
                    .schema()
                    .fields()
                    .iter()
                    .zip(expected_schema.fields())
                {
                    assert_eq!(actual.data_type(), expected.data_type());
                    assert_eq!(
                        actual.is_nullable(),
                        nullable,
                        "{actual:?}, ANSI={ansi_mode}, empty={empty}"
                    );
                    assert!(actual.metadata().is_empty());
                }
                let physical = query.create_physical_plan(&session).await?;
                assert_eq!(physical.schema(), expected_schema);
                let batches = collect(physical, session.task_ctx()).await?;
                for batch in &batches {
                    assert_eq!(batch.schema(), expected_schema);
                    for column in batch.columns() {
                        assert!(nullable || column.logical_null_count() == 0);
                        column.to_data().validate_full()?;
                    }
                }
                assert_eq!(
                    concat_batches(&expected_schema, &batches)?,
                    expected.slice(0, rows)
                );
            }
            let plain = native.sql(&sql).await?;
            let configured = session.sql(&sql).await?;
            assert_eq!(plain.schema(), configured.schema());
            let plain = plain.create_physical_plan().await?;
            let configured = configured.create_physical_plan().await?;
            assert_eq!(plain.schema(), configured.schema());
            assert_eq!(
                collect(plain, native.task_ctx()).await?,
                collect(configured, session.task_ctx()).await?
            );
        }
    }
    Ok(())
}
