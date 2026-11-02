//! NULL predicates must retain the conversion of string-to-floating expressions.

use std::collections::BTreeSet;
use std::error::Error;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn floating_casts_retain_spark_schemas_and_required_errors() -> TestResult {
    let records = include_str!("fixtures/floating-cast-nullability-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 172);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
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
        );
        for case in &records[1..] {
            let actual = capture_floating_cast_result(&session, case)
                .await
                .unwrap_or_else(|error| {
                    let message = error.to_string();
                    if message.contains("[CAST_INVALID_INPUT] Cannot cast string") {
                        json!({"condition":"CAST_INVALID_INPUT"})
                    } else {
                        json!({"unexpected_error":message})
                    }
                });
            if actual != case["expected"] {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn capture_floating_cast_result(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    let fields = query
        .logical_plan
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let physical = schema.field(index);
            assert_eq!(physical.name(), &query.column_names[index]);
            assert_eq!(physical.data_type(), field.data_type());
            assert_eq!(physical.metadata(), field.metadata());
            // Optimizers may narrow nullability after folding constants or unreachable branches.
            assert!(!physical.is_nullable() || field.is_nullable());
            let data_type = match field.data_type() {
                DataType::Int32 => "integer",
                DataType::Int64 => "long",
                DataType::Boolean => "boolean",
                DataType::Float32 => "float",
                DataType::Float64 => "double",
                DataType::Utf8 => "string",
                other => return Err(format!("unexpected result type: {other}").into()),
            };
            Ok(json!({"name":physical.name(),"type":data_type,
                "nullable":field.is_nullable(),"metadata":field.metadata()}))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for (index, column) in batch.columns().iter().enumerate() {
            assert!(schema.field(index).is_nullable() || column.null_count() == 0);
        }
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        Ok(if column.is_null(row) {
                            Value::Null
                        } else {
                            json!(match column.data_type() {
                                DataType::Float32 => format!(
                                    "{:08x}",
                                    column.as_primitive::<Float32Type>().value(row).to_bits()
                                ),
                                DataType::Float64 => format!(
                                    "{:016x}",
                                    column.as_primitive::<Float64Type>().value(row).to_bits()
                                ),
                                _ => array_value_to_string(column.as_ref(), row)?,
                            })
                        })
                    })
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({"schema":{"type":"struct","fields":fields},"rows":rows}))
}

#[tokio::test]
async fn native_null_safe_comparisons_remain_nonnullable() -> TestResult {
    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::logical_expr::{ExprSchemable, Operator, binary_expr, lit};

    for operator in [Operator::IsDistinctFrom, Operator::IsNotDistinctFrom] {
        let expression = binary_expr(lit(ScalarValue::Int32(None)), operator, lit(1_i32));
        assert!(!expression.nullable(&DFSchema::empty())?);
        assert!(!expression.to_field(&DFSchema::empty())?.1.is_nullable());
    }
    let session = SessionContext::new();
    let query = session
        .sql(
            "SELECT a IS DISTINCT FROM b AS different,a IS NOT DISTINCT FROM b AS same \
         FROM (VALUES(1,1),(1,2),(NULL,1),(1,NULL),(NULL,NULL)) t(a,b)",
        )
        .await?;
    assert!(
        query
            .schema()
            .fields()
            .iter()
            .all(|field| !field.is_nullable())
    );
    let batches = query.collect().await?;
    let mut rows = Vec::new();
    for batch in batches {
        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .all(|field| !field.is_nullable())
        );
        for column in batch.columns() {
            assert_eq!(column.null_count(), 0);
        }
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| column.as_boolean().value(row))
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![false, true],
            vec![false, true],
            vec![true, false],
            vec![true, false],
            vec![true, false]
        ]
    );
    Ok(())
}

#[tokio::test]
async fn configured_session_retains_native_cast_behavior() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT CAST('1.5' AS DOUBLE) AS r",
        "SELECT TRY_CAST('bad' AS FLOAT) AS r",
        "SELECT CAST(NULL AS FLOAT) AS r",
    ] {
        let expected = native.sql(sql).await?.collect().await?;
        let actual = configured.sql(sql).await?.collect().await?;
        assert_eq!(actual, expected, "{sql}");
    }
    Ok(())
}
