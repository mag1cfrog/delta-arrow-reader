//! Compare observable STRING and negation results after all six set operations.

use std::error::Error;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/float_inputs.rs"]
mod float_inputs;
use float_inputs::register_float_inputs;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn union_float_results_match_spark() -> TestResult {
    assert_set_result_fixture(
        include_str!("fixtures/union-zero-results-spark.jsonl"),
        332,
        0,
    )
    .await
}

#[tokio::test]
async fn intersect_except_float_results_match_spark() -> TestResult {
    assert_set_result_fixture(
        include_str!("fixtures/set-operation-zero-results-spark.jsonl"),
        4096,
        0,
    )
    .await
}

#[tokio::test]
async fn constant_result_context_matches_spark() -> TestResult {
    assert_set_result_fixture(
        include_str!("fixtures/set-constant-context-spark.jsonl"),
        592,
        0,
    )
    .await
}

#[tokio::test]
async fn enclosing_query_context_preserves_recorded_contract() -> TestResult {
    assert_set_result_fixture(
        include_str!("fixtures/set-parent-context-spark.jsonl"),
        220,
        24,
    )
    .await
}

#[tokio::test]
async fn set_operations_inside_scalar_exists_and_in_subqueries_execute() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for ansi in [false, true] {
        for kind in ["FLOAT", "DOUBLE"] {
            let set = format!(
                "SELECT CAST('-0.0' AS {kind}) AS k INTERSECT SELECT CAST('0.0' AS {kind}) AS k"
            );
            for (sql, expected) in [
                (
                    format!(
                        "SELECT CAST((SELECT k FROM ({set}) q) AS STRING) AS text,CAST(-(SELECT k FROM ({set}) q) AS STRING) AS negated"
                    ),
                    json!([["-0.0", "0.0"]]),
                ),
                (
                    format!(
                        "SELECT CAST(EXISTS({set}) AS STRING) AS text,CAST(NOT EXISTS({set}) AS STRING) AS negated"
                    ),
                    json!([["true", "false"]]),
                ),
                (
                    format!(
                        "SELECT CAST(0 IN ({set}) AS STRING) AS text,CAST(1 IN ({set}) AS STRING) AS negated"
                    ),
                    json!([["true", "false"]]),
                ),
            ] {
                let case = json!({"sql":sql,"ansi":ansi,"schema":{"fields":[
                    {"name":"text","type":"string"},{"name":"negated","type":"string"}
                ]}});
                assert_eq!(
                    execute_zero_result(&session, &case).await?,
                    expected,
                    "{sql}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn set_analysis_does_not_fold_unused_growing_strings() -> TestResult {
    use arrow::array::Float64Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_batch(
        "zero_input",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Float64, true)])),
            vec![Arc::new(Float64Array::from(vec![-0.0_f64]))],
        )?,
    )?;
    let largest_input = Arc::new(AtomicUsize::new(0));
    let observed = largest_input.clone();
    session.register_udf(create_udf(
        "double_probe_string",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(move |args| {
            let [ColumnarValue::Scalar(ScalarValue::Utf8(Some(value)))] = args else {
                return datafusion::common::internal_err!("Expected a scalar probe string");
            };
            observed.fetch_max(value.len(), Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                value.repeat(2),
            ))))
        }),
    ));
    let mut sizes = Vec::new();
    for depth in [8, 16] {
        largest_input.store(0, Ordering::SeqCst);
        let mut input = "SELECT k,'x' AS s FROM zero_input".to_string();
        for _ in 0..depth {
            input = format!("SELECT k,double_probe_string(s) AS s FROM ({input}) q");
        }
        let sql = format!("SELECT k FROM ({input}) q INTERSECT SELECT CAST(0 AS DOUBLE) AS k");
        plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
        sizes.push(largest_input.load(Ordering::SeqCst));
    }
    assert_eq!(sizes, vec![0, 0], "unused literal input sizes: {sizes:?}");
    Ok(())
}

async fn assert_set_result_fixture(
    fixture: &str,
    expected_cases: usize,
    expected_zero_sign_exceptions: usize,
) -> TestResult {
    let records = fixture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, expected_cases);
    assert_eq!(
        records[1..]
            .iter()
            .filter(|case| is_recorded_limit_zero_sign_case(case))
            .count(),
        expected_zero_sign_exceptions,
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        for partitions in [1, 2] {
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(
                            SessionConfig::new()
                                .with_batch_size(batch_size)
                                .with_target_partitions(partitions),
                        ),
                )
                .build(),
            );
            register_float_inputs(&session, &records[0], batch_size, partitions)?;
            if records[0].get("nonnull_tables").is_some() {
                register_non_nullable_inputs(&session)?;
            }
            for case in records[1..].iter().filter(|case| {
                case.get("partitions")
                    .is_none_or(|count| count == &json!(partitions))
            }) {
                let actual = execute_zero_result(&session, case).await;
                if !matches!(&actual, Ok(rows) if matches_spark_or_permitted_zero_sign(case, rows))
                {
                    differences.push(format!("{} ANSI={} adaptive={} partitions={partitions} batch={batch_size}: expected {}, got {actual:?}",
                        case["id"],case["ansi"],case["adaptive"],case["rows"]));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn is_recorded_limit_zero_sign_case(case: &Value) -> bool {
    let Some(id) = case["id"].as_str() else {
        return false;
    };
    let Some(shape) = id
        .strip_prefix("float_")
        .or_else(|| id.strip_prefix("double_"))
    else {
        return false;
    };
    matches!(
        shape,
        "unused_volatile_projection_limit"
            | "unused_constant_projection_limit"
            | "unused_function_projection_limit"
            | "single_unused_volatile_projection_limit"
            | "unused_volatile_alias_limit"
            | "unused_volatile_cast_before_limit"
    )
}

fn matches_spark_or_permitted_zero_sign(case: &Value, actual: &Value) -> bool {
    if !is_recorded_limit_zero_sign_case(case) {
        return actual == &case["rows"];
    }
    // #690 permits either representative, while negation must still reverse its sign.
    case["rows"] == json!([["0.0", "-0.0"]])
        && (actual == &case["rows"] || actual == &json!([["-0.0", "0.0"]]))
}

#[test]
fn zero_sign_exception_preserves_negation_and_row_contracts() {
    let mut case = json!({"id":"double_unused_constant_projection_limit", "rows":[["0.0","-0.0"]]});
    for rows in [json!([["0.0", "-0.0"]]), json!([["-0.0", "0.0"]])] {
        assert!(matches_spark_or_permitted_zero_sign(&case, &rows));
    }
    for rows in [
        json!([]),
        json!([[null, null]]),
        json!([["0.0", "0.0"]]),
        json!([["-0.0", "-0.0"]]),
        json!([["1.0", "-1.0"]]),
        json!([["0.0", "-0.0"], ["0.0", "-0.0"]]),
    ] {
        assert!(
            !matches_spark_or_permitted_zero_sign(&case, &rows),
            "{rows}"
        );
    }
    case["id"] = json!("double_nested_identity_projection_limit");
    assert!(!matches_spark_or_permitted_zero_sign(
        &case,
        &json!([["-0.0", "0.0"]])
    ));
    case["id"] = json!("double_unused_constant_projection_limit");
    case["rows"] = json!([["1.0", "-1.0"]]);
    assert!(!matches_spark_or_permitted_zero_sign(
        &case,
        &json!([["-0.0", "0.0"]])
    ));
}

fn register_non_nullable_inputs(session: &SessionContext) -> TestResult {
    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;

    for (name, array) in [
        (
            "float_nonnull",
            Arc::new(Float32Array::from(vec![0.0_f32])) as ArrayRef,
        ),
        (
            "double_nonnull",
            Arc::new(Float64Array::from(vec![0.0_f64])) as ArrayRef,
        ),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            array.data_type().clone(),
            false,
        )]));
        session.register_batch(name, RecordBatch::try_new(schema, vec![array])?)?;
    }
    Ok(())
}

#[tokio::test]
async fn nested_set_constants_work_without_optimizer_rules() -> TestResult {
    let records = include_str!("fixtures/set-operation-zero-results-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for optimize in [false, true] {
        let builder = configure_spark_sql(SessionStateBuilder::new().with_default_features());
        let builder = if optimize {
            builder
        } else {
            builder.with_optimizer_rules(vec![])
        };
        let session = SessionContext::new_with_state(builder.build());
        for id in [
            "float_nested_distinct_intersect_False",
            "double_nested_computed_intersect_False",
        ] {
            let case = records
                .iter()
                .find(|case| case["id"] == id)
                .ok_or("missing nested reference")?;
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions::default(),
            )
            .await?;
            assert!(
                query
                    .logical_plan
                    .display_indent()
                    .to_string()
                    .contains("preserve_set_constant")
            );
            let optimized = session.state().optimize(&query.logical_plan)?;
            assert_eq!(
                optimized
                    .display_indent()
                    .to_string()
                    .contains("preserve_set_constant"),
                !optimize
            );
            assert_eq!(
                execute_zero_result(&session, case).await?,
                case["rows"],
                "{id}, optimize={optimize}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn set_output_normalization_does_not_repeat_volatile_inputs_or_hide_errors() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(1)),
        )
        .build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_zero",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Volatile,
        Arc::new(move |args| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            observed.fetch_add(arrays[0].len(), Ordering::SeqCst);
            Ok(args[0].clone())
        }),
    ));
    for (operation, right_value, expected_calls, expected_rows, expected_text) in [
        ("INTERSECT", "CAST('0.0' AS DOUBLE)", 1, 1, "0.0"),
        (
            "UNION",
            "observed_zero(CAST('-0.0' AS DOUBLE))",
            2,
            1,
            "0.0",
        ),
        (
            "UNION ALL",
            "observed_zero(CAST('-0.0' AS DOUBLE))",
            2,
            2,
            "-0.0",
        ),
    ] {
        calls.store(0, Ordering::SeqCst);
        let sql = format!(
            "SELECT CAST(k AS STRING) AS text FROM (SELECT observed_zero(CAST('-0.0' AS DOUBLE)) AS k {operation} SELECT {right_value} AS k) q"
        );
        let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
        let physical = query.create_physical_plan(&session).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 0, "{operation} planning");
        let batches = collect(physical, session.task_ctx()).await?;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            expected_calls,
            "{operation} execution"
        );
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            expected_rows
        );
        for batch in batches {
            for row in 0..batch.num_rows() {
                assert_eq!(
                    array_value_to_string(batch.column(0).as_ref(), row)?,
                    expected_text
                );
            }
        }
    }
    for operation in [
        "UNION",
        "UNION ALL",
        "INTERSECT",
        "INTERSECT ALL",
        "EXCEPT",
        "EXCEPT ALL",
    ] {
        let query = plan_query(
            &session,
            &format!("SELECT CAST('bad' AS DOUBLE) AS k {operation} SELECT CAST(0 AS DOUBLE) AS k"),
            &SparkSqlOptions {
                ansi_mode: true,
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        let outcome = match query.create_physical_plan(&session).await {
            Ok(physical) => collect(physical, session.task_ctx()).await,
            Err(error) => Err(error),
        };
        assert!(
            outcome
                .expect_err("set normalization hid an invalid cast")
                .to_string()
                .contains("CAST_INVALID_INPUT")
        );
    }
    Ok(())
}

async fn execute_zero_result(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    let expected_fields = case["schema"]["fields"]
        .as_array()
        .ok_or("missing fields")?;
    assert_eq!(schema.fields().len(), expected_fields.len());
    assert_eq!(query.column_names.len(), expected_fields.len());
    for (index, (field, expected)) in schema.fields().iter().zip(expected_fields).enumerate() {
        assert_eq!(
            field.name(),
            expected["name"].as_str().ok_or("missing name")?
        );
        assert_eq!(&query.column_names[index], field.name());
        assert_eq!(expected["type"], "string");
        assert!(matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ));
        assert_eq!(
            field.data_type(),
            query.logical_plan.schema().field(index).data_type()
        );
    }
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for (index, column) in batch.columns().iter().enumerate() {
                values.push(if column.is_null(row) {
                    assert!(schema.field(index).is_nullable());
                    assert!(query.logical_plan.schema().field(index).is_nullable());
                    Value::Null
                } else {
                    json!(array_value_to_string(column.as_ref(), row)?)
                });
            }
            rows.push(values);
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!(rows))
}

#[tokio::test]
async fn speculative_analysis_does_not_evaluate_stable_functions() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let records = include_str!("fixtures/set-operation-zero-results-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    register_float_inputs(&session, &records[0], 1, 1)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "query_zero",
        vec![],
        DataType::Float64,
        Volatility::Stable,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Float64(Some(-0.0)),
            ))
        }),
    ));
    for (left_value, right_value) in [
        ("CAST('-0.0' AS DOUBLE)", "query_zero()"),
        ("query_zero()", "CAST('0.0' AS DOUBLE)"),
        ("query_zero()", "query_zero()"),
    ] {
        calls.store(0, Ordering::SeqCst);
        let sql = format!(
            "SELECT CAST(k AS STRING) AS text FROM (SELECT q.k FROM double_mixed_left p LEFT JOIN (SELECT {left_value} AS k,label FROM double_negative) q ON p.label=q.label INTERSECT SELECT {right_value} AS k) result"
        );
        let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "resolution evaluated a query-time function: {sql}"
        );
        let batches = collect(
            query.create_physical_plan(&session).await?,
            session.task_ctx(),
        )
        .await?;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert!(calls.load(Ordering::SeqCst) > 0);
    }
    Ok(())
}
