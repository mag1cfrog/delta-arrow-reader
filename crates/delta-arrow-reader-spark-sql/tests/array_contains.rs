//! Compare array membership with Spark and keep separately owned differences visible.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, ListBuilder, NullArray, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde::Deserialize;
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

type InputRow<T> = (i32, Option<Vec<Option<T>>>, Option<T>);

#[derive(Deserialize)]
struct ReferenceInputs {
    spark_version: String,
    timezone: String,
    legacy_size_of_null: String,
    null_row_count: usize,
    integer_rows: Vec<InputRow<i32>>,
    string_rows: Vec<InputRow<String>>,
}

fn create_session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

fn register_inputs(
    session: &SessionContext,
    inputs: &ReferenceInputs,
    batch_size: usize,
) -> TestResult {
    session.register_batch(
        "null_inputs",
        RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from_iter_values(
                    0..i32::try_from(inputs.null_row_count)?,
                )) as ArrayRef,
            ),
            (
                "a",
                Arc::new(NullArray::new(inputs.null_row_count)) as ArrayRef,
            ),
        ])?,
    )?;
    let integers = arrow::array::ListArray::from_iter_primitive::<Int32Type, _, _>(
        inputs
            .integer_rows
            .iter()
            .map(|(_, array, _)| array.clone()),
    );
    let mut strings = ListBuilder::new(StringBuilder::new());
    for (_, array, _) in &inputs.string_rows {
        if let Some(values) = array {
            for value in values {
                strings.values().append_option(value.as_deref());
            }
        }
        strings.append(array.is_some());
    }
    for (name, ids, arrays, lookups) in [
        (
            "integer_inputs",
            Int32Array::from_iter_values(inputs.integer_rows.iter().map(|(id, _, _)| *id)),
            Arc::new(integers) as ArrayRef,
            Arc::new(Int32Array::from_iter(
                inputs.integer_rows.iter().map(|(_, _, lookup)| *lookup),
            )) as ArrayRef,
        ),
        (
            "string_inputs",
            Int32Array::from_iter_values(inputs.string_rows.iter().map(|(id, _, _)| *id)),
            Arc::new(strings.finish()) as ArrayRef,
            Arc::new(StringArray::from_iter(
                inputs
                    .string_rows
                    .iter()
                    .map(|(_, _, lookup)| lookup.as_deref()),
            )) as ArrayRef,
        ),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", arrays.data_type().clone(), true),
            Field::new("n", lookups.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), arrays, lookups])?;
        let batches = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|start| batch.slice(start, batch_size.min(batch.num_rows() - start)))
            .collect();
        session.register_table(name, Arc::new(MemTable::try_new(schema, vec![batches])?))?;
    }
    Ok(())
}

async fn capture_query_outcome(session: &SessionContext, query: &Value) -> TestResult<Value> {
    let options = SparkSqlOptions {
        ansi_mode: query["ansi"].as_bool().ok_or("missing ANSI mode")?,
        ..SparkSqlOptions::default()
    };
    let planned = match plan_query(
        session,
        query["sql"].as_str().ok_or("missing SQL")?,
        &options,
    )
    .await
    {
        Ok(query) => query,
        Err(error) => {
            return Ok(json!({"status":"error", "stage":"planning", "error":error.to_string()}));
        }
    };
    let logical_nullable: Vec<_> = planned
        .logical_plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.is_nullable())
        .collect();
    let plan = match planned.create_physical_plan(session).await {
        Ok(plan) => plan,
        Err(error) => {
            return Ok(
                json!({"status":"error", "stage":"physical_planning", "error":error.to_string()}),
            );
        }
    };
    let schema = plan.schema();
    let batches = match collect(plan, session.task_ctx()).await {
        Ok(batches) => batches,
        Err(error) => {
            return Ok(json!({"status":"error", "stage":"execution", "error":error.to_string()}));
        }
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|col| {
                        if col.is_null(row) {
                            Ok(Value::Null)
                        } else {
                            array_value_to_string(col.as_ref(), row).map(Value::String)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(
        json!({"status":"ok", "rows":rows, "names":planned.column_names, "types":schema.fields().iter().map(|f| format!("{:?}", f.data_type())).collect::<Vec<_>>(), "logical_nullable":logical_nullable, "physical_nullable":schema.fields().iter().map(|f| f.is_nullable()).collect::<Vec<_>>()}),
    )
}

fn assert_spark_result(query: &Value, actual: &Value) -> TestResult {
    let expected = &query["expected"];
    let context = format!("{} ANSI={}: {actual}", query["id"], query["ansi"]);
    assert_eq!(actual["status"], expected["status"], "{context}");
    if expected["status"] == "error" {
        let message = actual["error"].as_str().ok_or("missing error message")?;
        match expected["condition"]
            .as_str()
            .ok_or("missing Spark error condition")?
        {
            "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" => {
                assert_eq!(actual["stage"], "planning", "{context}");
                assert!(message.contains("does not support type Int32"), "{context}");
            }
            "CAST_INVALID_INPUT" | "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION" => {
                assert!(
                    matches!(
                        actual["stage"].as_str(),
                        Some("physical_planning" | "execution")
                    ),
                    "{context}"
                );
                assert!(
                    message.contains("Cannot cast string 'bad' to value of Int32 type"),
                    "{context}"
                );
            }
            "SCALAR_SUBQUERY_TOO_MANY_ROWS" => {
                assert!(
                    message.contains("Scalar subquery returned more than one row"),
                    "{context}"
                );
            }
            "DIVIDE_BY_ZERO" => {
                assert!(
                    message.to_ascii_lowercase().contains("divide by zero")
                        || message.to_ascii_lowercase().contains("division by zero"),
                    "{context}"
                );
            }
            condition => return Err(format!("unhandled reference error {condition}").into()),
        }
        return Ok(());
    }
    let rows: Vec<Vec<Value>> = serde_json::from_value(expected["rows"].clone())?;
    let rows: Vec<Vec<Value>> = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    Value::Null | Value::String(_) => value,
                    value => Value::String(value.to_string()),
                })
                .collect()
        })
        .collect();
    assert_eq!(actual["rows"], json!(rows), "{context}");
    let fields = expected["schema"]["fields"]
        .as_array()
        .ok_or("missing Spark fields")?;
    assert_eq!(
        actual["names"],
        json!(fields.iter().map(|f| &f["name"]).collect::<Vec<_>>()),
        "{context}"
    );
    let types = fields
        .iter()
        .map(|field| match field["type"].as_str() {
            Some("boolean") => Ok("Boolean"),
            Some("byte") => Ok("Int8"),
            Some("integer") => Ok("Int32"),
            Some("long") => Ok("Int64"),
            _ => Err("unhandled reference output type"),
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(actual["types"], json!(types), "{context}");
    // Constant folding may narrow physical nullability; any returned NULL must
    // remain representable in both the logical and the physical output schema.
    for column in 0..fields.len() {
        if rows.iter().any(|row| row[column].is_null()) {
            assert_eq!(actual["logical_nullable"][column], true, "{context}");
            assert_eq!(actual["physical_nullable"][column], true, "{context}");
        }
    }
    Ok(())
}

fn assert_retained_difference(query: &Value, actual: &Value, null_rows: usize) -> TestResult {
    let context = format!("{} ANSI={}: {actual}", query["id"], query["ansi"]);
    match query["retained_difference"].as_str() {
        Some("mixed_decimal_coercion") => {
            assert_eq!(query["expected"]["status"], "ok");
            assert_eq!(actual["status"], "error", "{context}");
            assert_eq!(actual["stage"], "planning", "{context}");
            assert!(
                actual["error"]
                    .as_str()
                    .ok_or("missing error")?
                    .contains("coercion from List(Decimal128(10, 2)), Decimal128(8, 1)"),
                "{context}"
            );
        }
        Some("null_type_admission") => {
            assert_eq!(
                query["expected"]["condition"],
                "DATATYPE_MISMATCH.NULL_TYPE"
            );
            assert_eq!(actual["status"], "ok", "{context}");
            let rows = if query["id"] == "registered_null_type_empty_batch" {
                0
            } else {
                null_rows
            };
            assert_eq!(
                actual["rows"],
                json!(vec![vec![Value::Null]; rows]),
                "{context}"
            );
            assert_eq!(actual["types"], json!(["Boolean"]), "{context}");
            assert_eq!(actual["logical_nullable"], json!([true]), "{context}");
            assert_eq!(actual["physical_nullable"], json!([true]), "{context}");
        }
        difference => return Err(format!("unhandled retained difference {difference:?}").into()),
    }
    Ok(())
}

#[tokio::test]
async fn array_contains_values_and_input_errors() -> TestResult {
    let mut lines = include_str!("fixtures/array-contains-spark.jsonl").lines();
    let inputs: ReferenceInputs =
        serde_json::from_str(lines.next().ok_or("missing reference inputs")?)?;
    assert_eq!(inputs.spark_version, "4.2.0");
    assert_eq!(inputs.timezone, "UTC");
    assert_eq!(inputs.legacy_size_of_null, "true");
    let queries: Vec<Value> = lines.map(serde_json::from_str).collect::<Result<_, _>>()?;
    assert_eq!(queries.len(), 360);
    assert_eq!(
        queries
            .iter()
            .filter(|q| q.get("retained_difference").is_some())
            .count(),
        12
    );
    for batch_size in [1, 32] {
        let session = create_session(batch_size);
        register_inputs(&session, &inputs, batch_size)?;
        for query in &queries {
            let actual = capture_query_outcome(&session, query).await?;
            if query.get("retained_difference").is_some() {
                assert_retained_difference(query, &actual, inputs.null_row_count)?;
            } else {
                assert_spark_result(query, &actual)?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn array_contains_requires_two_arguments() -> TestResult {
    let session = create_session(32);
    for sql in [
        "SELECT ARRAY_CONTAINS(ARRAY(1))",
        "SELECT ARRAY_CONTAINS(ARRAY(1), 1, 2)",
    ] {
        assert!(
            plan_query(&session, sql, &SparkSqlOptions::default())
                .await
                .is_err(),
            "{sql}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn array_membership_preserves_volatile_lookup_evaluation() -> TestResult {
    let session = create_session(32);
    session.register_batch(
        "lookups",
        RecordBatch::try_from_iter(vec![(
            "n",
            Arc::new(Int32Array::from(vec![Some(1), None, Some(2)])) as ArrayRef,
        )])?,
    )?;
    let evaluated_rows = Arc::new(AtomicUsize::new(0));
    let counter = evaluated_rows.clone();
    session.register_udf(create_udf(
        "count_lookup",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |arguments| {
            let arrays = ColumnarValue::values_to_arrays(arguments)?;
            counter.fetch_add(arrays[0].len(), Ordering::SeqCst);
            Ok(ColumnarValue::Array(Arc::clone(&arrays[0])))
        }),
    ));
    let cases = [
        (
            "SELECT ARRAY_CONTAINS(CAST(ARRAY() AS ARRAY<INT>), count_lookup(n)) AS r FROM lookups",
            vec![vec![Some(false), None, Some(false)]],
            3,
        ),
        (
            "SELECT ARRAY_CONTAINS(ARRAY(1, NULL), count_lookup(n)) AS r FROM lookups",
            vec![vec![Some(true), None, None]],
            3,
        ),
        (
            "SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, count_lookup(n) AS n FROM lookups) t",
            vec![vec![None; 3]],
            0,
        ),
        (
            "SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT a,n FROM (SELECT ARRAY(count_lookup(n),NULL) AS a, CAST(NULL AS INT) AS n FROM lookups) x) y",
            vec![vec![None; 3]],
            0,
        ),
        (
            "SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT ARRAY(1,NULL) AS a, count_lookup(n) AS n FROM lookups) t",
            vec![vec![Some(true), None, None]],
            3,
        ),
        (
            "SELECT ARRAY_CONTAINS(a,n) AS r, ARRAY_CONTAINS(ARRAY(1,NULL),n) AS needed FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, count_lookup(n) AS n FROM lookups) t",
            vec![vec![None; 3], vec![Some(true), None, None]],
            3,
        ),
        (
            "SELECT ARRAY_CONTAINS(a,n) AS r, ARRAY_CONTAINS(a,1) AS needed FROM (SELECT ARRAY(count_lookup(n),NULL) AS a, CAST(NULL AS INT) AS n FROM lookups) t",
            vec![vec![None; 3], vec![Some(true), None, None]],
            3,
        ),
        (
            "SELECT ARRAY_CONTAINS(ARRAY(1,NULL),n) AS r, ARRAY_CONTAINS(ARRAY(2,NULL),n) AS other FROM (SELECT count_lookup(n) AS n FROM lookups) t",
            vec![vec![Some(true), None, None], vec![None, None, Some(true)]],
            3,
        ),
        (
            "SELECT l.n IS NULL AS r FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, count_lookup(n) AS n FROM lookups) l JOIN range(2) u ON ARRAY_CONTAINS(l.a,l.n)",
            vec![vec![]],
            0,
        ),
        (
            "SELECT l.n IS NULL AS r FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, count_lookup(n) AS n FROM lookups) l LEFT JOIN range(1) u ON ARRAY_CONTAINS(l.a,l.n) ORDER BY l.n NULLS LAST",
            vec![vec![Some(false), Some(false), Some(true)]],
            3,
        ),
        (
            "SELECT l.n IS NULL AS r FROM (SELECT ARRAY(1,2) AS a, count_lookup(n) AS n FROM lookups) l JOIN range(2) u ON ARRAY_CONTAINS(l.a,l.n)",
            vec![vec![Some(false); 4]],
            3,
        ),
        (
            "SELECT true AS r FROM lookups l LEFT JOIN (SELECT CAST(NULL AS ARRAY<INT>) AS a, count_lookup(n) AS n FROM lookups) u ON ARRAY_CONTAINS(u.a,u.n)",
            vec![vec![Some(true); 3]],
            0,
        ),
        (
            "SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, count_lookup(n) AS n FROM lookups WHERE n<0) t",
            vec![vec![]],
            0,
        ),
    ];
    for ansi_mode in [true, false] {
        for (sql, expected, expected_evaluated_rows) in &cases {
            evaluated_rows.store(0, Ordering::SeqCst);
            let query = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let plan = query.create_physical_plan(&session).await?;
            assert_eq!(
                evaluated_rows.load(Ordering::SeqCst),
                0,
                "planning must not evaluate a volatile input: {sql}"
            );
            let batches = collect(plan, session.task_ctx()).await?;
            let values: Vec<Vec<_>> = (0..expected.len())
                .map(|column| {
                    batches
                        .iter()
                        .flat_map(|batch| batch.column(column).as_boolean().iter())
                        .collect()
                })
                .collect();
            assert_eq!(&values, expected, "{sql}");
            assert_eq!(
                evaluated_rows.load(Ordering::SeqCst),
                *expected_evaluated_rows,
                "{sql}"
            );
        }
    }
    Ok(())
}
