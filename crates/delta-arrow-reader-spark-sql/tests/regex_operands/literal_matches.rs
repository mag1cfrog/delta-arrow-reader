//! Short literal regex patterns keep the original unanchored matching semantics.

use std::sync::Arc;

use arrow::compute::{cast, concat_batches};
use datafusion::physical_plan::{collect, displayable};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};

use super::{DFSchema, DataType, Expr, Like, RecordBatch, ScalarValue, StringArray, TestResult};
use super::{col, lit, session};

#[tokio::test]
async fn literal_matches_agree_with_the_previous_scalar_regex() -> TestResult {
    let session = session(32, true);
    let values = StringArray::from(vec![
        Some("item"),
        Some("préitem后"),
        Some("ITEM"),
        Some("with\nitem\nnewline"),
        Some("item2"),
        Some("x.y"),
        Some("a_b%"),
        Some(""),
        Some("0123456789abcdefghijklmnopqrstuvw"),
        None,
    ]);
    let input = RecordBatch::try_from_iter([("s", Arc::new(values) as _)])?;
    let empty = input.slice(0, 0);
    session.register_batch("regex_words", input.clone())?;
    session.register_batch("empty_regex_words", empty.clone())?;
    let schema = DFSchema::try_from(input.schema())?;
    let patterns = [
        (Some("item"), "'item'", true),
        (Some("ITEM"), "'ITEM'", true),
        (Some("2"), "'2'", true),
        (
            Some("0123456789abcdefghijklmnopqrstuv"),
            "'0123456789abcdefghijklmnopqrstuv'",
            true,
        ),
        (
            Some("0123456789abcdefghijklmnopqrstuvw"),
            "'0123456789abcdefghijklmnopqrstuvw'",
            false,
        ),
        (Some(""), "''", false),
        (Some("(?i)item"), "'(?i)item'", false),
        (Some("^item$"), "'^item$'", false),
        (Some("a_b%"), "'a_b%'", false),
        (Some(r"\."), r"r'\.'", false),
        (Some("pré"), "'pré'", false),
        (Some("["), "'['", false),
        (None, "CAST(NULL AS STRING)", false),
    ];

    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        };
        for (pattern, pattern_sql, literal_word) in patterns {
            let old = Expr::SimilarTo(Like::new(
                false,
                Box::new(col("s")),
                Box::new(lit(ScalarValue::Utf8(pattern.map(str::to_owned)))),
                None,
                false,
            ));
            let old = session.create_physical_expr(old, &schema)?;
            for (name, batch) in [("regex_words", &input), ("empty_regex_words", &empty)] {
                for function in ["regexp", "regexp_like", "rlike"] {
                    let sql = format!("SELECT {function}(s, {pattern_sql}) AS matched FROM {name}");
                    let query = plan_query(&session, &sql, &options).await?;
                    assert_eq!(
                        query
                            .logical_plan
                            .display_indent()
                            .to_string()
                            .contains(" SIMILAR TO "),
                        !literal_word,
                        "{sql}; ANSI={ansi_mode}",
                    );
                    let plan = query.create_physical_plan(&session).await?;
                    let result_schema = plan.schema();
                    assert_eq!(result_schema.field(0).data_type(), &DataType::Boolean);
                    if literal_word && name == "regex_words" {
                        assert!(
                            displayable(plan.as_ref())
                                .indent(false)
                                .to_string()
                                .contains(" LIKE "),
                            "{sql}; ANSI={ansi_mode}"
                        );
                    }
                    let actual = collect(plan, session.task_ctx()).await;
                    let expected = old.evaluate(batch);
                    match (actual, expected) {
                        (Ok(actual), Ok(expected)) => {
                            let actual = concat_batches(&result_schema, &actual)?;
                            let expected = expected.into_array(batch.num_rows())?;
                            assert_eq!(
                                actual.column(0).as_ref(),
                                expected.as_ref(),
                                "{sql}; ANSI={ansi_mode}",
                            );
                        }
                        (Err(actual), Err(expected)) => {
                            assert!(actual.to_string().contains("unclosed character class"));
                            assert!(expected.to_string().contains("unclosed character class"));
                        }
                        (actual, expected) => {
                            return Err(format!(
                                "{sql}; ANSI={ansi_mode}: {actual:?} vs {expected:?}"
                            )
                            .into());
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn literal_matches_keep_the_regex_path_for_other_storage_and_types() -> TestResult {
    let session = session(32, true);
    let values = StringArray::from(vec![Some("item"), Some("ITEM"), None]);
    session.register_batch(
        "large_regex_words",
        RecordBatch::try_from_iter([("s", cast(&values, &DataType::LargeUtf8)?)])?,
    )?;
    session.register_batch(
        "view_regex_words",
        RecordBatch::try_from_iter([("s", cast(&values, &DataType::Utf8View)?)])?,
    )?;
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        };
        for function in ["regexp", "regexp_like", "rlike"] {
            for table in ["large_regex_words", "view_regex_words"] {
                let sql = format!("SELECT {function}(s, 'item') AS matched FROM {table}");
                let query = plan_query(&session, &sql, &options).await?;
                assert!(
                    query
                        .logical_plan
                        .display_indent()
                        .to_string()
                        .contains(" SIMILAR TO ")
                );
                let plan = query.create_physical_plan(&session).await?;
                let schema = plan.schema();
                let batches = collect(plan, session.task_ctx()).await?;
                let actual = concat_batches(&schema, &batches)?;
                let expected =
                    arrow::array::BooleanArray::from(vec![Some(true), Some(false), None]);
                assert_eq!(
                    actual.column(0).as_ref(),
                    &expected as &dyn arrow::array::Array
                );
            }
            let sql = format!("SELECT {function}(42, 'item')");
            let query = plan_query(&session, &sql, &options).await?;
            let error = query
                .create_physical_plan(&session)
                .await
                .err()
                .ok_or("expected the nonstring regexp operand to be rejected")?;
            assert!(error.to_string().contains("SIMILAR TO expression"));
        }
    }
    Ok(())
}
