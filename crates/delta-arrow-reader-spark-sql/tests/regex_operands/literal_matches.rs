//! Short literal regex patterns keep the original unanchored matching semantics.

use std::sync::Arc;

use arrow::array::Array;
use arrow::compute::concat_batches;
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
                    let plan = plan_query(&session, &sql, &options)
                        .await?
                        .create_physical_plan(&session)
                        .await?;
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
                                actual.column(0).as_ref() as &dyn Array,
                                expected.as_ref(),
                                "{sql}; ANSI={ansi_mode}",
                            );
                        }
                        (Err(actual), Err(expected)) => {
                            assert!(actual.to_string().contains("unclosed character class"));
                            assert!(expected.to_string().contains("unclosed character class"));
                        }
                        (actual, expected) => {
                            panic!("{sql}; ANSI={ansi_mode}: {actual:?} vs {expected:?}")
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
