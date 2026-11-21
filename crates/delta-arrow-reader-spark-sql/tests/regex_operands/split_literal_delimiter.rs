//! SPLIT keeps its regex results when a literal delimiter uses string splitting.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, LargeStringArray, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::string::spark_split::{parse_regex, split_to_array};

#[tokio::test]
async fn split_literal_delimiter_and_regex_controls_agree_with_existing_splitter() -> Result<()> {
    let values = [
        Some("-a--"),
        Some("a-"),
        Some("--"),
        Some("no match"),
        Some(""),
        Some("é-a-😀"),
        Some("x|a.A|~"),
        Some("A a a"),
        None,
        Some("a:"),
    ];
    let limits = [
        Some(i32::MIN),
        Some(i32::MAX),
        Some(2),
        Some(1),
        Some(0),
        Some(-1),
        Some(2),
        Some(0),
        Some(1),
        None,
    ];
    let limit_array = Arc::new(Int32Array::from(limits.to_vec()));
    let input_batch = |value_array: ArrayRef| {
        RecordBatch::try_from_iter([("s", value_array), ("n", limit_array.clone())])
    };
    let utf8 = input_batch(Arc::new(StringArray::from(values.to_vec())))?;
    let inputs = [
        ("split_inputs", utf8.clone()),
        (
            "split_large_inputs",
            input_batch(Arc::new(LargeStringArray::from(values.to_vec())))?,
        ),
        ("empty_split_inputs", utf8.slice(0, 0)),
    ];
    let patterns = [
        (Some("-"), "'-'"),
        (Some(" "), "' '"),
        (Some("a"), "'a'"),
        (Some(":"), "':'"),
        (Some("~"), "'~'"),
        (Some("é"), "'é'"),
        (Some("."), "'.'"),
        (Some("[|]"), "'[|]'"),
        (Some(r"\."), r"r'\.'"),
        (Some(""), "''"),
        (None, "CAST(NULL AS STRING)"),
    ];

    for ansi_mode in [false, true] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
        );
        for (name, input) in &inputs {
            session.register_batch(*name, input.clone())?;
        }
        let options = SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        };
        for (pattern, pattern_sql) in patterns {
            let regex = pattern.map(parse_regex).transpose()?;
            for (table, input) in &inputs {
                let sql = format!("SELECT split(s, {pattern_sql}, n) AS parts FROM {table}");
                let plan = plan_query(&session, &sql, &options)
                    .await?
                    .create_physical_plan(&session)
                    .await?;
                let schema = plan.schema();
                let batches = collect(plan, session.task_ctx()).await?;
                let actual = concat_batches(&schema, &batches)?;
                let mut expected = ListBuilder::new(StringBuilder::new());
                for index in 0..input.num_rows() {
                    match (values[index], regex.as_ref(), limits[index]) {
                        (Some(value), Some(regex), Some(limit)) => {
                            expected.append_value(split_to_array(value, regex, limit)?);
                        }
                        _ => expected.append_null(),
                    }
                }
                assert_eq!(
                    actual.column(0).as_ref(),
                    &expected.finish() as &dyn Array,
                    "{sql}; ANSI={ansi_mode}",
                );
            }
        }

        let invalid = plan_query(&session, "SELECT split(s, '[') FROM split_inputs", &options)
            .await?
            .create_physical_plan(&session)
            .await?;
        let result = collect(invalid, session.task_ctx()).await;
        assert!(matches!(result, Err(error) if error.to_string().contains("Invalid regex")));
    }
    Ok(())
}
