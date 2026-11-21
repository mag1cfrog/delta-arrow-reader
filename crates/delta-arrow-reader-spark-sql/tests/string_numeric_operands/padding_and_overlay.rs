//! Integer literals select Utf8 kernels with the same padding and overlay results.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, StringViewArray};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};

use super::spark_session;

async fn execute(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
) -> Result<(String, RecordBatch)> {
    let plan = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        },
    )
    .await?
    .create_physical_plan(session)
    .await?;
    let description = displayable(plan.as_ref()).indent(false).to_string();
    let schema = plan.schema();
    let batches = collect(plan, session.task_ctx()).await?;
    Ok((description, concat_batches(&schema, &batches)?))
}

#[tokio::test]
async fn padding_and_overlay_match_the_view_signature_in_both_ansi_modes() -> Result<()> {
    let text = vec![
        Some("item"),
        Some(""),
        Some("aé"),
        Some("🙂z"),
        Some("a\0b"),
        None,
        Some("wide"),
    ];
    let other = vec![
        Some("xz"),
        Some("🙂"),
        Some(""),
        Some("éx"),
        Some("A"),
        Some("fill"),
        None,
    ];
    let utf8 = RecordBatch::try_from_iter([
        ("s", Arc::new(StringArray::from(text.clone())) as ArrayRef),
        ("p", Arc::new(StringArray::from(other.clone())) as ArrayRef),
    ])?;
    let views = RecordBatch::try_from_iter([
        ("s", Arc::new(StringViewArray::from(text)) as ArrayRef),
        ("p", Arc::new(StringViewArray::from(other)) as ArrayRef),
    ])?;
    let session = spark_session(32);
    session.register_batch("utf8_inputs", utf8.clone())?;
    session.register_batch("view_inputs", views.clone())?;
    session.register_batch("empty_utf8_inputs", utf8.slice(0, 0))?;
    session.register_batch("empty_view_inputs", views.slice(0, 0))?;

    for ansi in [false, true] {
        for (table, reference) in [
            ("utf8_inputs", "view_inputs"),
            ("empty_utf8_inputs", "empty_view_inputs"),
        ] {
            for expression in [
                "lpad(s,12,p)",
                "rpad(s,12,p)",
                "overlay(s,p,2)",
                "lpad(s,8)",
                "rpad(s,8)",
                "lpad(s,0,'')",
                "rpad(s,0,'')",
                "lpad(s,1,p)",
                "rpad(s,1,p)",
                "lpad(s,-2147483648,p)",
                "rpad(s,-2147483648,p)",
                "lpad(s,CAST(NULL AS INT),p)",
                "rpad(s,CAST(NULL AS INT),p)",
                "lpad(CAST(NULL AS STRING),2147483647,p)",
                "rpad(CAST(NULL AS STRING),2147483647,p)",
                "overlay(s,p,1,3)",
                "overlay(s,p,2,-1)",
                "overlay(s,p,1,0)",
                "overlay(s,p,1,1)",
                "overlay(s,p,0)",
                "overlay(s,p,-2147483648)",
                "overlay(s,p,2147483647)",
                "overlay(s,p,1,-2147483648)",
                "overlay(s,p,1,2147483647)",
                "overlay(s,p,CAST(NULL AS INT))",
                "overlay(s,p,1,CAST(NULL AS INT))",
            ] {
                let candidate = execute(
                    &session,
                    &format!("SELECT {expression} AS value FROM {table}"),
                    ansi,
                )
                .await;
                let reference = execute(
                    &session,
                    &format!("SELECT {expression} AS value FROM {reference}"),
                    ansi,
                )
                .await;
                if table == "utf8_inputs"
                    && matches!(
                        expression,
                        "lpad(s,12,p)" | "rpad(s,12,p)" | "overlay(s,p,2)"
                    )
                {
                    assert!(
                        candidate
                            .as_ref()
                            .is_ok_and(|(description, _)| !description.contains("AS Utf8View")),
                        "{candidate:?}",
                    );
                }
                assert_eq!(
                    candidate
                        .map(|(_, batch)| batch)
                        .map_err(|error| error.to_string()),
                    reference
                        .map(|(_, batch)| batch)
                        .map_err(|error| error.to_string()),
                    "{expression}, ANSI={ansi}, {table}",
                );
            }
        }
    }
    Ok(())
}
