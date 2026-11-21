//! ASCII prefix matching preserves the original ILIKE results.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

#[tokio::test]
async fn prefix_ilike_matches_existing_lower_then_like() -> Result<()> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let ascii = StringArray::from(vec![
        Some("item"),
        Some("ITEMnext"),
        Some("item\n"),
        Some("item%value"),
        Some("0item"),
        Some("KELVIN"),
        Some("Snow"),
        Some(""),
        Some("0123456789abcdefghijklmnopqrstuvw"),
        None,
    ]);
    let mixed = StringArray::from(vec![
        Some("İtem"),
        Some("item🙂"),
        Some("Kelvin"),
        Some("ſnow"),
        Some("Σ"),
        Some(""),
        None,
    ]);
    let inputs = [
        ("ascii_inputs", Arc::new(ascii) as ArrayRef, true),
        ("mixed_inputs", Arc::new(mixed.clone()) as ArrayRef, true),
        (
            "empty_inputs",
            Arc::new(mixed.slice(0, 0)) as ArrayRef,
            true,
        ),
        ("view_inputs", cast(&mixed, &DataType::Utf8View)?, false),
    ];
    for (name, values, _) in &inputs {
        session.register_batch(
            *name,
            RecordBatch::try_from_iter([("s", Arc::clone(values))])?,
        )?;
    }

    let patterns = [
        ("'item%'", true),
        ("'ITEM%'", true),
        ("'k%'", true),
        ("'S%'", true),
        ("'0123456789abcdefghijklmnopqrstuv%'", true),
        ("'0123456789abcdefghijklmnopqrstuvw%'", false),
        ("'é%'", false),
        ("'i_em%'", false),
        ("'%item%'", false),
        (r"r'item\%%'", false),
        ("'item'", false),
        ("'%'", false),
        ("''", false),
        ("CAST(NULL AS STRING)", false),
    ];
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        };
        for (name, _, utf8) in &inputs {
            for (pattern, literal_prefix) in patterns {
                let mut results = Vec::new();
                for escape in ["", r", r'\'"] {
                    let sql = format!("SELECT ilike(s, {pattern}{escape}) AS matched FROM {name}");
                    let query = plan_query(&session, &sql, &options).await?;
                    assert_eq!(
                        query
                            .logical_plan
                            .display_indent()
                            .to_string()
                            .contains("spark_ilike_ascii_prefix"),
                        *utf8 && literal_prefix && escape.is_empty(),
                        "{sql}; ANSI={ansi_mode}",
                    );
                    let plan = query.create_physical_plan(&session).await?;
                    let schema = plan.schema();
                    assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
                    let batches = collect(plan, session.task_ctx()).await?;
                    results.push(concat_batches(&schema, &batches)?);
                }
                assert_eq!(results[0], results[1], "{name} {pattern}; ANSI={ansi_mode}");
            }
        }

        let sql = r"SELECT ilike(s, r'item\q%') AS matched FROM ascii_inputs";
        let query = plan_query(&session, sql, &options).await?;
        assert!(
            !query
                .logical_plan
                .display_indent()
                .to_string()
                .contains("spark_ilike_ascii_prefix")
        );
        let error = query.create_physical_plan(&session).await;
        assert!(matches!(error, Err(error) if error.to_string().contains("INVALID_FORMAT")));
    }
    Ok(())
}
