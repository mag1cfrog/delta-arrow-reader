//! Literal prefix LIKE agrees with the former case-sensitive matcher.

use arrow::compute::{cast, concat_batches};

use super::{
    Arc, ArrayRef, DataFusionResult, DataType, RecordBatch, SparkSqlOptions, StringArray, collect,
    plan_query, session,
};

#[tokio::test]
async fn literal_prefix_like_matches_the_default_escape_path() -> DataFusionResult<()> {
    let session = session(32);
    let values = StringArray::from(vec![
        Some("item"),
        Some("ITEM"),
        Some("item\n"),
        Some("itemé"),
        Some("éitem"),
        Some("item%tail"),
        Some("0123456789abcdefghijklmnopqrstuvw"),
        Some(""),
        None,
    ]);
    let inputs = [
        ("prefix_slices", Arc::new(values.slice(1, 8)) as ArrayRef),
        ("prefix_empty", Arc::new(values.slice(0, 0)) as ArrayRef),
        ("prefix_views", cast(&values, &DataType::Utf8View)?),
        ("prefix_strings", Arc::new(values) as ArrayRef),
    ];
    for (name, values) in &inputs {
        session.register_batch(
            *name,
            RecordBatch::try_from_iter([("s", Arc::clone(values))])?,
        )?;
    }
    let patterns = [
        ("'item%'", true),
        ("'ITEM%'", true),
        ("'i%'", true),
        ("'0123456789abcdefghijklmnopqrstuv%'", true),
        ("'0123456789abcdefghijklmnopqrstuvw%'", false),
        ("'%item%'", false),
        ("'i_em%'", false),
        (r"r'item\%%'", false),
        ("'é%'", false),
        ("'%'", false),
        ("''", false),
        ("CAST(NULL AS STRING)", false),
    ];
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        };
        for (name, _) in &inputs {
            for (pattern, prefix) in patterns {
                let mut results = Vec::new();
                let explicit_escape = if prefix { ", '!'" } else { r", r'\'" };
                for escape in ["", explicit_escape] {
                    let sql = format!("SELECT like(s, {pattern}{escape}) AS matched FROM {name}");
                    let query = plan_query(&session, &sql, &options).await?;
                    let plan = query.create_physical_plan(&session).await?;
                    let schema = plan.schema();
                    assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
                    let batches = collect(plan, session.task_ctx()).await?;
                    results.push(concat_batches(&schema, &batches)?);
                }
                assert_eq!(results[0], results[1], "{name} {pattern}; ANSI={ansi_mode}");
            }
        }
        let sql = r"SELECT like(s, r'item\q%') AS matched FROM prefix_strings";
        let query = plan_query(&session, sql, &options).await?;
        let error = query.create_physical_plan(&session).await;
        assert!(
            matches!(error, Err(error) if error.to_string().contains("INVALID_FORMAT.ESC_IN_THE_MIDDLE"))
        );
        for escape in ["", ", '!'"] {
            let sql = format!(
                "SELECT assert_true(like(s, 'item%'{escape})) FROM (VALUES ('other')) t(s) LIMIT 0"
            );
            let query = plan_query(&session, &sql, &options).await?;
            let error = query.create_physical_plan(&session).await;
            assert!(
                matches!(error, Err(error) if error.to_string().contains("USER_RAISED_EXCEPTION"))
            );
        }
    }
    Ok(())
}
