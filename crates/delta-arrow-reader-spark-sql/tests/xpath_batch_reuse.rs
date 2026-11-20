//! A whole batch and fresh executions of its individual rows agree.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

const FUNCTIONS: [&str; 9] = [
    "xpath",
    "xpath_boolean",
    "xpath_double",
    "xpath_float",
    "xpath_int",
    "xpath_long",
    "xpath_number",
    "xpath_short",
    "xpath_string",
];

fn session() -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(1)),
        )
        .build(),
    )
}

fn input(rows: &[(Option<&str>, Option<&str>)]) -> Result<RecordBatch> {
    let xmls: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|(xml, _)| *xml)));
    let paths: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|(_, path)| *path)));
    Ok(RecordBatch::try_from_iter([
        ("xml", xmls),
        ("path", paths),
    ])?)
}

async fn execute(
    session: &SessionContext,
    projection: &str,
    table: &str,
    ansi_mode: bool,
) -> Result<RecordBatch> {
    let planned = plan_query(
        session,
        &format!("SELECT {projection} FROM {table}"),
        &SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        },
    )
    .await?;
    let physical = planned.create_physical_plan(session).await?;
    let batches = collect(physical, session.task_ctx()).await?;
    let [batch]: [RecordBatch; 1] = batches.try_into().map_err(|batches: Vec<_>| {
        DataFusionError::Internal(format!(
            "expected one XPath test batch, got {}",
            batches.len()
        ))
    })?;
    Ok(batch)
}

fn captured_values(batch: &RecordBatch) -> Result<Value> {
    let types: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{:?}", field.data_type()))
        .collect();
    let mut rows = Vec::new();
    for row in 0..batch.num_rows() {
        let mut values = Vec::new();
        for column in batch.columns() {
            let value = ScalarValue::try_from_array(column, row)?;
            values.push(match value {
                value if value.is_null() => Value::Null,
                ScalarValue::Float32(Some(value)) => json!(format!("{:08x}", value.to_bits())),
                ScalarValue::Float64(Some(value)) => json!(format!("{:016x}", value.to_bits())),
                ScalarValue::List(list) => {
                    let elements = list.value(0);
                    let strings =
                        elements
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "expected XPath to emit a list of strings".into(),
                                )
                            })?;
                    json!(strings.iter().collect::<Vec<_>>())
                }
                value => json!(value.to_string()),
            });
        }
        rows.push(values);
    }
    Ok(json!({"types": types, "rows": rows}))
}

fn assert_staging_observations(kind: &str, observations: Vec<Value>) -> Result<()> {
    let reference: Value =
        serde_json::from_str(include_str!("fixtures/xpath-batch-reuse/staging.json"))
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
    assert_eq!(
        Value::Array(observations),
        reference[kind],
        "{kind} changed"
    );
    Ok(())
}

#[tokio::test]
async fn xpath_batch_reuse_matches_fresh_rows_in_both_ansi_modes() -> Result<()> {
    let rows = [
        (Some("<r><x>42</x></r>"), Some("/r/x")),
        (Some("<r><x>bad number</x><y>-0</y></r>"), Some("/r/y")),
        (None, Some("[")),
        (Some("<r><x>not a number</x></r>"), Some("/r/x")),
        (Some("<r><x>32768</x></r>"), Some("/r/x")),
        (Some("<r><x>-32769</x></r>"), Some("/r/x")),
        (Some("<r><x>2147483648</x></r>"), Some("/r/x")),
        (Some("<r><x>-2147483649</x></r>"), Some("/r/x")),
        (Some("<r><x>9223372036854775808</x></r>"), Some("/r/x")),
        (Some("<r><x>-9223372036854775809</x></r>"), Some("/r/x")),
        (Some("<r><x>3.5e38</x></r>"), Some("/r/x")),
        (Some("<r><x>1e-1000</x></r>"), Some("/r/x")),
        (Some("<r x=\"2\"/>"), Some("/r/@x")),
        (Some("<r><x>27</x></r>"), Some("/r/x/text()")),
        (Some("<r/>"), Some("/r/missing")),
        (Some("<r><x></x></r>"), Some("/r/x")),
        (Some(""), Some("[")),
        (Some("invalid XML"), Some("")),
        (Some("invalid XML"), None),
    ];
    let session = session();
    let mut observations = Vec::new();
    for (argument, fixture) in [
        ("path", rows),
        (
            "'/r/x'",
            rows.map(|(xml, path)| {
                // Use empty XML on rows where the literal replaces an absent path.
                let xml = if path.is_none_or(str::is_empty) {
                    Some("")
                } else {
                    xml
                };
                (xml, path)
            }),
        ),
    ] {
        let projection = FUNCTIONS
            .map(|name| format!("{name}(xml, {argument})"))
            .join(", ");
        let batch = input(&fixture)?;
        session.register_batch("input", batch.clone())?;
        for ansi_mode in [false, true] {
            let actual = execute(&session, &projection, "input", ansi_mode).await?;
            assert_eq!(actual.num_rows(), fixture.len());
            observations.push(json!({
                "argument": argument, "ansi_mode": ansi_mode, "result": captured_values(&actual)?,
            }));
            for row in 0..fixture.len() {
                session.register_batch("single_row", batch.slice(row, 1))?;
                let reference = execute(&session, &projection, "single_row", ansi_mode).await?;
                for (column, function) in FUNCTIONS.iter().enumerate() {
                    assert_eq!(
                        actual.column(column).slice(row, 1).to_data(),
                        reference.column(column).to_data(),
                        "{function}, {argument}, ANSI={ansi_mode}, row={row}",
                    );
                }
                session.deregister_table("single_row")?;
            }
        }
        session.deregister_table("input")?;
    }
    assert_staging_observations("values", observations)?;
    Ok(())
}

#[tokio::test]
async fn xpath_batch_reuse_preserves_first_error() -> Result<()> {
    let session = session();
    let mut observations = Vec::new();
    let valid = (Some("<r><x>1</x></r>"), Some("/r/x"));
    let malformed_xml = (Some("<r>"), Some("["));
    let malformed_path = (Some("<r/>"), Some("["));
    for function in FUNCTIONS {
        let evaluation_failure = if function == "xpath" {
            (Some("<r/>"), Some("1"))
        } else {
            (Some("<r/>"), Some("(1, 2)"))
        };
        let evaluation_message = if function == "xpath" {
            "Cannot convert XPath result to a NodeList"
        } else {
            "Error evaluating XPath"
        };
        let projection = format!("{function}(xml, path)");
        for (bad_row, later_row, message) in [
            (malformed_xml, malformed_path, "Invalid XML document"),
            (malformed_path, malformed_xml, "Invalid XPath '['"),
            (evaluation_failure, malformed_xml, evaluation_message),
        ] {
            session.register_batch("input", input(&[valid, valid, bad_row, later_row])?)?;
            session.register_batch("single_row", input(&[bad_row])?)?;
            for ansi_mode in [false, true] {
                let actual = execute(&session, &projection, "input", ansi_mode)
                    .await
                    .expect_err(message)
                    .to_string();
                let reference = execute(&session, &projection, "single_row", ansi_mode)
                    .await
                    .expect_err(message)
                    .to_string();
                assert!(reference.contains(message), "{function}: {reference}");
                assert_eq!(actual, reference, "{function}, ANSI={ansi_mode}");
                observations.push(json!({
                    "function": function, "ansi_mode": ansi_mode, "fixture": message, "error": actual,
                }));
            }
            session.deregister_table("input")?;
            session.deregister_table("single_row")?;
        }
    }
    assert_staging_observations("errors", observations)?;
    Ok(())
}
