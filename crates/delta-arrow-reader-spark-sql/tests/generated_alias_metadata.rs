//! Spark output aliases preserve exact public and executed metadata stages.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[expect(
    dead_code,
    reason = "This replay uses the shared encoders with resolved schemas."
)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_schema, encode_value, spark_schema_type, validate_batch};

fn records() -> TestResult<Vec<Value>> {
    Ok(
        include_str!("fixtures/generated-alias-metadata-spark-v2.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()?,
    )
}

fn has_engine_condition(result: &TestResult, condition: &str) -> bool {
    result
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<datafusion::error::DataFusionError>())
        .is_some_and(|error| error.to_string().contains(condition))
}

async fn compare_case(session: &SessionContext, case: &Value) -> TestResult {
    compare_case_with_encoder(session, case, encode_value).await
}

async fn compare_case_with_encoder(
    session: &SessionContext,
    case: &Value,
    encode: fn(&dyn arrow::array::Array, usize) -> TestResult<Value>,
) -> TestResult {
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
    let fields = query
        .logical_plan
        .schema()
        .fields()
        .iter()
        .zip(&query.column_names)
        .map(|(field, name)| {
            Ok(
                json!({"name":name,"type":spark_schema_type(field.data_type())?,
                "nullable":field.is_nullable(),"metadata":field.metadata()}),
            )
        })
        .collect::<TestResult<Vec<_>>>()?;
    let public_schema = json!({"type":"struct","fields":fields});
    let mut differences = Vec::new();
    if !case["public_schema"].is_null() && public_schema != case["public_schema"] {
        differences.push(format!(
            "resolved public schema: expected {}, got {public_schema}",
            case["public_schema"]
        ));
    }
    if case["expected"].get("condition").is_some() && !differences.is_empty() {
        return Err(differences.join("\n").into());
    }
    let physical = query.create_physical_plan(session).await?;
    if physical.schema().fields().len() != fields.len() {
        return Err("physical field count changed".into());
    }
    for (index, field) in physical.schema().fields().iter().enumerate() {
        let resolved = query.logical_plan.schema().field(index);
        // Optimizers may prove that a nullable resolved expression cannot be NULL.
        if field.name() != &query.column_names[index]
            || field.data_type() != resolved.data_type()
            || field.metadata() != resolved.metadata()
            || (field.is_nullable() && !resolved.is_nullable())
        {
            differences.push(format!(
                "physical field {field:?} differs from resolved {resolved:?}"
            ));
        }
    }
    if let Some(expected) = case.get("executed_schema_after_collect") {
        let fields = physical
            .schema()
            .fields()
            .iter()
            .map(|field| {
                Ok(
                    json!({"name":field.name(), "type":spark_schema_type(field.data_type())?,
                "nullable":field.is_nullable(), "metadata":field.metadata()}),
                )
            })
            .collect::<TestResult<Vec<_>>>()?;
        let schema = json!({"type":"struct", "fields":fields});
        if schema != *expected {
            differences.push(format!(
                "executed schema: expected {expected}, got {schema}"
            ));
        }
    }
    // A required engine failure cannot waive an earlier schema mismatch.
    if case["expected"].get("condition").is_some() && !differences.is_empty() {
        return Err(differences.join("\n").into());
    }
    let schema = physical.schema();
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        validate_batch(schema.as_ref(), &batch)?;
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    if case["expected"].get("condition").is_some() {
        return Err("query unexpectedly succeeded".into());
    }
    rows.sort_by_key(|row| json!(row).to_string());
    if json!(rows) != case["expected"]["rows"] {
        differences.push("complete result rows differ".into());
    }
    if differences.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{}; expected {}, got {}",
            differences.join("\n"),
            case["expected"],
            json!({"schema":public_schema,"rows":rows})
        )
        .into())
    }
}

#[tokio::test]
async fn generated_alias_metadata_matches_spark() -> TestResult {
    let records = records()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 240);
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let result = compare_case(&session, case).await;
            let difference = match case["expected"]["condition"].as_str() {
                Some(condition) => (!has_engine_condition(&result, condition))
                    .then(|| format!("expected {condition}, got {result:?}")),
                None => result.err().map(|error| error.to_string()),
            };
            if let Some(difference) = difference {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {difference}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn alias_classification_and_computed_grouping_match_spark() -> TestResult {
    let records = include_str!("fixtures/generated-alias-metadata-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 28);
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            if let Err(error) = compare_case(&session, case).await {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {error}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn configured_native_sql_keeps_metadata_unchanged() -> TestResult {
    let records = records()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 2)?;
    }
    for sql in [
        "SELECT k,CAST(k AS BIGINT) AS cast_value,ABS(k) AS absolute FROM alias_mixed",
        "SELECT SUM(k),MAX(k),COUNT(*) FROM alias_mixed",
        "SELECT nested.item FROM alias_mixed",
    ] {
        let mut results = Vec::new();
        for session in [&native, &configured] {
            let frame = session.sql(sql).await?;
            let schema = frame.schema().clone();
            let task_ctx = std::sync::Arc::new(frame.task_ctx());
            let physical = frame.create_physical_plan().await?;
            let physical_schema = physical.schema();
            let batches = collect(physical, task_ctx).await?;
            for batch in &batches {
                validate_batch(physical_schema.as_ref(), batch)?;
            }
            results.push((schema, batches));
        }
        assert_eq!(results[0], results[1], "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn generated_aliases_preserve_arrow_extension_fields() -> TestResult {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let extension = HashMap::from([
        ("ARROW:extension:name".into(), "arrow.json".into()),
        ("ARROW:extension:metadata".into(), "".into()),
    ]);
    let mut source = extension.clone();
    source.insert("source".into(), "provider".into());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("j", DataType::Utf8, true).with_metadata(source.clone()),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec![Some("{\"n\":1}"), None])),
        ],
    )?;
    session.register_table(
        "json_values",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    let mut generated = extension.clone();
    generated.insert("__autoGeneratedAlias".into(), "true".into());
    let expected = [
        &source, &extension, &generated, &source, &extension, &generated,
    ];
    for ansi_mode in [true, false] {
        let query = plan_query(
            &session,
            "SELECT j,CAST(j AS STRING),CAST(CAST(j AS STRING) AS STRING),\
             j AS explicit,CAST(j AS STRING) AS cast_explicit,\
             LAG(j) OVER (ORDER BY id) FROM json_values",
            &SparkSqlOptions {
                ansi_mode,
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        let physical = query.create_physical_plan(&session).await?;
        for (index, metadata) in expected.iter().enumerate() {
            let logical = query.logical_plan.schema().field(index);
            let field = physical.schema().field(index).clone();
            assert_eq!(logical.metadata(), *metadata);
            assert_eq!(field.metadata(), *metadata);
            assert_eq!(field.data_type(), &DataType::Utf8);
            assert!(field.is_nullable());
        }
        let schema = physical.schema();
        let mut rows = Vec::new();
        for batch in collect(physical, session.task_ctx()).await? {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                );
            }
        }
        rows.sort_by_key(|row| json!(row).to_string());
        let mut expected_rows = vec![
            json!([
                "{\"n\":1}",
                "{\"n\":1}",
                "{\"n\":1}",
                "{\"n\":1}",
                "{\"n\":1}",
                null
            ]),
            json!([null, null, null, null, null, "{\"n\":1}"]),
        ];
        expected_rows.sort_by_key(Value::to_string);
        assert_eq!(json!(rows), json!(expected_rows));
    }
    Ok(())
}

#[tokio::test]
async fn generated_metadata_preserves_null_and_error_semantics() -> TestResult {
    let records = include_str!("fixtures/review-710-metadata-semantics-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 48);
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
            let result = compare_case(&session, case).await;
            let difference = match case["expected"]["condition"].as_str() {
                Some(condition) => (!has_engine_condition(&result, condition))
                    .then(|| format!("expected {condition}, got {result:?}")),
                None => result.err().map(|error| error.to_string()),
            };
            if let Some(difference) = difference {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {difference}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn identifier_extraction_retains_source_metadata() -> TestResult {
    let records = include_str!("fixtures/review-710-identifier-extraction-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 16);
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            if let Err(error) = compare_case(&session, case).await {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {error}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn metadata_annotations_evaluate_each_volatile_value_once() -> TestResult {
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let records = records()?;
    for ansi_mode in [true, false] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
        );
        register_recorded_inputs(&session, &records[0], 1)?;
        let evaluated_rows = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&evaluated_rows);
        session.register_udf(create_udf(
            "alias_metadata_counter",
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Volatile,
            Arc::new(move |arguments| {
                let rows = match &arguments[0] {
                    ColumnarValue::Array(array) => array.len(),
                    ColumnarValue::Scalar(_) => 1,
                };
                observed.fetch_add(rows, Ordering::SeqCst);
                Ok(arguments[0].clone())
            }),
        ));
        let query = plan_query(&session,
            "SELECT alias_metadata_counter(k),alias_metadata_counter(k) AS explicit,CAST(alias_metadata_counter(k) AS BIGINT) FROM alias_mixed",
            &SparkSqlOptions { ansi_mode, ..SparkSqlOptions::default() }).await?;
        assert_eq!(evaluated_rows.load(Ordering::SeqCst), 0);
        let physical = query.create_physical_plan(&session).await?;
        assert_eq!(evaluated_rows.load(Ordering::SeqCst), 0);
        for (index, generated) in [true, false, true].into_iter().enumerate() {
            let expected = if generated {
                json!({"__autoGeneratedAlias":"true"})
            } else {
                json!({})
            };
            assert_eq!(
                json!(query.logical_plan.schema().field(index).metadata()),
                expected
            );
            assert_eq!(json!(physical.schema().field(index).metadata()), expected);
        }
        let schema = physical.schema();
        let mut rows = Vec::new();
        for batch in collect(physical, session.task_ctx()).await? {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                );
            }
        }
        assert_eq!(evaluated_rows.load(Ordering::SeqCst), 12);
        rows.sort_by_key(|row| json!(row).to_string());
        let mut expected = vec![
            vec![json!("2"); 3],
            vec![json!("-3"); 3],
            vec![json!("4"); 3],
            vec![Value::Null; 3],
        ];
        expected.sort_by_key(|row| json!(row).to_string());
        assert_eq!(rows, expected);
    }
    Ok(())
}

#[tokio::test]
async fn native_null_udf_rewrites_keep_cast_errors_and_hook_order() -> TestResult {
    use std::sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{Result as DataFusionResult, ScalarValue};
    use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion::logical_expr::{
        ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
        Signature, Volatility,
    };

    static SIMPLIFY_CALLS: AtomicUsize = AtomicUsize::new(0);
    static INVOKE_ROWS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct NullRewrite;

    impl ScalarUDFImpl for NullRewrite {
        fn name(&self) -> &str {
            "native_null_rewrite"
        }

        fn signature(&self) -> &Signature {
            static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
                Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable)
            });
            &SIGNATURE
        }

        fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> {
            Ok(DataType::Int32)
        }

        fn return_field_from_args(&self, _: ReturnFieldArgs) -> DataFusionResult<FieldRef> {
            Ok(Arc::new(Field::new(self.name(), DataType::Int32, true)))
        }

        fn simplify(
            &self,
            _: Vec<Expr>,
            _: &SimplifyContext,
        ) -> DataFusionResult<ExprSimplifyResult> {
            SIMPLIFY_CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::Int32(None),
                None,
            )))
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
            INVOKE_ROWS.fetch_add(args.number_rows, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)))
        }
    }

    async fn execute(
        session: &SessionContext,
        expression: &str,
    ) -> TestResult<(DataType, Vec<Value>)> {
        let frame = session
            .sql(&format!(
                "SELECT {expression} AS value FROM native_null_inputs"
            ))
            .await?;
        assert!(frame.schema().field(0).is_nullable());
        let data_type = frame.schema().field(0).data_type().clone();
        let task_ctx = Arc::new(frame.task_ctx());
        let physical = frame.create_physical_plan().await?;
        let schema = physical.schema();
        let mut rows = Vec::new();
        for batch in collect(physical, task_ctx).await? {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(encode_value(batch.column(0).as_ref(), row)?);
            }
        }
        Ok((data_type, rows))
    }

    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        session.register_udf(ScalarUDF::from(NullRewrite));
        session.register_batch(
            "native_null_inputs",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)])),
                vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
            )?,
        )?;
        for expression in [
            "CAST('bad' AS INT) + native_null_rewrite(k)",
            "native_null_rewrite(k) + CAST('bad' AS INT)",
        ] {
            SIMPLIFY_CALLS.store(0, Ordering::SeqCst);
            INVOKE_ROWS.store(0, Ordering::SeqCst);
            let error = execute(session, expression).await.expect_err(expression);
            assert!(
                error
                    .to_string()
                    .contains("Cannot cast string 'bad' to value of Int32 type"),
                "{expression}: {error}"
            );
            assert_eq!(SIMPLIFY_CALLS.load(Ordering::SeqCst), 0, "{expression}");
            assert_eq!(INVOKE_ROWS.load(Ordering::SeqCst), 0, "{expression}");
        }
        for (expression, data_type, simplify_calls) in [
            ("native_null_rewrite(k)", DataType::Int32, 1),
            ("native_null_rewrite(k) + 1", DataType::Int64, 1),
            (
                "CAST(NULL AS INT) + native_null_rewrite(k)",
                DataType::Int32,
                0,
            ),
            (
                "native_null_rewrite(k) + CAST(NULL AS INT)",
                DataType::Int32,
                0,
            ),
        ] {
            SIMPLIFY_CALLS.store(0, Ordering::SeqCst);
            INVOKE_ROWS.store(0, Ordering::SeqCst);
            let result = execute(session, expression).await?;
            assert_eq!(result, (data_type, vec![Value::Null; 2]), "{expression}");
            assert_eq!(
                SIMPLIFY_CALLS.load(Ordering::SeqCst),
                simplify_calls,
                "{expression}"
            );
            assert_eq!(INVOKE_ROWS.load(Ordering::SeqCst), 0, "{expression}");
        }
    }
    Ok(())
}

async fn replay_annotation_boundary_capture(
    raw: &str,
    kind: Option<&str>,
    count: usize,
) -> TestResult {
    let records = raw
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let cases = records[1..]
        .iter()
        .filter(|case| kind.is_none_or(|kind| case["kind"] == kind))
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), count);
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &cases {
            if let Err(error) = compare_case(&session, case).await {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {error}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn literal_grouping_aliases_match_spark_fields_and_rows() -> TestResult {
    replay_annotation_boundary_capture(
        include_str!("fixtures/review-710-literal-grouping-spark.jsonl"),
        Some("literal_grouping"),
        6,
    )
    .await
}

#[tokio::test]
async fn conditionals_preserve_public_and_executed_schema_stages() -> TestResult {
    replay_annotation_boundary_capture(
        include_str!("fixtures/review-710-conditional-schema-spark.jsonl"),
        None,
        8,
    )
    .await
}

#[tokio::test]
async fn nullable_column_casts_preserve_spark_conditional_declarations() -> TestResult {
    replay_annotation_boundary_capture(
        include_str!("fixtures/review-710-cast-visibility-spark.jsonl"),
        None,
        8,
    )
    .await
}

#[tokio::test]
async fn volatile_order_values_are_materialized_once_per_expression() -> TestResult {
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::sorts::{
        sort::SortExec, sort_preserving_merge::SortPreservingMergeExec,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn check_sort_keys(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
        let sort = plan.downcast_ref::<SortExec>();
        let merge = plan.downcast_ref::<SortPreservingMergeExec>();
        if let Some(keys) = sort
            .map(SortExec::expr)
            .or_else(|| merge.map(SortPreservingMergeExec::expr))
        {
            for key in keys.iter() {
                assert!(key.expr.is::<Column>(), "computed sort/merge key: {key:?}");
            }
        }
        let mut counts = (usize::from(sort.is_some()), usize::from(merge.is_some()));
        for child in plan.children() {
            let child = check_sort_keys(child);
            counts.0 += child.0;
            counts.1 += child.1;
        }
        counts
    }

    fn register_counter(session: &SessionContext) -> Arc<AtomicUsize> {
        let rows = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&rows);
        session.register_udf(create_udf(
            "review_annotation_counter",
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Volatile,
            Arc::new(move |arguments| {
                let count = match &arguments[0] {
                    ColumnarValue::Array(array) => array.len(),
                    ColumnarValue::Scalar(_) => 1,
                };
                counted.fetch_add(count, Ordering::SeqCst);
                Ok(arguments[0].clone())
            }),
        ));
        rows
    }

    let records = include_str!("fixtures/review-710-literal-grouping-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let cases = records[1..]
        .iter()
        .filter(|case| case["kind"] == "order_by_counter")
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 4);
    for batch_size in [1, 32] {
        for case in &cases {
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
            register_recorded_inputs(&session, &records[0], batch_size)?;
            let invoked = register_counter(&session);
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
            assert_eq!(query.column_names.len(), 1);
            let field = query.logical_plan.schema().field(0);
            let public_schema = json!({"type":"struct", "fields":[{
                "name":query.column_names[0], "type":spark_schema_type(field.data_type())?,
                "nullable":field.is_nullable(), "metadata":field.metadata(),
            }]});
            assert_eq!(public_schema, case["public_schema"]);
            let physical = query.create_physical_plan(&session).await?;
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
            assert_eq!(physical.schema().fields().len(), 1);
            let physical_field = physical.schema().field(0).clone();
            assert_eq!(physical_field.name(), &query.column_names[0]);
            assert_eq!(physical_field.data_type(), field.data_type());
            assert_eq!(physical_field.is_nullable(), field.is_nullable());
            assert_eq!(physical_field.metadata(), field.metadata());
            let counts = check_sort_keys(&physical);
            assert!(
                counts.0 > 0 && counts.1 > 0,
                "expected partition sort and merge"
            );
            let schema = physical.schema();
            let mut rows = Vec::new();
            for batch in collect(physical, session.task_ctx()).await? {
                validate_batch(schema.as_ref(), &batch)?;
                for row in 0..batch.num_rows() {
                    rows.push(vec![encode_value(batch.column(0).as_ref(), row)?]);
                }
            }
            assert_eq!(json!(rows), case["expected"]["rows"]);
            assert_eq!(case["count_before_planning"], 0);
            assert_eq!(case["count_after_planning"], 0);
            assert_eq!(case["count_before_collect"], 0);
            assert_eq!(case["collect_actions"], 1);
            assert_eq!(
                invoked.load(Ordering::SeqCst) as u64,
                case["count_after_collect"]
                    .as_u64()
                    .ok_or("missing Spark counter")?
            );
        }
        for sql in [
            "SELECT review_annotation_counter(k) AS r FROM annotation_counter_inputs ORDER BY r",
            "SELECT review_annotation_counter(k) FROM annotation_counter_inputs ORDER BY review_annotation_counter(k)",
        ] {
            let session = SessionContext::new();
            register_recorded_inputs(&session, &records[0], batch_size)?;
            let invoked = register_counter(&session);
            let frame = session.sql(sql).await?;
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
            let physical = frame.create_physical_plan().await?;
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
            assert!(check_sort_keys(&physical).0 > 0);
            assert_eq!(physical.schema().fields().len(), 1);
            assert_eq!(physical.schema().field(0).data_type(), &DataType::Int32);
            assert!(physical.schema().field(0).is_nullable());
            assert!(physical.schema().field(0).metadata().is_empty());
            let schema = physical.schema();
            let mut rows = Vec::new();
            for batch in collect(physical, session.task_ctx()).await? {
                validate_batch(schema.as_ref(), &batch)?;
                for row in 0..batch.num_rows() {
                    rows.push(vec![encode_value(batch.column(0).as_ref(), row)?]);
                }
            }
            assert_eq!(json!(rows), json!([["1"], [null]]));
            assert_eq!(invoked.load(Ordering::SeqCst), 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn historical_local_errors_precede_outer_null_pruning() -> TestResult {
    let records = include_str!("fixtures/generated-alias-metadata-historical-errors-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 24);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
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
            let result = compare_case(&session, case).await;
            // Check the required failure condition through the existing Arrow
            // diagnostic representation; public Spark error identifiers remain #149.
            let matches = match (case["expected"]["condition"].as_str(), &result) {
                (None, Ok(())) => true,
                (Some(condition), Err(error))
                    if error.is::<datafusion::error::DataFusionError>() =>
                {
                    let message = error.to_string();
                    match condition {
                        "CAST_INVALID_INPUT" => {
                            message.contains("[CAST_INVALID_INPUT]")
                                || message.contains("Cannot cast string 'bad' to value of Int")
                        }
                        "CAST_OVERFLOW" => {
                            message.contains("[CAST_OVERFLOW]")
                                || message.contains("Can't cast value NaN to type Int")
                        }
                        "ARITHMETIC_OVERFLOW" => message.contains("[ARITHMETIC_OVERFLOW]"),
                        "DIVIDE_BY_ZERO" => {
                            message.contains("[DIVIDE_BY_ZERO]")
                                || message.contains("Divide by zero error")
                        }
                        _ => false,
                    }
                }
                _ => false,
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {result:?}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn reviewed_if_declarations_match_exact_spark_stages() -> TestResult {
    fn encode_text(array: &dyn arrow::array::Array, row: usize) -> TestResult<Value> {
        if matches!(array.data_type(), arrow::datatypes::DataType::Null)
            || array
                .logical_nulls()
                .is_some_and(|nulls| nulls.is_null(row))
        {
            Ok(Value::Null)
        } else {
            Ok(arrow::util::display::array_value_to_string(array, row)?.into())
        }
    }
    let records = include_str!("fixtures/generated-alias-metadata-accepted-if-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 16);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
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
            assert_eq!(case["public_schema"], case["analyzed_schema"]);
            assert_eq!(
                case["optimized_schema"],
                case["executed_schema_before_collect"]
            );
            assert_eq!(
                case["executed_schema_before_collect"],
                case["executed_schema_after_collect"]
            );
            // This immutable capture uses decimal text for floating rows.
            if let Err(error) = compare_case_with_encoder(&session, case, encode_text).await {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {error}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn replay_alias_schema_stages(raw: &str, count: usize) -> TestResult {
    let records = raw
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, count);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case.get("input").and_then(Value::as_str),
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        assert!(case.get("error").is_none());
        assert_eq!(case["public_schema"], case["analyzed_schema"]);
        assert_eq!(case["public_schema"], case["expected"]["schema"]);
        assert_eq!(
            case["optimized_schema"],
            case["executed_schema_before_collect"]
        );
        assert_eq!(
            case["executed_schema_before_collect"],
            case["executed_schema_after_collect"]
        );
    }
    let mut differences = Vec::new();
    let mut comparisons = 0;
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    session_timezone: "UTC".into(),
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let public = capture_schema(
                query.logical_plan.schema().fields(),
                Some(&query.column_names),
            )?;
            let physical = query.create_physical_plan(&session).await?;
            let schema = physical.schema();
            let executed = capture_schema(schema.fields(), None)?;
            let mut rows = Vec::new();
            for batch in collect(physical, session.task_ctx()).await? {
                validate_batch(schema.as_ref(), &batch)?;
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| encode_value(column.as_ref(), row))
                            .collect::<TestResult<Vec<_>>>()?,
                    );
                }
            }
            rows.sort_by_key(|row| json!(row).to_string());
            comparisons += 1;
            eprintln!(
                "alias_schema_stages: {}",
                json!({"id":case["id"],"input":case["input"],"ansi":case["ansi"],
                    "batch_size":batch_size,"matches":public == case["public_schema"]
                        && executed == case["executed_schema_after_collect"]
                        && json!(rows) == case["expected"]["rows"],
                    "expected":case,"actual":{"public_schema":public,
                        "executed_schema":executed,"rows":rows}})
            );
            for (stage, actual, expected) in [
                ("public schema", public, &case["public_schema"]),
                (
                    "executed schema",
                    executed,
                    &case["executed_schema_after_collect"],
                ),
                ("complete rows", json!(rows), &case["expected"]["rows"]),
            ] {
                if actual != *expected {
                    differences.push(format!(
                        "{} input={} ANSI={} batch={batch_size} {stage}: expected {expected}, got {actual}",
                        case["id"], case["input"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert_eq!(comparisons, count * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn positive_aliases_match_public_and_executed_spark_schemas() -> TestResult {
    replay_alias_schema_stages(
        include_str!("fixtures/review-710-positive-alias-spark.jsonl"),
        24,
    )
    .await
}

#[tokio::test]
async fn nested_positive_aliases_match_public_and_executed_spark_schemas() -> TestResult {
    replay_alias_schema_stages(
        include_str!("fixtures/review-710-nested-positive-alias-spark.jsonl"),
        12,
    )
    .await
}

#[tokio::test]
async fn extracted_positive_aliases_match_public_and_executed_spark_schemas() -> TestResult {
    replay_alias_schema_stages(
        include_str!("fixtures/review-710-nested-positive-alias-forms-spark.jsonl"),
        24,
    )
    .await
}

#[tokio::test]
async fn successful_queries_cannot_supply_their_expected_error() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in ["SELECT 1", "SELECT 'CAST_INVALID_INPUT' AS marker"] {
        // Fault injection changes only the expectation; execution still succeeds.
        let case = json!({"sql":sql, "ansi":true,
            "expected":{"condition":"CAST_INVALID_INPUT"}});
        let result = compare_case(&session, &case).await;
        assert!(result.is_err(), "the required error was not raised: {sql}");
        assert!(!has_engine_condition(&result, "CAST_INVALID_INPUT"));
        assert!(
            !result
                .unwrap_err()
                .to_string()
                .contains("CAST_INVALID_INPUT")
        );
    }
    let schema_mismatch = json!({"sql":"SELECT 1", "ansi":true,
        "public_schema":{"type":"struct", "fields":[{"name":"CAST_INVALID_INPUT",
            "type":"integer", "nullable":false, "metadata":{}}]},
        "expected":{"condition":"CAST_INVALID_INPUT"}});
    let result = compare_case(&session, &schema_mismatch).await;
    assert!(result.is_err());
    assert!(!has_engine_condition(&result, "CAST_INVALID_INPUT"));
    let required_cast = json!({"sql":"SELECT CAST('bad' AS INT) AS value", "ansi":true,
        "expected":{"condition":"CAST_INVALID_INPUT"}});
    let result = compare_case(&session, &required_cast).await;
    assert!(has_engine_condition(
        &result,
        "Cannot cast string 'bad' to value of Int32 type"
    ));
    Ok(())
}

#[tokio::test]
async fn collection_errors_cannot_hide_physical_schema_mismatches() -> TestResult {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_batch(
        "fault_cast_inputs",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["bad", "1"]))],
        )?,
    )?;
    let sql = "SELECT CAST(s AS INT) AS r FROM fault_cast_inputs";
    let condition = "Cannot cast string 'bad' to value of Int32 type";
    // A provider-backed column keeps this failure out of constant folding.
    let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
    let physical = query.create_physical_plan(&session).await?;
    let error = collect(physical, session.task_ctx())
        .await
        .expect_err("collection must fail");
    assert!(error.to_string().contains(condition));
    let mut case = json!({"sql":sql,"ansi":true,"expected":{"condition":condition}});
    let control = compare_case(&session, &case).await;
    assert!(has_engine_condition(&control, condition));

    // This invented schema is fault injection, not an independent Spark record.
    case["executed_schema_after_collect"] = json!({"type":"struct","fields":[{
        "name":"fault_injected_name", "type":"integer", "nullable":true,"metadata":{}
    }]});
    let result = compare_case(&session, &case).await;
    assert!(result.is_err());
    assert!(
        !has_engine_condition(&result, condition),
        "physical schema mismatch was hidden by collection error: {result:?}"
    );
    assert!(result.unwrap_err().to_string().contains("executed schema"));
    Ok(())
}

#[tokio::test]
async fn correlated_scalar_aliases_match_public_and_executed_spark_schemas() -> TestResult {
    let raw = include_str!("fixtures/correlated-scalar-metadata-stages/scope-spark.jsonl");
    let seed = include_str!("fixtures/correlated-scalar-metadata-stages/original-scope-spark.jsonl");
    assert!(
        raw.starts_with(seed),
        "original 24 observations were rewritten"
    );
    let records = raw
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 36);
    assert_eq!(seed.lines().count() - 1, 24);
    assert!(
        records[0]["spark_build"]
            .as_str()
            .ok_or("missing Spark build")?
            .contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    );
    for case in &records[1..] {
        assert!(
            [
                "correlated_direct_outer_alias",
                "struct_direct_child_alias",
                "struct_positive_child_alias",
                "struct_unary_positive_child_alias",
                "correlated_struct_inner_alias",
                "correlated_direct_inner_alias",
            ]
            .contains(&case["id"].as_str().ok_or("missing ID")?)
        );
        assert!(
            ["struct_mixed", "struct_nulls", "struct_empty"]
                .contains(&case["input"].as_str().ok_or("missing input")?)
        );
    }
    replay_alias_schema_stages(raw, 36).await
}
