//! LIKE's discarded local evaluator honors a host UDF's planning opt-out.

use std::collections::HashSet;
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::{Array, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::json;

static CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, PartialEq, Eq, Hash)]
struct NoFold;

impl ScalarUDFImpl for NoFold {
    fn name(&self) -> &str {
        "like_no_fold"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Utf8], Volatility::Immutable));
        &SIGNATURE
    }

    fn supports_constant_folding(&self) -> bool {
        false
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(args.args[0].clone())
    }
}

#[tokio::test]
async fn like_local_checks_preserve_host_folding_permissions() -> Result<()> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_udf(ScalarUDF::from(NoFold));
    let host_options = session.state().config_options().entries();
    let mut contexts = HashSet::new();
    for (scenario, template, calls, rows) in [
        (
            "standalone_control",
            "SELECT like_no_fold('abc') AS r FROM (VALUES (1)) v(k)",
            1,
            1,
        ),
        (
            "direct_source",
            "SELECT like_no_fold('abc') {op} '%' AS r FROM (VALUES (1)) v(k)",
            1,
            1,
        ),
        (
            "direct_pattern",
            "SELECT 'abc' {op} like_no_fold('%') AS r FROM (VALUES (1)) v(k)",
            1,
            1,
        ),
        (
            "opaque_child_projection",
            "SELECT s {op} '%' AS r FROM (SELECT like_no_fold('abc') AS s FROM (VALUES (1)) v(k)) q",
            1,
            1,
        ),
        (
            "false_filter",
            "SELECT like_no_fold('abc') {op} '%' AS r FROM (VALUES (1)) v(k) WHERE false",
            0,
            0,
        ),
        (
            "limit_zero",
            "SELECT like_no_fold('abc') {op} '%' AS r FROM (VALUES (1)) v(k) LIMIT 0",
            0,
            0,
        ),
        (
            "opaque_child_false_filter",
            "SELECT s {op} '%' AS r FROM (SELECT like_no_fold('abc') AS s FROM (VALUES (1)) v(k)) q WHERE false",
            0,
            0,
        ),
        (
            "dead_case",
            "SELECT CASE WHEN false THEN like_no_fold('abc') {op} '%' ELSE true END AS r FROM (VALUES (1)) v(k)",
            0,
            1,
        ),
    ] {
        let operators: &[&str] = if scenario == "standalone_control" {
            &[""]
        } else {
            &["LIKE", "ILIKE"]
        };
        for operator in operators {
            let sql = template.replace("{op}", operator);
            for ansi in [false, true] {
                for spark in [true, false] {
                    assert!(contexts.insert((sql.clone(), ansi, spark)));
                    CALLS.store(0, Ordering::SeqCst);
                    let data_type = if scenario == "standalone_control" {
                        DataType::Utf8
                    } else {
                        DataType::Boolean
                    };
                    let (analyzed, physical, resolved_calls) = if spark {
                        let query = plan_query(
                            &session,
                            &sql,
                            &SparkSqlOptions {
                                ansi_mode: ansi,
                                ..Default::default()
                            },
                        )
                        .await?;
                        let resolved_calls = CALLS.load(Ordering::SeqCst);
                        assert_eq!(resolved_calls, 0, "resolve: {sql}");
                        assert_eq!(query.column_names, ["r"]);
                        // Use the actual public query names with every real Arrow
                        // field attribute, as the shared observation helper does.
                        let schema = Arc::new(Schema::new_with_metadata(
                            query
                                .logical_plan
                                .schema()
                                .fields()
                                .iter()
                                .zip(&query.column_names)
                                .map(|(field, name)| field.as_ref().clone().with_name(name.clone()))
                                .collect::<Vec<_>>(),
                            query.logical_plan.schema().inner().metadata().clone(),
                        ));
                        (
                            schema,
                            query.create_physical_plan(&session).await?,
                            resolved_calls,
                        )
                    } else {
                        // The ANSI flag applies only to the Spark resolver, just
                        // as in the independently recorded 60-context baseline.
                        let frame = session.sql(&sql).await?;
                        let resolved_calls = CALLS.load(Ordering::SeqCst);
                        assert_eq!(resolved_calls, 0, "native resolve: {sql}");
                        let schema = Arc::clone(frame.schema().inner());
                        (schema, frame.create_physical_plan().await?, resolved_calls)
                    };
                    let planned_calls = CALLS.load(Ordering::SeqCst);
                    assert_eq!(
                        planned_calls, 0,
                        "planning: {sql}, Spark={spark}, ANSI={ansi}"
                    );
                    assert_eq!(
                        analyzed,
                        Arc::new(Schema::new(vec![Field::new(
                            "r",
                            data_type.clone(),
                            scenario != "dead_case" || spark,
                        )]))
                    );
                    let expected_physical = Arc::new(Schema::new(vec![Field::new(
                        "r",
                        data_type,
                        scenario != "dead_case",
                    )]));
                    let physical_schema = physical.schema();
                    assert_eq!(physical_schema, expected_physical);
                    let batches = collect(physical, session.task_ctx()).await?;
                    let final_calls = CALLS.load(Ordering::SeqCst);
                    assert_eq!(
                        final_calls, calls,
                        "execution: {sql}, Spark={spark}, ANSI={ansi}"
                    );
                    let actual_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
                    assert_eq!(actual_rows, rows);
                    for batch in &batches {
                        assert_eq!(batch.schema(), expected_physical);
                        assert_eq!(batch.column(0).null_count(), 0);
                        for row in 0..batch.num_rows() {
                            if scenario == "standalone_control" {
                                let values = batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                assert_eq!(values.value(row), "abc");
                            } else {
                                let values = batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<BooleanArray>()
                                    .unwrap();
                                assert!(values.value(row));
                            }
                        }
                    }
                    assert_eq!(session.state().config_options().entries(), host_options);
                    assert!(
                        session
                            .state()
                            .config_options()
                            .extensions
                            .get::<SparkSqlExpressionSemantics>()
                            .is_none()
                    );
                    eprintln!(
                        "c06 like folding regression: {}",
                        json!({
                            "scenario":scenario,"operator":operator,"ansi":ansi,"spark":spark,"sql":sql,
                            "resolved_calls":resolved_calls,"planned_calls":planned_calls,"final_calls":final_calls,
                            "analyzed_arrow_schema":format!("{analyzed:?}"),
                            "physical_arrow_schema":format!("{physical_schema:?}"),
                            "rows":actual_rows,"apache_spark_oracle_credit":false,
                        })
                    );
                }
            }
        }
    }
    assert_eq!(contexts.len(), 60);
    Ok(())
}
