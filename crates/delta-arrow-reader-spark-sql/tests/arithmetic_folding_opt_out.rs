//! Shared preparation may fold permitted children without invoking opaque UDF ancestors.
//! These native extension-contract controls have no Apache Spark UDF oracle credit.

use std::hash::{Hash, Hasher};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::json;

#[expect(dead_code, reason = "Only collected-batch capture is needed here.")]
#[path = "support/string_binary.rs"]
mod string_binary;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
struct CountedIdentity {
    name: &'static str,
    signature: Signature,
    error_if_invoked: bool,
    clone_argument_field: bool,
    calls: Arc<AtomicUsize>,
}

impl PartialEq for CountedIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.signature == other.signature
            && self.error_if_invoked == other.error_if_invoked
            && self.clone_argument_field == other.clone_argument_field
    }
}
impl Eq for CountedIdentity {}
impl Hash for CountedIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.error_if_invoked.hash(state);
        self.clone_argument_field.hash(state);
    }
}

impl ScalarUDFImpl for CountedIdentity {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn supports_constant_folding(&self) -> bool {
        self.name == "allowed_constant"
    }
    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        Ok(arguments[0].clone())
    }
    fn return_field_from_args(&self, arguments: ReturnFieldArgs) -> Result<FieldRef> {
        if self.clone_argument_field {
            return Ok(Arc::clone(&arguments.arg_fields[0]));
        }
        // Preserve DataFusion's default nullable return Field for the original probes.
        let types = arguments
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            true,
        )))
    }
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.error_if_invoked {
            return exec_err!("folding_opt_out_invoked");
        }
        Ok(arguments.args[0].clone())
    }
}

fn session(spark: bool, error_if_invoked: bool) -> (SessionContext, [Arc<AtomicUsize>; 2]) {
    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            SessionConfig::new()
                .with_batch_size(32)
                .with_target_partitions(1),
        );
    let session = SessionContext::new_with_state(if spark {
        configure_spark_sql(builder).build()
    } else {
        builder.build()
    });
    let retained = Arc::new(AtomicUsize::new(0));
    let allowed = Arc::new(AtomicUsize::new(0));
    for (name, calls) in [
        ("retain_constant", Arc::clone(&retained)),
        ("index_callback", Arc::clone(&retained)),
        ("mixed_counted_producer", Arc::clone(&retained)),
        ("allowed_constant", Arc::clone(&allowed)),
    ] {
        let signature = if name == "index_callback" {
            Signature::any(1, Volatility::Immutable)
        } else if name == "mixed_counted_producer" {
            Signature::exact(vec![DataType::Int32], Volatility::Immutable)
        } else {
            Signature::exact(vec![DataType::Float64], Volatility::Immutable)
        };
        let function = ScalarUDF::from(CountedIdentity {
            name,
            signature,
            error_if_invoked: error_if_invoked && name != "allowed_constant",
            clone_argument_field: name == "mixed_counted_producer",
            calls,
        });
        let function = if name == "retain_constant" {
            function.with_aliases(["retained_alias"])
        } else {
            function
        };
        session.register_udf(function);
    }
    (session, [retained, allowed])
}

fn counts(counters: &[Arc<AtomicUsize>; 2]) -> [usize; 2] {
    counters
        .each_ref()
        .map(|counter| counter.load(Ordering::SeqCst))
}

struct FoldingCase {
    sql: String,
    analyzed_fields: Vec<Field>,
    physical_fields: Vec<Field>,
    rows: Vec<Vec<ScalarValue>>,
    expected_opt_out_calls: usize,
    permitted_planning_limit: usize,
    expected_execution_error: Option<&'static str>,
}

async fn check_case(
    case: FoldingCase,
    spark: bool,
    ansi_mode: bool,
    error_if_invoked: bool,
) -> TestResult {
    let (session, counters) = session(spark, error_if_invoked);
    let (analyzed, names, after_resolve, physical) = if spark {
        let query = plan_query(
            &session,
            &case.sql,
            &SparkSqlOptions {
                ansi_mode,
                ..Default::default()
            },
        )
        .await?;
        let after_resolve = counts(&counters);
        assert_eq!(after_resolve, [0, 0], "resolve: {}", case.sql);
        let analyzed = Arc::clone(query.logical_plan.schema().inner());
        let names = query.column_names.clone();
        let physical = query.create_physical_plan(&session).await?;
        (analyzed, names, after_resolve, physical)
    } else {
        let frame = session.sql(&case.sql).await?;
        let after_resolve = counts(&counters);
        assert_eq!(after_resolve, [0, 0], "native resolve: {}", case.sql);
        let analyzed = Arc::clone(frame.schema().inner());
        let names = analyzed
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let physical = frame.create_physical_plan().await?;
        (analyzed, names, after_resolve, physical)
    };
    let after_physical = counts(&counters);
    assert_eq!(after_physical[0], 0, "prepare: {}", case.sql);
    if case.permitted_planning_limit == 0 {
        assert_eq!(
            after_physical[1], 0,
            "unexpected permitted callback: {}",
            case.sql
        );
    } else {
        // This source-bound cost ceiling preserves existing work, not an exact-once UDF guarantee.
        assert!(
            (1..=case.permitted_planning_limit).contains(&after_physical[1]),
            "permitted planning calls increased: {}: {:?}",
            case.sql,
            after_physical
        );
    }
    assert_eq!(
        names,
        case.analyzed_fields
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(analyzed.fields().len(), names.len());
    // Public output names are separate from the resolver's internal Field names.
    // Clone every other actual Field attribute, including nested declarations and metadata.
    let public_fields = analyzed
        .fields()
        .iter()
        .zip(&names)
        .map(|(field, name)| field.as_ref().clone().with_name(name.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        Schema::new_with_metadata(public_fields, analyzed.metadata().clone()),
        Schema::new(case.analyzed_fields),
        "analyzed: {}",
        case.sql
    );
    let physical_schema = physical.schema();
    assert_eq!(
        physical_schema.as_ref(),
        &Schema::new(case.physical_fields),
        "physical: {}",
        case.sql
    );
    let result = collect(physical, session.task_ctx()).await;
    let after_execute = counts(&counters);
    assert_eq!(
        after_execute,
        [case.expected_opt_out_calls, after_physical[1]],
        "execute: {}",
        case.sql
    );
    let observed = match result {
        Err(error) if error_if_invoked && case.expected_opt_out_calls != 0 => {
            assert_eq!(
                error.find_root().to_string(),
                "Execution error: folding_opt_out_invoked"
            );
            json!({"error":error.to_string(), "root_error":error.find_root().to_string(), "error_stage":"execute"})
        }
        Err(error) => match case.expected_execution_error {
            Some(expected) => {
                assert_eq!(error.find_root().to_string(), expected);
                json!({"error":error.to_string(), "root_error":error.find_root().to_string(), "error_stage":"execute"})
            }
            None => return Err(error.into()),
        },
        Ok(batches) => {
            assert!(
                case.expected_execution_error.is_none(),
                "missing execution error: {}",
                case.sql
            );
            assert!(
                !error_if_invoked || case.expected_opt_out_calls == 0,
                "missing callback error: {}",
                case.sql
            );
            for batch in &batches {
                assert_eq!(batch.schema(), physical_schema);
            }
            let mut rows = Vec::new();
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| ScalarValue::try_from_array(column.as_ref(), row))
                            .collect::<Result<Vec<_>>>()?,
                    );
                }
            }
            assert_eq!(rows, case.rows, "{}", case.sql);
            string_binary::capture_batches(&batches)?
        }
    };
    eprintln!(
        "shared folding permission: {}",
        json!({
            "sql":case.sql,"spark":spark,"ansi":ansi_mode,"ansi_option_applied":spark,
            "error_if_invoked":error_if_invoked,"after_resolve":after_resolve,
            "after_physical":after_physical,"after_execute":after_execute,
            "count_order":["opt_out","permitted"],
            "permitted_planning_cost_ceiling":case.permitted_planning_limit,
            "analyzed_arrow_schema":format!("{analyzed:?}"),"column_names":names,
            "physical_arrow_schema":format!("{physical_schema:?}"),"result":observed,
            "spark_oracle_credit":false
        })
    );
    Ok(())
}

fn scalar_case(
    sql: String,
    data_type: DataType,
    rows: Vec<Vec<ScalarValue>>,
    expected_opt_out_calls: usize,
    permitted_planning_limit: usize,
) -> FoldingCase {
    let field = Field::new("r", data_type, true);
    FoldingCase {
        sql,
        analyzed_fields: vec![field.clone()],
        physical_fields: vec![field],
        rows,
        expected_opt_out_calls,
        permitted_planning_limit,
        expected_execution_error: None,
    }
}

#[tokio::test]
async fn arithmetic_descendants_aliases_and_parents_wait_for_execution() -> TestResult {
    for ansi in [true, false] {
        for spark in [true, false] {
            for error_if_invoked in [false, true] {
                for (expression, data_type, value, permitted_planning_limit) in [
                    (
                        "retain_constant(1/1)",
                        DataType::Float64,
                        ScalarValue::Float64(Some(1.0)),
                        0,
                    ),
                    (
                        "retain_constant(1+1)",
                        DataType::Float64,
                        ScalarValue::Float64(Some(2.0)),
                        0,
                    ),
                    (
                        "retained_alias(1/1)",
                        DataType::Float64,
                        ScalarValue::Float64(Some(1.0)),
                        0,
                    ),
                    (
                        "retain_constant(1/1)+1",
                        DataType::Float64,
                        ScalarValue::Float64(Some(2.0)),
                        0,
                    ),
                    (
                        "CAST(retain_constant(1/1) AS BIGINT)",
                        DataType::Int64,
                        ScalarValue::Int64(Some(1)),
                        0,
                    ),
                    (
                        "retain_constant(allowed_constant(1+1)/1)",
                        DataType::Float64,
                        ScalarValue::Float64(Some(2.0)),
                        if spark { 2 } else { 1 },
                    ),
                ] {
                    check_case(
                        scalar_case(
                            format!("SELECT {expression} AS r"),
                            data_type,
                            vec![vec![value]],
                            1,
                            permitted_planning_limit,
                        ),
                        spark,
                        ansi,
                        error_if_invoked,
                    )
                    .await?;
                }
                // Unconfigured native DataFusion does not register Spark's ARRAY function.
                if spark {
                    let field = Field::new(
                        "r",
                        DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
                        false,
                    );
                    check_case(
                        FoldingCase {
                            sql: "SELECT ARRAY(retain_constant(1/1)) AS r".into(),
                            analyzed_fields: vec![field.clone()],
                            physical_fields: vec![field],
                            rows: vec![vec![ScalarValue::List(ScalarValue::new_list(
                                &[ScalarValue::Float64(Some(1.0))],
                                &DataType::Float64,
                                true,
                            ))]],
                            expected_opt_out_calls: 1,
                            permitted_planning_limit: 0,
                            expected_execution_error: None,
                        },
                        spark,
                        ansi,
                        error_if_invoked,
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn scalar_parents_and_cast_producers_preserve_empty_and_live_opt_outs() -> TestResult {
    for ansi in [true, false] {
        for error_if_invoked in [false, true] {
            for (producer, data_type, value) in [
                (
                    "regexp_extract(s,p,CAST(index_callback(i) AS BIGINT))",
                    DataType::Utf8,
                    "a",
                ),
                ("CAST(index_callback(i) AS BIGINT)", DataType::Int64, "1"),
            ] {
                for empty in [false, true] {
                    let fields = vec![
                        Field::new("r", data_type.clone(), true),
                        Field::new("q", DataType::Int32, true),
                    ];
                    check_case(FoldingCase {
                        sql:format!("SELECT r,(SELECT 1) AS q FROM (SELECT {producer} AS r FROM VALUES('a','(a)','1') AS t(s,p,i)) AS u{}", if empty {" LIMIT 0"} else {""}),
                        analyzed_fields:fields.clone(), physical_fields:vec![
                            fields[0].clone(), Field::new("q", DataType::Int32, false)],
                        rows:if empty {vec![]} else {vec![vec![if data_type == DataType::Utf8 {ScalarValue::Utf8(Some(value.into()))} else {ScalarValue::Int64(Some(1))},ScalarValue::Int32(Some(1))]]},
                        expected_opt_out_calls:usize::from(!empty),permitted_planning_limit:0,
                        expected_execution_error:None,
                    }, true, ansi, error_if_invoked).await?;
                }
            }
            for spark in [true, false] {
                for empty in [false, true] {
                    check_case(scalar_case(format!("SELECT CAST(index_callback(i) AS BIGINT) AS r FROM VALUES('1') AS t(i){}",
                        if empty {" LIMIT 0"} else {""}), DataType::Int64,
                        if empty {vec![]} else {vec![vec![ScalarValue::Int64(Some(1))]]}, usize::from(!empty),0),
                        spark, ansi, error_if_invoked).await?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn conditional_null_and_empty_parents_do_not_invoke_discarded_calls() -> TestResult {
    for ansi in [true, false] {
        for spark in [true, false] {
            for (expression, suffix, analyzed_nullable_in_spark, nullable, rows, calls) in [
                (
                    "CASE WHEN false THEN retain_constant(1/1) ELSE CAST(7 AS DOUBLE) END",
                    "",
                    true,
                    false,
                    vec![vec![ScalarValue::Float64(Some(7.0))]],
                    0,
                ),
                (
                    "COALESCE(CAST(7 AS DOUBLE),retain_constant(1/1))",
                    "",
                    false,
                    false,
                    vec![vec![ScalarValue::Float64(Some(7.0))]],
                    0,
                ),
                (
                    "CAST(NULL AS DOUBLE)*retain_constant(1/1)",
                    "",
                    true,
                    true,
                    vec![vec![ScalarValue::Float64(None)]],
                    0,
                ),
                ("retain_constant(1/1)", " LIMIT 0", true, true, vec![], 0),
                (
                    "retain_constant(1/1)",
                    " WHERE false",
                    true,
                    true,
                    vec![],
                    0,
                ),
                (
                    "retain_constant(CAST(NULL AS DOUBLE))",
                    "",
                    true,
                    true,
                    vec![vec![ScalarValue::Float64(None)]],
                    1,
                ),
            ] {
                let mut case = scalar_case(
                    format!("SELECT {expression} AS r{suffix}"),
                    DataType::Float64,
                    rows,
                    calls,
                    0,
                );
                case.analyzed_fields[0] = case.analyzed_fields[0].clone().with_nullable(if spark {
                    analyzed_nullable_in_spark
                } else {
                    nullable
                });
                case.physical_fields[0] = case.physical_fields[0].clone().with_nullable(nullable);
                check_case(case, spark, ansi, calls == 0).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn required_children_and_siblings_keep_the_first_error_without_invoking_opt_outs()
-> TestResult {
    let fields = [
        Field::new("deferred", DataType::Float64, true),
        Field::new("required", DataType::Int64, true),
    ];
    for (sql, expected_fields, expected_root) in [
        (
            "SELECT retain_constant(1 DIV 0) AS r",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT (1 DIV 0)+ARRAY(mixed_counted_producer(7))[0] AS r",
            vec![Field::new("r", DataType::Int64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT retain_constant(CAST('bad' AS INT)) AS r",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
        ),
        (
            "SELECT retain_constant(1/1) AS deferred, 1 DIV 0 AS required",
            fields.to_vec(),
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT 1 DIV 0 AS required, retain_constant(1/1) AS deferred",
            fields.into_iter().rev().collect(),
            "Arrow error: Divide by zero error",
        ),
    ] {
        // If an opaque call runs, its sentinel must not replace the required division failure.
        let (session, counters) = session(true, true);
        let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
        let after_resolve = counts(&counters);
        assert_eq!(after_resolve, [0, 0], "{sql}");
        let analyzed = query.logical_plan.schema();
        let public_fields = analyzed
            .fields()
            .iter()
            .zip(&query.column_names)
            .map(|(field, name)| field.as_ref().clone().with_name(name.clone()))
            .collect::<Vec<_>>();
        assert_eq!(analyzed.fields().len(), expected_fields.len());
        assert_eq!(
            query.column_names,
            expected_fields
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            Schema::new_with_metadata(public_fields, analyzed.metadata().clone()),
            Schema::new(expected_fields)
        );
        let error = match query.create_physical_plan(&session).await {
            Err(error) => error,
            Ok(_) => return Err(format!("required child failure was deferred: {sql}").into()),
        };
        let after_physical = counts(&counters);
        assert_eq!(after_physical, [0, 0], "{sql}: {error}");
        assert_eq!(error.find_root().to_string(), expected_root);
        if expected_root.contains("Cast error") {
            assert!(
                error.to_string().contains("[CAST_INVALID_INPUT]"),
                "{error}"
            );
        }
        eprintln!(
            "shared folding required error: {}",
            json!({
                "sql":sql,"ansi":true,"spark_oracle_credit":false,
                "after_resolve":after_resolve,"after_physical":after_physical,
                "analyzed_arrow_schema":format!("{:?}", analyzed.as_arrow()),
                "column_names":query.column_names,"error_stage":"physical",
                "error":error.to_string(),"root_error":error.find_root().to_string(),
                "physical_fields_available":false,"rows_available":false
            })
        );
    }
    for error_if_invoked in [false, true] {
        for sql in [
            "SELECT retain_constant(1 DIV 0) AS r",
            "SELECT retain_constant(CAST('bad' AS INT)) AS r",
        ] {
            check_case(
                scalar_case(
                    sql.into(),
                    DataType::Float64,
                    vec![vec![ScalarValue::Float64(None)]],
                    1,
                    0,
                ),
                true,
                false,
                error_if_invoked,
            )
            .await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn array_index_parent_inference_does_not_invoke_opaque_int_producers() -> TestResult {
    for ansi in [true, false] {
        for error_if_invoked in [false, true] {
            for (expression, data_type, value, physical_nullable) in [
                (
                    "(1 DIV 1)+ARRAY(mixed_counted_producer(7))[0]",
                    DataType::Int64,
                    ScalarValue::Int64(Some(8)),
                    true,
                ),
                (
                    "ARRAY(mixed_counted_producer(7))[0]+(1 DIV 1)",
                    DataType::Int64,
                    ScalarValue::Int64(Some(8)),
                    true,
                ),
                (
                    "1+ARRAY(mixed_counted_producer(7))[0]",
                    DataType::Int32,
                    ScalarValue::Int32(Some(8)),
                    true,
                ),
                (
                    "ARRAY(mixed_counted_producer(7))[0]+1",
                    DataType::Int32,
                    ScalarValue::Int32(Some(8)),
                    true,
                ),
                (
                    "(1 DIV 1)+mixed_counted_producer(7)",
                    DataType::Int64,
                    ScalarValue::Int64(Some(8)),
                    false,
                ),
                (
                    "mixed_counted_producer(7)+(1 DIV 1)",
                    DataType::Int64,
                    ScalarValue::Int64(Some(8)),
                    false,
                ),
            ] {
                let mut case = scalar_case(
                    format!("SELECT {expression} AS r"),
                    data_type,
                    vec![vec![value]],
                    1,
                    0,
                );
                // An opaque index cannot prove the generated error branch unreachable during inference.
                case.physical_fields[0] = case.physical_fields[0]
                    .clone()
                    .with_nullable(physical_nullable);
                check_case(case, true, ansi, error_if_invoked).await?;
            }
            for spark in [true, false] {
                let mut standalone = scalar_case(
                    "SELECT mixed_counted_producer(CAST(7 AS INT)) AS r".into(),
                    DataType::Int32,
                    vec![vec![ScalarValue::Int32(Some(7))]],
                    1,
                    0,
                );
                standalone.analyzed_fields[0] =
                    standalone.analyzed_fields[0].clone().with_nullable(false);
                standalone.physical_fields[0] =
                    standalone.physical_fields[0].clone().with_nullable(false);
                check_case(standalone, spark, ansi, error_if_invoked).await?;
                check_case(scalar_case(
                    "SELECT CASE WHEN mixed_counted_producer(CAST(7 AS INT))=7 THEN CAST(NULL AS INT) ELSE CAST(8 AS INT) END AS r".into(),
                    DataType::Int32, vec![vec![ScalarValue::Int32(None)]], 1, 0),
                    spark, ansi, error_if_invoked).await?;
            }
            let mut reversed = scalar_case(
                "SELECT ARRAY(mixed_counted_producer(7))[0]+(1 DIV 0) AS r".into(),
                DataType::Int64,
                vec![vec![ScalarValue::Int64(None)]],
                usize::from(ansi),
                0,
            );
            if ansi {
                // The live left callback precedes the lazy right DIV error; its sentinel wins if it fails.
                reversed.expected_execution_error = Some("Arrow error: Divide by zero error");
            }
            check_case(reversed, true, ansi, error_if_invoked).await?;
            if !ansi {
                check_case(
                    scalar_case(
                        "SELECT (1 DIV 0)+ARRAY(mixed_counted_producer(7))[0] AS r".into(),
                        DataType::Int64,
                        vec![vec![ScalarValue::Int64(None)]],
                        0,
                        0,
                    ),
                    true,
                    ansi,
                    error_if_invoked,
                )
                .await?;
            }
        }
    }
    Ok(())
}
