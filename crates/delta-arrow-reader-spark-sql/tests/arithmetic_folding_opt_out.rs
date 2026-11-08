//! Shared preparation may fold permitted children without invoking opaque UDF ancestors.
//! These native extension-contract controls have no Apache Spark UDF oracle credit.

use std::hash::{Hash, Hasher};
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::expr::HigherOrderFunction;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, ValueOrLambda, Volatility,
};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::json;

#[expect(dead_code, reason = "Only collected-batch capture is needed here.")]
#[path = "support/string_binary.rs"]
mod string_binary;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default)]
struct InvocationCounters {
    opaque: Arc<AtomicUsize>,
    permitted: Arc<AtomicUsize>,
}

#[derive(Debug, PartialEq, Eq)]
struct InvocationSnapshot {
    opaque: usize,
    permitted: usize,
}

impl InvocationCounters {
    fn snapshot(&self) -> InvocationSnapshot {
        InvocationSnapshot {
            opaque: self.opaque.load(Ordering::SeqCst),
            permitted: self.permitted.load(Ordering::SeqCst),
        }
    }
}

#[derive(Debug)]
struct CountedIdentity {
    name: &'static str,
    signature: Signature,
    folding_permitted: bool,
    short_circuits: bool,
    any_null: bool,
    error_if_invoked: bool,
    clone_argument_field: bool,
    rewrite_target: Option<Arc<ScalarUDF>>,
    calls: Arc<AtomicUsize>,
}

impl PartialEq for CountedIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.signature == other.signature
            && self.folding_permitted == other.folding_permitted
            && self.short_circuits == other.short_circuits
            && self.any_null == other.any_null
            && self.error_if_invoked == other.error_if_invoked
            && self.clone_argument_field == other.clone_argument_field
            && self.rewrite_target == other.rewrite_target
    }
}
impl Eq for CountedIdentity {}
impl Hash for CountedIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.folding_permitted.hash(state);
        self.short_circuits.hash(state);
        self.any_null.hash(state);
        self.error_if_invoked.hash(state);
        self.clone_argument_field.hash(state);
        self.rewrite_target.hash(state);
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
        self.folding_permitted
    }
    fn short_circuits(&self) -> bool {
        self.short_circuits
    }
    fn short_circuits_on_any_null_argument(&self) -> bool {
        self.any_null
    }
    fn conditional_arguments<'a>(&self, _: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        // The SDK defines None as all eager arguments, even when short_circuits is true.
        None
    }
    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        Ok(arguments[0].clone())
    }
    fn return_field_from_args(&self, arguments: ReturnFieldArgs) -> Result<FieldRef> {
        // A rewrite preserves the complete target Field, including its name.
        if let Some(target) = &self.rewrite_target {
            return target.return_field_from_args(arguments);
        }
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
    fn simplify(&self, arguments: Vec<Expr>, _: &SimplifyContext) -> Result<ExprSimplifyResult> {
        Ok(match &self.rewrite_target {
            Some(target) => ExprSimplifyResult::Simplified(target.call(arguments)),
            None => ExprSimplifyResult::Original(arguments),
        })
    }
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.error_if_invoked {
            return exec_err!("folding_opt_out_invoked");
        }
        Ok(arguments.args[0].clone())
    }
}

// Value-only higher-order identities exercise NoFold and signature-Volatile target permissions.
#[derive(Debug)]
struct HigherOrderIdentity {
    volatile: bool,
    error_if_invoked: bool,
    calls: Arc<AtomicUsize>,
}

impl PartialEq for HigherOrderIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.volatile == other.volatile && self.error_if_invoked == other.error_if_invoked
    }
}
impl Eq for HigherOrderIdentity {}
impl Hash for HigherOrderIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.volatile.hash(state);
        self.error_if_invoked.hash(state);
    }
}

impl HigherOrderUDFImpl for HigherOrderIdentity {
    fn name(&self) -> &str {
        if self.volatile {
            "higher_volatile_identity"
        } else {
            "higher_no_fold_identity"
        }
    }
    fn signature(&self) -> &HigherOrderSignature {
        static IMMUTABLE: LazyLock<HigherOrderSignature> =
            LazyLock::new(|| HigherOrderSignature::any(1, Volatility::Immutable));
        static VOLATILE: LazyLock<HigherOrderSignature> =
            LazyLock::new(|| HigherOrderSignature::any(1, Volatility::Volatile));
        if self.volatile { &VOLATILE } else { &IMMUTABLE }
    }
    fn lambda_parameters(
        &self,
        _: usize,
        _: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        Ok(LambdaParametersProgress::Complete(vec![]))
    }
    fn supports_constant_folding(&self) -> bool {
        // Exercise the signature veto independently from the NoFold flag.
        self.volatile
    }
    fn return_field_from_args(&self, arguments: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        match arguments.arg_fields {
            [ValueOrLambda::Value(field)] => Ok(Arc::clone(field)),
            _ => exec_err!("higher identity requires one value argument"),
        }
    }
    fn invoke_with_args(&self, arguments: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.error_if_invoked {
            return exec_err!("folding_opt_out_invoked");
        }
        match arguments.args.as_slice() {
            [ValueOrLambda::Value(value)] => Ok(value.clone()),
            _ => exec_err!("higher identity requires one value argument"),
        }
    }
}

#[derive(Debug)]
struct ScalarToHigherOrderRewriter {
    name: &'static str,
    target: Arc<HigherOrderUDF>,
    error_if_invoked: bool,
    calls: Arc<AtomicUsize>,
}

impl PartialEq for ScalarToHigherOrderRewriter {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.target == other.target
            && self.error_if_invoked == other.error_if_invoked
    }
}
impl Eq for ScalarToHigherOrderRewriter {}
impl Hash for ScalarToHigherOrderRewriter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.target.hash(state);
        self.error_if_invoked.hash(state);
    }
}

impl ScalarUDFImpl for ScalarToHigherOrderRewriter {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Float64], Volatility::Immutable));
        &SIGNATURE
    }
    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        Ok(arguments[0].clone())
    }
    fn return_field_from_args(&self, arguments: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::clone(&arguments.arg_fields[0]))
    }
    fn short_circuits(&self) -> bool {
        true
    }
    fn conditional_arguments<'a>(&self, _: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        None
    }
    fn simplify(&self, arguments: Vec<Expr>, _: &SimplifyContext) -> Result<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Simplified(Expr::HigherOrderFunction(
            HigherOrderFunction::new(Arc::clone(&self.target), arguments),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum NullPolicy {
    NullIfFirstArgumentNull,
    FirstNonNullArgument,
}

#[derive(Debug)]
struct NullSelectionScalarFunction {
    policy: NullPolicy,
    argument_count: usize,
    error_if_invoked: bool,
    calls: Arc<AtomicUsize>,
}
impl PartialEq for NullSelectionScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.policy == other.policy
            && self.argument_count == other.argument_count
            && self.error_if_invoked == other.error_if_invoked
    }
}
impl Eq for NullSelectionScalarFunction {}
impl Hash for NullSelectionScalarFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.policy.hash(state);
        self.argument_count.hash(state);
        self.error_if_invoked.hash(state);
    }
}
impl ScalarUDFImpl for NullSelectionScalarFunction {
    fn name(&self) -> &str {
        match (self.policy, self.argument_count) {
            (NullPolicy::NullIfFirstArgumentNull, 2) => "opaque_null_first",
            (NullPolicy::NullIfFirstArgumentNull, 3) => "opaque_null_first_three_arguments",
            (NullPolicy::FirstNonNullArgument, 2) => "opaque_first_non_null",
            (NullPolicy::FirstNonNullArgument, 3) => "opaque_first_non_null_three_arguments",
            _ => unreachable!("fixture uses only two or three arguments"),
        }
    }
    fn signature(&self) -> &Signature {
        static TWO: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Float64; 2], Volatility::Immutable));
        static THREE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Float64; 3], Volatility::Immutable));
        match self.argument_count {
            2 => &TWO,
            3 => &THREE,
            _ => unreachable!("fixture uses only two or three arguments"),
        }
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn supports_constant_folding(&self) -> bool {
        false
    }
    fn short_circuits(&self) -> bool {
        true
    }
    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        Some((vec![&args[0]], args[1..].iter().collect()))
    }
    fn short_circuits_on_null_first_arg(&self) -> bool {
        self.policy == NullPolicy::NullIfFirstArgumentNull
    }
    fn returns_first_non_null_argument(&self) -> bool {
        self.policy == NullPolicy::FirstNonNullArgument
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.error_if_invoked {
            return exec_err!("folding_opt_out_invoked");
        }
        // Native first-non-NULL selection bypasses the owner kernel.
        Ok(args.args[0].clone())
    }
}

// Multiplication is NULL-propagating for both arguments, including skipped argument values.
#[derive(Debug)]
struct NullPropagatingProduct {
    calls: Arc<AtomicUsize>,
}
impl PartialEq for NullPropagatingProduct {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for NullPropagatingProduct {}
impl Hash for NullPropagatingProduct {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "null_propagating_product".hash(state);
    }
}
impl ScalarUDFImpl for NullPropagatingProduct {
    fn name(&self) -> &str {
        "null_propagating_product"
    }
    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Float64; 2], Volatility::Immutable));
        &SIGNATURE
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn supports_constant_folding(&self) -> bool {
        false
    }
    fn short_circuits(&self) -> bool {
        true
    }
    fn short_circuits_on_any_null_argument(&self) -> bool {
        true
    }
    fn conditional_arguments<'a>(&self, _: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        None
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        Ok(ColumnarValue::Array(arrow::compute::kernels::numeric::mul(
            &arrays[0].as_ref(),
            &arrays[1].as_ref(),
        )?))
    }
}

fn session(spark: bool, error_if_invoked: bool) -> (SessionContext, InvocationCounters) {
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
    let counters = InvocationCounters::default();
    // The named counters track runtime-only targets and the permitted scalar rewriter.
    let higher_order_target = Arc::new(HigherOrderUDF::new_from_impl(HigherOrderIdentity {
        volatile: false,
        error_if_invoked,
        calls: Arc::clone(&counters.opaque),
    }));
    session.register_udf(ScalarUDF::from(ScalarToHigherOrderRewriter {
        name: "rewrite_to_higher_no_fold",
        target: higher_order_target,
        error_if_invoked,
        calls: Arc::clone(&counters.permitted),
    }));
    session.register_udf(ScalarUDF::from(ScalarToHigherOrderRewriter {
        name: "rewrite_to_volatile_higher",
        target: Arc::new(HigherOrderUDF::new_from_impl(HigherOrderIdentity {
            volatile: true,
            error_if_invoked,
            calls: Arc::clone(&counters.opaque),
        })),
        error_if_invoked,
        calls: Arc::clone(&counters.permitted),
    }));
    for (policy, argument_count) in [
        (NullPolicy::NullIfFirstArgumentNull, 2),
        (NullPolicy::NullIfFirstArgumentNull, 3),
        (NullPolicy::FirstNonNullArgument, 2),
        (NullPolicy::FirstNonNullArgument, 3),
    ] {
        session.register_udf(ScalarUDF::from(NullSelectionScalarFunction {
            policy,
            argument_count,
            error_if_invoked,
            calls: Arc::clone(&counters.opaque),
        }));
    }
    session.register_udf(ScalarUDF::from(NullPropagatingProduct {
        calls: Arc::clone(&counters.opaque),
    }));
    let retained_identity = Arc::new(
        ScalarUDF::from(CountedIdentity {
            name: "retain_constant",
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
            folding_permitted: false,
            short_circuits: false,
            any_null: false,
            error_if_invoked,
            clone_argument_field: false,
            rewrite_target: None,
            calls: Arc::clone(&counters.opaque),
        })
        .with_aliases(["retained_alias"]),
    );
    session.register_udf(retained_identity.as_ref().clone());
    for probe in [
        CountedIdentity {
            name: "rewrite_to_retained",
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
            folding_permitted: true,
            short_circuits: true,
            any_null: false,
            error_if_invoked,
            clone_argument_field: false,
            rewrite_target: Some(retained_identity),
            calls: Arc::clone(&counters.permitted),
        },
        CountedIdentity {
            name: "index_callback",
            signature: Signature::any(1, Volatility::Immutable),
            folding_permitted: false,
            short_circuits: false,
            any_null: false,
            error_if_invoked,
            clone_argument_field: false,
            rewrite_target: None,
            calls: Arc::clone(&counters.opaque),
        },
        CountedIdentity {
            name: "mixed_counted_producer",
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
            folding_permitted: false,
            short_circuits: false,
            any_null: false,
            error_if_invoked,
            clone_argument_field: true,
            rewrite_target: None,
            calls: Arc::clone(&counters.opaque),
        },
        CountedIdentity {
            name: "sdk_none_eager_short",
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
            folding_permitted: false,
            short_circuits: true,
            any_null: false,
            error_if_invoked,
            clone_argument_field: false,
            rewrite_target: None,
            calls: Arc::clone(&counters.opaque),
        },
        CountedIdentity {
            name: "any_null_identity",
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
            folding_permitted: false,
            short_circuits: false,
            any_null: true,
            error_if_invoked,
            clone_argument_field: false,
            rewrite_target: None,
            calls: Arc::clone(&counters.opaque),
        },
        CountedIdentity {
            name: "allowed_constant",
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
            folding_permitted: true,
            short_circuits: false,
            any_null: false,
            error_if_invoked: false,
            clone_argument_field: false,
            rewrite_target: None,
            calls: Arc::clone(&counters.permitted),
        },
    ] {
        session.register_udf(ScalarUDF::from(probe));
    }
    (session, counters)
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
        let after_resolve = counters.snapshot();
        assert_eq!(
            after_resolve,
            InvocationSnapshot {
                opaque: 0,
                permitted: 0
            },
            "resolve: {}",
            case.sql
        );
        let analyzed = Arc::clone(query.logical_plan.schema().inner());
        let names = query.column_names.clone();
        let physical = query.create_physical_plan(&session).await?;
        (analyzed, names, after_resolve, physical)
    } else {
        let frame = session.sql(&case.sql).await?;
        let after_resolve = counters.snapshot();
        assert_eq!(
            after_resolve,
            InvocationSnapshot {
                opaque: 0,
                permitted: 0
            },
            "native resolve: {}",
            case.sql
        );
        let analyzed = Arc::clone(frame.schema().inner());
        let names = analyzed
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let physical = frame.create_physical_plan().await?;
        (analyzed, names, after_resolve, physical)
    };
    let after_physical = counters.snapshot();
    assert_eq!(after_physical.opaque, 0, "prepare: {}", case.sql);
    if case.permitted_planning_limit == 0 {
        assert_eq!(
            after_physical.permitted, 0,
            "unexpected permitted callback: {}",
            case.sql
        );
    } else {
        // This source-bound cost ceiling preserves existing work, not an exact-once UDF guarantee.
        assert!(
            (1..=case.permitted_planning_limit).contains(&after_physical.permitted),
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
    let after_execute = counters.snapshot();
    assert_eq!(
        after_execute,
        InvocationSnapshot {
            opaque: case.expected_opt_out_calls,
            permitted: after_physical.permitted,
        },
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
            "error_if_invoked":error_if_invoked,"after_resolve":[after_resolve.opaque,after_resolve.permitted],
            "after_physical":[after_physical.opaque,after_physical.permitted],"after_execute":[after_execute.opaque,after_execute.permitted],
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
            "SELECT null_propagating_product(a,1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),0) AS t(a,d)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT opaque_null_first(a,1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),0) AS t(a,d)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT opaque_first_non_null(a,1/d) AS r FROM VALUES(CAST(7 AS DOUBLE),0),(CAST(NULL AS DOUBLE),0) AS t(a,d)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT opaque_null_first(a,index_callback(1/d)) AS r FROM VALUES(CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),0) AS t(a,d)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT opaque_first_non_null(a,index_callback(1/d)) AS r FROM VALUES(CAST(7 AS DOUBLE),0),(CAST(NULL AS DOUBLE),0) AS t(a,d)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT opaque_null_first_three_arguments(a,index_callback(v),1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),CAST(NULL AS DOUBLE),0) AS t(a,v,d)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT opaque_null_first(CAST(7 AS DOUBLE),1/0) AS r FROM (SELECT COUNT(*) AS c FROM VALUES(1) AS t(k)) AS q",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT opaque_first_non_null(CAST(NULL AS DOUBLE),1/0) AS r FROM (SELECT COUNT(*) AS c FROM VALUES(1) AS t(k)) AS q",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT SLICE(a, 1 DIV d, CAST(index_callback(n) AS BIGINT)) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>),0,1),(ARRAY(7),0,1) AS t(a,d,n)",
            vec![Field::new(
                "r",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            )],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT SLICE(a, 1 DIV d, CAST(index_callback(1 DIV n) AS BIGINT)) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>),0,0),(ARRAY(7),1,0) AS t(a,d,n)",
            vec![Field::new(
                "r",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            )],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN rewrite_to_higher_no_fold(v/0)>0 THEN CAST(7 AS DOUBLE) ELSE CAST(8 AS DOUBLE) END AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)",
            vec![Field::new("r", DataType::Float64, false)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN (1 DIV d)=1 THEN CAST(7 AS DOUBLE) WHEN retain_constant(v)>0 THEN CAST(8 AS DOUBLE) ELSE CAST(9 AS DOUBLE) END AS r FROM VALUES(0,CAST(1 AS DOUBLE)) AS t(d,v)",
            vec![Field::new("r", DataType::Float64, false)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN first_flag THEN retain_constant(1 DIV d) WHEN retain_constant(v)>0 THEN CAST(8 AS DOUBLE) ELSE CAST(9 AS DOUBLE) END AS r FROM VALUES(true,0,CAST(1 AS DOUBLE)),(false,1,CAST(1 AS DOUBLE)) AS t(first_flag,d,v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN retain_constant(v/0)>0 THEN CAST(7 AS DOUBLE) ELSE CAST(8 AS DOUBLE) END AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)",
            vec![Field::new("r", DataType::Float64, false)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN (1 DIV 0)=1 THEN retain_constant(1/1) ELSE CAST(7 AS DOUBLE) END AS r",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN v/0=CAST(1 AS DOUBLE) THEN retain_constant(1/1) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN true THEN retain_constant(1 DIV 0) ELSE CAST(7 AS DOUBLE) END AS r",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN false THEN CAST(7 AS DOUBLE) ELSE retain_constant(1 DIV 0) END AS r",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN flag THEN retain_constant(v/0) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(true,CAST(1 AS DOUBLE)) AS t(flag,v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CASE WHEN flag THEN retain_constant(1/v) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(false,CAST(0 AS DOUBLE)),(true,CAST(0 AS DOUBLE)) AS t(flag,v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT retain_constant(1 DIV 0) AS r",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT sdk_none_eager_short(1 DIV 0) AS r",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT rewrite_to_retained(v/0) AS r FROM VALUES(1.0) AS t(v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT rewrite_to_retained(CAST(v AS INT)/1) AS r FROM VALUES('bad') AS t(v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
        ),
        (
            "SELECT retain_constant(v/0) AS r FROM VALUES(1.0) AS t(v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT retain_constant(CAST(v AS INT)/1) AS r FROM VALUES('bad') AS t(v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
        ),
        (
            "SELECT v/0 AS r FROM VALUES(1.0) AS t(v)",
            vec![Field::new("r", DataType::Decimal128(7, 6), true)],
            "Arrow error: Divide by zero error",
        ),
        (
            "SELECT CAST(v AS INT)/1 AS r FROM VALUES('bad') AS t(v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
        ),
        (
            // The full local batch must be checked, including a failure after a valid first row.
            "SELECT retain_constant(CAST(v AS INT)/1) AS r FROM VALUES('2'),('bad') AS t(v)",
            vec![Field::new("r", DataType::Float64, true)],
            "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
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
        for error_if_invoked in [false, true] {
            // If an opaque call runs, its sentinel must not replace the required division failure.
            let (session, counters) = session(true, error_if_invoked);
            let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
            let after_resolve = counters.snapshot();
            assert_eq!(
                after_resolve,
                InvocationSnapshot {
                    opaque: 0,
                    permitted: 0
                },
                "{sql}"
            );
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
                Schema::new(expected_fields.clone())
            );
            let error = match query.create_physical_plan(&session).await {
                Err(error) => error,
                Ok(_) => return Err(format!("required child failure was deferred: {sql}").into()),
            };
            let after_physical = counters.snapshot();
            assert_eq!(
                after_physical,
                InvocationSnapshot {
                    opaque: 0,
                    permitted: 0
                },
                "{sql}: {error}"
            );
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
                    "sql":sql,"ansi":true,"error_if_invoked":error_if_invoked,"spark_oracle_credit":false,
                    "after_resolve":[after_resolve.opaque,after_resolve.permitted],"after_physical":[after_physical.opaque,after_physical.permitted],
                    "analyzed_arrow_schema":format!("{:?}", analyzed.as_arrow()),
                    "column_names":query.column_names,"error_stage":"physical",
                    "error":error.to_string(),"root_error":error.find_root().to_string(),
                    "physical_fields_available":false,"rows_available":false
                })
            );
        }
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
            // ANSI checked-add keeps the RHS ARRAY in CASE's live predicate and result.
            // Success invokes both copies; a callback error stops at the first copy.
            for (expression, data_type, value, physical_nullable, ansi_success_calls) in [
                (
                    "(1 DIV 1)+ARRAY(mixed_counted_producer(7))[0]",
                    DataType::Int64,
                    ScalarValue::Int64(Some(8)),
                    true,
                    2,
                ),
                (
                    "ARRAY(mixed_counted_producer(7))[0]+(1 DIV 1)",
                    DataType::Int64,
                    ScalarValue::Int64(Some(8)),
                    true,
                    1,
                ),
                (
                    "1+ARRAY(mixed_counted_producer(7))[0]",
                    DataType::Int32,
                    ScalarValue::Int32(Some(8)),
                    true,
                    2,
                ),
                (
                    "ARRAY(mixed_counted_producer(7))[0]+1",
                    DataType::Int32,
                    ScalarValue::Int32(Some(8)),
                    true,
                    1,
                ),
                (
                    "(1 DIV 1)+mixed_counted_producer(7)",
                    DataType::Int64,
                    ScalarValue::Int64(Some(8)),
                    false,
                    1,
                ),
                (
                    "mixed_counted_producer(7)+(1 DIV 1)",
                    DataType::Int64,
                    ScalarValue::Int64(Some(8)),
                    false,
                    1,
                ),
            ] {
                let expected_calls = if ansi && !error_if_invoked {
                    ansi_success_calls
                } else {
                    1
                };
                let mut case = scalar_case(
                    format!("SELECT {expression} AS r"),
                    data_type,
                    vec![vec![value]],
                    expected_calls,
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

#[tokio::test]
async fn sdk_none_argument_partition_preserves_eager_children_and_opaque_calls() -> TestResult {
    for ansi in [true, false] {
        for error_if_invoked in [false, true] {
            for function in ["sdk_none_eager_short", "retain_constant"] {
                check_case(
                    scalar_case(
                        format!("SELECT {function}(1 DIV 1) AS r"),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(Some(1.0))]],
                        1,
                        0,
                    ),
                    true,
                    ansi,
                    error_if_invoked,
                )
                .await?;
                if !ansi {
                    check_case(
                        scalar_case(
                            format!("SELECT {function}(1 DIV 0) AS r"),
                            DataType::Float64,
                            vec![vec![ScalarValue::Float64(None)]],
                            1,
                            0,
                        ),
                        true,
                        ansi,
                        error_if_invoked,
                    )
                    .await?;
                }
            }
            for (sql, rows, expected_calls, physical_nullable) in [
                (
                    "SELECT sdk_none_eager_short(CAST(NULL AS DOUBLE)) AS r",
                    vec![vec![ScalarValue::Float64(None)]],
                    1,
                    true,
                ),
                (
                    "SELECT CASE WHEN false THEN sdk_none_eager_short(1 DIV 0) ELSE CAST(7 AS DOUBLE) END AS r",
                    vec![vec![ScalarValue::Float64(Some(7.0))]],
                    0,
                    false,
                ),
                (
                    "SELECT sdk_none_eager_short(1 DIV 1) AS r LIMIT 0",
                    vec![],
                    0,
                    true,
                ),
            ] {
                let mut case = scalar_case(sql.into(), DataType::Float64, rows, expected_calls, 0);
                case.physical_fields[0] = case.physical_fields[0]
                    .clone()
                    .with_nullable(physical_nullable);
                check_case(case, true, ansi, error_if_invoked).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn local_rows_preserve_opaque_execution_and_legacy_child_nulls() -> TestResult {
    for ansi in [true, false] {
        for error_if_invoked in [false, true] {
            for (sql, expected_value) in [
                (
                    "SELECT retain_constant(v/1) AS r FROM VALUES(1.0) AS t(v)",
                    1.0,
                ),
                (
                    "SELECT retain_constant(CAST(v AS INT)/1) AS r FROM VALUES('2') AS t(v)",
                    2.0,
                ),
            ] {
                for spark in [true, false] {
                    check_case(
                        scalar_case(
                            sql.into(),
                            DataType::Float64,
                            vec![vec![ScalarValue::Float64(Some(expected_value))]],
                            1,
                            0,
                        ),
                        spark,
                        ansi,
                        error_if_invoked,
                    )
                    .await?;
                }
            }
            if !ansi {
                for sql in [
                    "SELECT retain_constant(v/0) AS r FROM VALUES(1.0) AS t(v)",
                    "SELECT retain_constant(CAST(v AS INT)/1) AS r FROM VALUES('bad') AS t(v)",
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
async fn post_rewrite_permissions_preserve_local_and_closed_identity_controls() -> TestResult {
    for ansi in [true, false] {
        for spark in [true, false] {
            for error_if_invoked in [false, true] {
                // A closed permitted call can fold before its rewrite; a failing call is retained.
                let mut closed_rewrite = scalar_case(
                    "SELECT rewrite_to_retained(1/1) AS r".into(),
                    DataType::Float64,
                    vec![vec![ScalarValue::Float64(Some(1.0))]],
                    usize::from(error_if_invoked),
                    // Source-bound permitted preview ceilings, not a UDF exact-once guarantee.
                    if error_if_invoked {
                        if spark { 3 } else { 2 }
                    } else {
                        1
                    },
                );
                closed_rewrite.physical_fields[0] = closed_rewrite.physical_fields[0]
                    .clone()
                    .with_nullable(error_if_invoked);
                for case in [
                    scalar_case(
                        "SELECT rewrite_to_retained(v/1) AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)".into(),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(Some(1.0))]],
                        1,
                        0,
                    ),
                    scalar_case(
                        "SELECT retain_constant(v/1) AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)".into(),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(Some(1.0))]],
                        1,
                        0,
                    ),
                    closed_rewrite,
                    scalar_case(
                        "SELECT retain_constant(1/1) AS r".into(),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(Some(1.0))]],
                        1,
                        0,
                    ),
                ] {
                    check_case(case, spark, ansi, error_if_invoked).await?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn post_rewrite_local_consumers_keep_opaque_targets_for_execution() -> TestResult {
    for ansi in [true, false] {
        for error_if_invoked in [false, true] {
            let result_field = Field::new("r", DataType::Float64, true);
            check_case(
                FoldingCase {
                    sql: "SELECT r,(SELECT 1) AS q FROM (SELECT rewrite_to_retained(v/1) AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)) AS u".into(),
                    analyzed_fields: vec![result_field.clone(), Field::new("q", DataType::Int32, true)],
                    physical_fields: vec![result_field, Field::new("q", DataType::Int32, false)],
                    rows: vec![vec![ScalarValue::Float64(Some(1.0)), ScalarValue::Int32(Some(1))]],
                    expected_opt_out_calls: 1,
                    permitted_planning_limit: 0,
                    expected_execution_error: None,
                },
                true,
                ansi,
                error_if_invoked,
            ).await?;
            check_case(
                scalar_case(
                    "SELECT CAST(NULL AS DOUBLE) AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v) WHERE rewrite_to_retained(v/1)=CAST(1 AS DOUBLE)".into(),
                    DataType::Float64,
                    vec![vec![ScalarValue::Float64(None)]],
                    1,
                    0,
                ),
                true,
                ansi,
                error_if_invoked,
            ).await?;
            for spark in [true, false] {
                let mut cast_null_predicate = scalar_case(
                    "SELECT CAST(rewrite_to_retained(v/1) AS BIGINT) IS NULL AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)".into(),
                    DataType::Boolean,
                    vec![vec![ScalarValue::Boolean(Some(false))]],
                    1,
                    0,
                );
                cast_null_predicate.analyzed_fields[0] = cast_null_predicate.analyzed_fields[0]
                    .clone()
                    .with_nullable(false);
                cast_null_predicate.physical_fields[0] = cast_null_predicate.physical_fields[0]
                    .clone()
                    .with_nullable(false);
                check_case(cast_null_predicate, spark, ansi, error_if_invoked).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn local_any_null_parent_waits_for_execution() -> TestResult {
    for ansi in [true, false] {
        for spark in [true, false] {
            for error_if_invoked in [false, true] {
                check_case(
                    scalar_case(
                        "SELECT any_null_identity(v/1) AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)"
                            .into(),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(Some(1.0))]],
                        1,
                        0,
                    ),
                    spark,
                    ansi,
                    error_if_invoked,
                )
                .await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn scalar_rewrite_to_higher_order_preserves_target_folding_permission() -> TestResult {
    for ansi in [true, false] {
        for spark in [true, false] {
            for error_if_invoked in [false, true] {
                for (argument, spark_analyzed_nullable) in [("v/1", true), ("v", false)] {
                    for function in ["rewrite_to_higher_no_fold", "rewrite_to_volatile_higher"] {
                        let mut case = scalar_case(
                            format!(
                                "SELECT {function}({argument}) AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)"
                            ),
                            DataType::Float64,
                            vec![vec![ScalarValue::Float64(Some(1.0))]],
                            1,
                            0,
                        );
                        case.analyzed_fields[0] = case.analyzed_fields[0]
                            .clone()
                            .with_nullable(!spark || spark_analyzed_nullable);
                        case.physical_fields[0] =
                            case.physical_fields[0].clone().with_nullable(!spark);
                        check_case(case, spark, ansi, error_if_invoked).await?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn case_reachability_preserves_required_errors_and_opaque_execution() -> TestResult {
    for ansi in [true, false] {
        for error_if_invoked in [false, true] {
            for (sql, value, calls, physical_nullable) in [
                (
                    "SELECT CASE WHEN (1 DIV 1)=1 THEN retain_constant(1/1) ELSE CAST(7 AS DOUBLE) END AS r",
                    1.0,
                    1,
                    true,
                ),
                (
                    "SELECT CASE WHEN false THEN retain_constant(1 DIV 0) ELSE CAST(7 AS DOUBLE) END AS r",
                    7.0,
                    0,
                    false,
                ),
                (
                    "SELECT CASE WHEN flag THEN retain_constant(v/0) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(false,CAST(1 AS DOUBLE)) AS t(flag,v)",
                    7.0,
                    0,
                    true,
                ),
            ] {
                let mut case = scalar_case(
                    sql.into(),
                    DataType::Float64,
                    vec![vec![ScalarValue::Float64(Some(value))]],
                    calls,
                    0,
                );
                case.physical_fields[0] = case.physical_fields[0]
                    .clone()
                    .with_nullable(physical_nullable);
                check_case(case, true, ansi, error_if_invoked).await?;
            }
            for spark in [true, false] {
                // One guard call and one selected-arm call occur in this one-batch fixture.
                check_case(
                    scalar_case(
                        "SELECT CASE WHEN retain_constant(v)>0 THEN retain_constant(1/d) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(CAST(1 AS DOUBLE),CAST(1 AS DOUBLE)),(CAST(-1 AS DOUBLE),CAST(0 AS DOUBLE)) AS t(v,d)".into(),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(Some(1.0))], vec![ScalarValue::Float64(Some(7.0))]],
                        if error_if_invoked { 1 } else { 2 }, 0,
                    ), spark, ansi, error_if_invoked,
                ).await?;
                // One local batch has one selected row; the false row must not evaluate 1/0.
                check_case(
                    scalar_case(
                        "SELECT CASE WHEN flag THEN retain_constant(1/v) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(true,CAST(1 AS DOUBLE)),(false,CAST(0 AS DOUBLE)) AS t(flag,v)".into(),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(Some(1.0))], vec![ScalarValue::Float64(Some(7.0))]],
                        1, 0,
                    ), spark, ansi, error_if_invoked,
                ).await?;
            }
            if !ansi {
                for sql in [
                    "SELECT CASE WHEN (1 DIV 0)=1 THEN retain_constant(1/1) ELSE CAST(7 AS DOUBLE) END AS r",
                    "SELECT CASE WHEN v/0=CAST(1 AS DOUBLE) THEN retain_constant(1/1) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)",
                ] {
                    let mut case = scalar_case(
                        sql.into(),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(Some(7.0))]],
                        0,
                        0,
                    );
                    case.physical_fields[0] = case.physical_fields[0].clone().with_nullable(false);
                    check_case(case, true, ansi, error_if_invoked).await?;
                }
                for sql in [
                    "SELECT CASE WHEN true THEN retain_constant(1 DIV 0) ELSE CAST(7 AS DOUBLE) END AS r",
                    "SELECT CASE WHEN false THEN CAST(7 AS DOUBLE) ELSE retain_constant(1 DIV 0) END AS r",
                    "SELECT CASE WHEN flag THEN retain_constant(v/0) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(true,CAST(1 AS DOUBLE)) AS t(flag,v)",
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
                        ansi,
                        error_if_invoked,
                    )
                    .await?;
                }
                check_case(
                    scalar_case(
                        "SELECT CASE WHEN flag THEN retain_constant(1/v) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(false,CAST(0 AS DOUBLE)),(true,CAST(0 AS DOUBLE)) AS t(flag,v)".into(),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(Some(7.0))], vec![ScalarValue::Float64(None)]],
                        1, 0,
                    ), true, ansi, error_if_invoked,
                ).await?;
            }
            // The native surface succeeds on its ordinary-division condition control.
            check_case(
                scalar_case(
                    "SELECT CASE WHEN v/0=CAST(1 AS DOUBLE) THEN retain_constant(1/1) ELSE CAST(7 AS DOUBLE) END AS r FROM VALUES(CAST(1 AS DOUBLE)) AS t(v)".into(),
                    DataType::Float64,
                    vec![vec![ScalarValue::Float64(Some(7.0))]], 0, 0,
                ), false, ansi, error_if_invoked,
            ).await?;
            for (sql, expected_error) in [
                (
                    "SELECT CASE WHEN (1 DIV 0)=1 THEN retain_constant(1/1) ELSE CAST(7 AS DOUBLE) END AS r",
                    "SQL error: ParserError(\"Expected: end of statement, found: ( at Line: 1, Column: 18\")",
                ),
                (
                    "SELECT CASE WHEN (1 DIV 1)=1 THEN retain_constant(1/1) ELSE CAST(7 AS DOUBLE) END AS r",
                    "SQL error: ParserError(\"Expected: end of statement, found: ( at Line: 1, Column: 18\")",
                ),
                (
                    "SELECT CASE WHEN true THEN retain_constant(1 DIV 0) ELSE CAST(7 AS DOUBLE) END AS r",
                    "SQL error: ParserError(\"Expected: end of statement, found: true at Line: 1, Column: 18\")",
                ),
                (
                    "SELECT CASE WHEN false THEN CAST(7 AS DOUBLE) ELSE retain_constant(1 DIV 0) END AS r",
                    "SQL error: ParserError(\"Expected: end of statement, found: false at Line: 1, Column: 18\")",
                ),
                (
                    "SELECT CASE WHEN false THEN retain_constant(1 DIV 0) ELSE CAST(7 AS DOUBLE) END AS r",
                    "SQL error: ParserError(\"Expected: end of statement, found: false at Line: 1, Column: 18\")",
                ),
            ] {
                let (native, counters) = session(false, error_if_invoked);
                let error = native
                    .sql(sql)
                    .await
                    .expect_err("native DIV syntax remains rejected");
                assert_eq!(error.to_string(), expected_error, "{sql}");
                assert_eq!(
                    counters.snapshot(),
                    InvocationSnapshot {
                        opaque: 0,
                        permitted: 0
                    }
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn higher_order_case_fields_do_not_evaluate_opaque_predicates() -> TestResult {
    for ansi in [true, false] {
        for spark in [true, false] {
            for error_if_invoked in [false, true] {
                check_case(
                    scalar_case(
                        "SELECT CASE WHEN rewrite_to_higher_no_fold(CAST(1 AS DOUBLE))>0 THEN CAST(NULL AS DOUBLE) ELSE CAST(7 AS DOUBLE) END AS r".into(),
                        DataType::Float64,
                        vec![vec![ScalarValue::Float64(None)]],
                        1,
                        // Field inference may invoke the permitted scalar source before its rewrite.
                        if error_if_invoked { 6 } else { 1 },
                    ),
                    spark,
                    ansi,
                    error_if_invoked,
                )
                .await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn any_null_masks_survive_later_opaque_arguments() -> TestResult {
    let list_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
    let null = ScalarValue::try_new_null(&list_type)?;
    let seven = ScalarValue::List(ScalarValue::new_list(
        &[ScalarValue::Int32(Some(7))],
        &DataType::Int32,
        true,
    ));
    for ansi in [true, false] {
        for error_if_invoked in [false, true] {
            for (sql, rows, calls) in [
                (
                    "SELECT SLICE(a, 1 DIV d, CAST(index_callback(n) AS BIGINT)) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>),0,1) AS t(a,d,n)",
                    vec![vec![null.clone()]],
                    0,
                ),
                (
                    "SELECT SLICE(a, 1 DIV d, CAST(index_callback(n) AS BIGINT)) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>),0,1),(ARRAY(7),1,1) AS t(a,d,n)",
                    vec![vec![null.clone()], vec![seven.clone()]],
                    1,
                ),
                (
                    "SELECT SLICE(a, 1 DIV d, CAST(index_callback(1 DIV n) AS BIGINT)) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>),0,0),(ARRAY(7),1,1) AS t(a,d,n)",
                    vec![vec![null.clone()], vec![seven.clone()]],
                    1,
                ),
                (
                    "SELECT SLICE(a, CAST(index_callback(s) AS BIGINT), 1 DIV d) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>),1,0),(ARRAY(7),CAST(NULL AS INT),0) AS t(a,s,d)",
                    vec![vec![null.clone()], vec![null.clone()]],
                    1,
                ),
            ] {
                check_case(
                    scalar_case(sql.into(), list_type.clone(), rows, calls, 0),
                    true,
                    ansi,
                    error_if_invoked,
                )
                .await?;
            }
            if !ansi {
                for (sql, calls) in [
                    (
                        "SELECT SLICE(a, 1 DIV d, CAST(index_callback(n) AS BIGINT)) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>),0,1),(ARRAY(7),0,1) AS t(a,d,n)",
                        0,
                    ),
                    (
                        "SELECT SLICE(a, 1 DIV d, CAST(index_callback(1 DIV n) AS BIGINT)) AS r FROM VALUES(CAST(NULL AS ARRAY<INT>),0,0),(ARRAY(7),1,0) AS t(a,d,n)",
                        1,
                    ),
                ] {
                    check_case(
                        scalar_case(
                            sql.into(),
                            list_type.clone(),
                            vec![vec![null.clone()], vec![null.clone()]],
                            calls,
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
    }
    Ok(())
}

#[tokio::test]
async fn null_choice_metadata_keeps_selected_rows_and_opaque_execution() -> TestResult {
    let value = |v| vec![ScalarValue::Float64(v)];
    for ansi in [true, false] {
        for spark in [true, false] {
            for error_if_invoked in [false, true] {
                for (sql, rows, calls) in [
                    (
                        "SELECT opaque_null_first(a,1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),1) AS t(a,d)",
                        vec![value(None), value(Some(7.0))],
                        1,
                    ),
                    (
                        "SELECT opaque_first_non_null(a,1/d) AS r FROM VALUES(CAST(7 AS DOUBLE),0),(CAST(NULL AS DOUBLE),1) AS t(a,d)",
                        vec![value(Some(7.0)), value(Some(1.0))],
                        0,
                    ),
                    (
                        "SELECT opaque_null_first(a,1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),0),(CAST(NULL AS DOUBLE),0) AS t(a,d)",
                        vec![value(None), value(None)],
                        0,
                    ),
                    (
                        "SELECT opaque_first_non_null(a,1/d) AS r FROM VALUES(CAST(7 AS DOUBLE),0),(CAST(8 AS DOUBLE),0) AS t(a,d)",
                        vec![value(Some(7.0)), value(Some(8.0))],
                        0,
                    ),
                    (
                        "SELECT opaque_null_first_three_arguments(a,index_callback(v),1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),CAST(NULL AS DOUBLE),0),(CAST(NULL AS DOUBLE),CAST(NULL AS DOUBLE),0) AS t(a,v,d)",
                        vec![value(None), value(None)],
                        0,
                    ),
                    (
                        "SELECT opaque_null_first_three_arguments(a,index_callback(v),1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),CAST(NULL AS DOUBLE),1) AS t(a,v,d)",
                        vec![value(None), value(Some(7.0))],
                        if error_if_invoked { 1 } else { 2 },
                    ),
                    (
                        "SELECT opaque_first_non_null_three_arguments(a,index_callback(v),1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),CAST(1 AS DOUBLE),0) AS t(a,v,d)",
                        vec![value(Some(1.0))],
                        1,
                    ),
                ] {
                    check_case(
                        scalar_case(sql.into(), DataType::Float64, rows, calls, 0),
                        spark,
                        ansi,
                        error_if_invoked,
                    )
                    .await?;
                }
                if !ansi && spark {
                    for (sql, rows, calls) in [
                        (
                            "SELECT opaque_null_first(a,1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),0) AS t(a,d)",
                            vec![value(None), value(Some(7.0))],
                            1,
                        ),
                        (
                            "SELECT opaque_first_non_null(a,1/d) AS r FROM VALUES(CAST(7 AS DOUBLE),0),(CAST(NULL AS DOUBLE),0) AS t(a,d)",
                            vec![value(Some(7.0)), value(None)],
                            0,
                        ),
                        (
                            "SELECT opaque_null_first_three_arguments(a,index_callback(v),1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),CAST(NULL AS DOUBLE),0) AS t(a,v,d)",
                            vec![value(None), value(Some(7.0))],
                            if error_if_invoked { 1 } else { 2 },
                        ),
                    ] {
                        check_case(
                            scalar_case(sql.into(), DataType::Float64, rows, calls, 0),
                            spark,
                            ansi,
                            error_if_invoked,
                        )
                        .await?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn any_null_row_masks_ignore_logical_short_circuit_declaration() -> TestResult {
    for ansi in [true, false] {
        for (sql, rows) in [
            (
                "SELECT null_propagating_product(a,1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),0),(CAST(NULL AS DOUBLE),0) AS t(a,d)",
                vec![
                    vec![ScalarValue::Float64(None)],
                    vec![ScalarValue::Float64(None)],
                ],
            ),
            (
                "SELECT null_propagating_product(a,1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),1) AS t(a,d)",
                vec![
                    vec![ScalarValue::Float64(None)],
                    vec![ScalarValue::Float64(Some(7.0))],
                ],
            ),
        ] {
            check_case(
                scalar_case(sql.into(), DataType::Float64, rows, 1, 0),
                true,
                ansi,
                false,
            )
            .await?;
        }
        if !ansi {
            check_case(
                scalar_case("SELECT null_propagating_product(a,1/d) AS r FROM VALUES(CAST(NULL AS DOUBLE),0),(CAST(7 AS DOUBLE),0) AS t(a,d)".into(), DataType::Float64,
                    vec![vec![ScalarValue::Float64(None)], vec![ScalarValue::Float64(None)]], 1, 0),
                true, ansi, false,
            ).await?;
        }
    }
    Ok(())
}
