// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion_common::DFSchemaRef;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{EmptyRelation, Expr, ExprSchemable, LogicalPlan, expr};
use datafusion_functions_nested::concat::ArrayConcat;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::multi_expr::MultiExpr;
use sail_sql_analyzer::expression::{
    DATETIME_UNIT_FUNCTIONS_WITH_ROUTINE, DATETIME_UNIT_FUNCTIONS_WITHOUT_ROUTINE,
};

use crate::error::{PlanError, PlanResult};
use crate::formatter::SparkPlanFormatter;
use crate::function::common::{
    AggFunctionInput, FunctionContextInput, ScalarFunctionInput, dictionary_string_argument,
    dictionary_string_type, unresolved_routine_error,
};
use crate::function::float_keys::normalize_spark_float_key;
use crate::function::{
    get_built_in_aggregate_function, get_built_in_function, is_higher_order_function,
};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::day_time_interval::cast_date_beside_time_interval;
use crate::resolver::expression::lambda::is_spec_lambda_argument;
use crate::resolver::interval_qualifier::{
    IntervalOperand, function_qualifier, resolved_qualifier,
};
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_function(
        &self,
        function: spec::UnresolvedFunction,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        let spec::UnresolvedFunction {
            function_name,
            arguments,
            named_arguments,
            is_distinct,
            is_user_defined_function: _,
            is_internal: _,
            ignore_nulls,
            filter,
            order_by,
        } = function;

        let Ok(function_name) = <Vec<String>>::from(function_name).one() else {
            return Err(PlanError::unsupported("qualified function name"));
        };
        let canonical_function_name = function_name.to_ascii_lowercase();
        // Spark has no TIMESTAMP_DIFF or TIMESTAMP_ADD routine, and resolves the routine before
        // its arguments, named or not, whatever the unit; a host registration may add one.
        if matches!(
            canonical_function_name.as_str(),
            "timestamp_diff" | "timestamp_add"
        ) && self.ctx.udf(&canonical_function_name).is_err()
        {
            return Err(unresolved_routine_error(&function_name, self.ctx));
        }
        // Spark resolves these routines before named arguments, too.
        let (arguments, arity_error) =
            self.resolve_datetime_unit_call(&function_name, arguments)?;
        if !named_arguments.is_empty()
            || arguments
                .iter()
                .any(|arg| matches!(arg, spec::Expr::NamedArgument { .. }))
        {
            return Err(PlanError::unsupported("named function arguments"));
        }

        // ARRAY_CONCAT is native-only; a host registration may replace it.
        if canonical_function_name == "array_concat"
            && self
                .ctx
                .udf(&canonical_function_name)
                .map_or(true, |udf| udf.inner().is::<ArrayConcat>())
        {
            return Err(unresolved_routine_error(&function_name, self.ctx));
        }

        // For `mode() WITHIN GROUP (ORDER BY col)`, move the sort expression and a
        // tie-break sentinel into the argument list before resolution, so that the
        // aggregate function receives the column and the output column name can be
        // derived from the argument display names.
        let (arguments, order_by) =
            Self::convert_mode_within_group(&canonical_function_name, arguments, order_by)?;

        let has_spec_lambda_argument = arguments.iter().any(is_spec_lambda_argument);

        // Each argument's interval qualifier decides the qualifier of an interval result, and
        // a day-time interval's end field decides Spark's DATE arithmetic (#952, #1152).
        let (argument_display_names, arguments, interval_operands) = if canonical_function_name
            == "struct"
        {
            let (names, arguments) = self
                .resolve_struct_arguments(arguments, schema, state)
                .await?;
            (names, arguments, vec![])
        } else if has_spec_lambda_argument && is_higher_order_function(&canonical_function_name) {
            let (names, arguments) = self
                .resolve_higher_order_function_arguments(
                    &canonical_function_name,
                    arguments,
                    schema,
                    state,
                )
                .await?;
            (names, arguments, vec![])
        } else {
            self.resolve_expression_arguments_with_qualifiers(arguments, schema, state)
                .await?
        };
        if let Some(error) = arity_error {
            return Err(error);
        }

        let has_lambda_argument = arguments.iter().any(|x| matches!(x, expr::Expr::Lambda(_)));
        // Operand types only choose Spark's display order for interval and
        // timestamp arithmetic. If a type is not resolvable yet, keep the
        // generic operator name; the call itself still reports type errors.
        let argument_types = if matches!(canonical_function_name.as_str(), "+" | "-" | "*") {
            arguments
                .iter()
                .map(|arg| arg.get_type(schema).ok())
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default()
        } else {
            vec![]
        };
        // After `argument_types`, so that the call's name still reads a DATE operand.
        let arguments = if matches!(
            canonical_function_name.as_str(),
            "+" | "-" | "try_add" | "try_subtract"
        ) {
            cast_date_beside_time_interval(
                &canonical_function_name,
                arguments,
                &interval_operands,
                schema,
                &self.config.session_timezone,
            )
        } else {
            arguments
        };

        // The native registry also contains DataFusion builtins. Keep Spark's
        // implementations (and explicit unsupported entries) authoritative.
        let func = if let Ok(func) = get_built_in_function(&canonical_function_name) {
            if ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                return Err(PlanError::invalid("invalid scalar function clause"));
            }
            let arguments =
                if keeps_dictionary_strings(&canonical_function_name, &arguments, schema)? {
                    arguments
                } else {
                    arguments
                        .into_iter()
                        .map(|arg| dictionary_string_argument(arg, schema))
                        .collect()
                };
            let input = ScalarFunctionInput {
                arguments,
                function_context: FunctionContextInput {
                    argument_display_names: &argument_display_names,
                    plan_config: &self.config,
                    session_context: self.ctx,
                    schema,
                },
            };
            func(input)?
        } else if let Ok(func) = get_built_in_aggregate_function(&canonical_function_name) {
            let filter = match filter {
                Some(x) => Some(Box::new(self.resolve_expression(*x, schema, state).await?)),
                None => None,
            };
            let order_by = match order_by {
                Some(x) => self.resolve_sort_orders(x, true, schema, state).await?,
                None => vec![],
            };
            // For DISTINCT aggregate functions with a wildcard argument (e.g., COUNT(DISTINCT *)),
            // expand the wildcard to visible column references here in the resolver where we have
            // access to `state` for hidden-column filtering. This ensures hidden columns (e.g.,
            // join keys) are excluded from the distinct count.
            #[expect(deprecated)]
            let arguments = if is_distinct
                && matches!(
                    arguments.as_slice(),
                    [expr::Expr::Wildcard {
                        qualifier: None,
                        options: _
                    }]
                ) {
                schema
                    .columns()
                    .into_iter()
                    .filter(|c| {
                        state
                            .get_field_info(&c.name)
                            .is_ok_and(|info| !info.is_hidden())
                    })
                    .map(expr::Expr::Column)
                    .collect()
            } else {
                arguments
            };
            if let Some((pivot_schema, references)) = &mut state.pivot_aggregate_references
                && Arc::ptr_eq(pivot_schema, schema)
            {
                // An aggregate builder can erase a reference, as COUNT does for non-null inputs.
                for expression in arguments
                    .iter()
                    .chain(filter.as_deref())
                    .chain(order_by.iter().map(|order| &order.expr))
                {
                    Self::add_pivot_references(expression, schema, references)?;
                }
            }
            // An aggregate reads a dictionary-encoded STRING argument as its STRING values (#1242).
            let arguments = arguments
                .into_iter()
                .map(|arg| dictionary_string_argument(arg, schema))
                .collect();
            let input = AggFunctionInput {
                arguments,
                distinct: is_distinct,
                ignore_nulls,
                filter,
                order_by,
                function_context: FunctionContextInput {
                    argument_display_names: &argument_display_names,
                    plan_config: &self.config,
                    session_context: self.ctx,
                    schema,
                },
            };
            // Normalize only the DISTINCT operations retained by the function builder.
            func(input)?
                .transform_up(|mut expression| {
                    if let Expr::AggregateFunction(function) = &mut expression
                        && function.params.distinct
                    {
                        function.params.args = std::mem::take(&mut function.params.args)
                            .into_iter()
                            .map(|arg| normalize_spark_float_key(arg, schema))
                            .collect::<datafusion_common::Result<_>>()?;
                        Ok(Transformed::yes(expression))
                    } else {
                        Ok(Transformed::no(expression))
                    }
                })
                .data()?
        } else if let Ok(udf) = self.ctx.udf(&canonical_function_name) {
            if is_distinct || ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                return Err(PlanError::invalid("invalid scalar function clause"));
            }
            udf.call(arguments)
        } else {
            return Err(PlanError::unsupported(format!(
                "unknown function: {function_name}",
            )));
        };

        // DataFusion lambda variables carry no type until resolved against the schema.
        // Sail bypasses the DataFusion SQL planner, so resolution happens here — but
        // only when not inside an enclosing lambda scope: a higher-order function
        // nested in another lambda body has free variables that only the outermost
        // resolution, which sees the whole expression tree, can bind.
        let func = if has_lambda_argument && !state.in_lambda_scope() {
            func.resolve_lambda_variables(schema)?.data
        } else {
            func
        };

        // When `COUNT(DISTINCT *)` is used, expand the wildcard display names
        // to individual column names so the output header matches Spark JVM behavior
        // (e.g., `count(DISTINCT a, b, c)` instead of `count(DISTINCT *)`).
        let argument_display_names =
            if is_distinct && argument_display_names.iter().any(|n| n == "*") {
                schema
                    .columns()
                    .iter()
                    .filter_map(|c| {
                        let info = state.get_field_info(&c.name).ok()?;
                        if info.is_hidden() {
                            None
                        } else {
                            Some(info.name().to_string())
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                argument_display_names
            };

        let name = SparkPlanFormatter.function_to_string_with_types(
            &canonical_function_name,
            argument_display_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
            &argument_types,
        )?;

        // Extract metadata from UDF if it implements return_field_from_args
        let metadata = if let expr::Expr::ScalarFunction(ScalarFunction {
            func: udf, args, ..
        }) = &func
        {
            extract_metadata_from_udf(udf, args)?
        } else {
            vec![]
        };

        // Spark's qualifier of an interval result, from the function or what computed it.
        let interval_qualifier = match IntervalOperand::of(&func, None, schema) {
            IntervalOperand::Interval { .. } => {
                function_qualifier(&canonical_function_name, &interval_operands)
                    .or_else(|| resolved_qualifier(&func, schema))
            }
            IntervalOperand::NotInterval => None,
        };

        let named = NamedExpr::new(vec![name], func).with_interval_qualifier(interval_qualifier);
        if !metadata.is_empty() {
            Ok(named.with_metadata(metadata))
        } else {
            Ok(named)
        }
    }

    pub(super) async fn resolve_expression_call_function(
        &self,
        function_name: spec::ObjectName,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let function = spec::UnresolvedFunction {
            function_name,
            arguments,
            named_arguments: vec![],
            is_distinct: false,
            is_user_defined_function: false,
            is_internal: None,
            ignore_nulls: None,
            filter: None,
            order_by: None,
        };
        self.resolve_expression_function(function, schema, state)
            .await
    }

    async fn resolve_struct_arguments(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Vec<expr::Expr>)> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().include_cast_in_expression_name = true;
        let mut names: Vec<String> = vec![];
        let mut exprs: Vec<expr::Expr> = vec![];

        for expression in expressions {
            let is_dotted_attribute = matches!(
                &expression,
                spec::Expr::UnresolvedAttribute { name, .. } if name.parts().len() > 1
            );
            let (named, alias_display_name) = match expression {
                spec::Expr::Alias {
                    expr,
                    name,
                    metadata,
                } => {
                    let direct_alias_source = Self::is_direct_alias_source(&expr);
                    let named = self.resolve_named_expression(*expr, schema, state).await?;
                    let source_name = named.expression_name()?;
                    let has_alias = !name.is_empty();
                    let named = Self::apply_expression_alias(
                        named,
                        name,
                        metadata,
                        direct_alias_source,
                        schema,
                    )?;
                    let display_name = if has_alias {
                        Some(struct_alias_display_name(
                            &source_name,
                            &named.name.clone().one()?,
                        ))
                    } else {
                        None
                    };
                    (named, display_name)
                }
                expression => (
                    self.resolve_named_expression(expression, schema, state)
                        .await?,
                    None,
                ),
            };
            let NamedExpr {
                name,
                expression_name,
                expr,
                ..
            } = named;

            match expr {
                // Expand wildcard inside `struct(...)` only, to match Spark behavior:
                // - struct(*) expands to all visible columns
                // - struct(alias.*) expands to all visible columns from that qualifier
                #[expect(deprecated)]
                Expr::Wildcard { qualifier, options } => {
                    let plan = LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    });

                    let wildcard = NamedExpr::new(name, Expr::Wildcard { qualifier, options });
                    let (_, expanded) = self.rewrite_wildcard(plan, vec![wildcard], state)?;
                    for named in expanded {
                        names.push(named.name.one()?);
                        exprs.push(named.expr);
                    }
                }

                // Nested-field wildcard expansion can produce a MultiExpr.
                // Flatten it so `struct(a.*)` can become `struct(a.x, a.y, ...)`.
                Expr::ScalarFunction(ScalarFunction { func, args })
                    if func.inner().is::<MultiExpr>() =>
                {
                    if name.len() == args.len() {
                        for (n, arg) in name.into_iter().zip(args) {
                            let field_name = n;
                            names.push(field_name.clone());
                            exprs.push(Expr::Alias(expr::Alias::new(
                                arg,
                                None::<datafusion_common::TableReference>,
                                field_name,
                            )));
                        }
                    } else {
                        return Err(PlanError::internal(format!(
                            "MultiExpr in struct argument must provide one name per expression, got {} names for {} expressions",
                            name.len(),
                            args.len()
                        )));
                    }
                }

                other => {
                    let projection_name = name.one()?;
                    names.push(
                        alias_display_name
                            .or(expression_name)
                            .unwrap_or_else(|| projection_name.clone()),
                    );
                    let expr = if is_dotted_attribute
                        && !matches!(other, Expr::OuterReferenceColumn(..))
                        && other.contains_outer()
                    {
                        other.alias(projection_name)
                    } else {
                        other
                    };
                    exprs.push(expr);
                }
            }
        }

        Ok((names, exprs))
    }

    /// For `mode() WITHIN GROUP (ORDER BY col)`, move the sort expression into the
    /// argument list along with a tie-break sentinel literal derived from the sort
    /// direction. Spark returns the lowest value among equally frequent values for
    /// ascending order and the highest for descending order, so this canonicalizes
    /// the WITHIN GROUP syntax to the same argument form as `mode(col, deterministic)`
    /// for both the aggregate function and the output column name formatter.
    fn convert_mode_within_group(
        function_name: &str,
        arguments: Vec<spec::Expr>,
        order_by: Option<Vec<spec::SortOrder>>,
    ) -> PlanResult<(Vec<spec::Expr>, Option<Vec<spec::SortOrder>>)> {
        if function_name.trim().to_lowercase() != "mode" {
            return Ok((arguments, order_by));
        }
        let Some(sorts) = order_by else {
            // order_by on UnresolvedFunction is populated exclusively from the within_group clause.
            return Ok((arguments, None));
        };
        if !arguments.is_empty() {
            return Err(PlanError::invalid(
                "mode() with WITHIN GROUP does not take arguments",
            ));
        }
        let spec::SortOrder {
            child,
            direction,
            null_ordering: _,
        } = sorts.one()?;
        let tie_break = match direction {
            spec::SortDirection::Descending => "highest",
            spec::SortDirection::Ascending | spec::SortDirection::Unspecified => "lowest",
        };
        Ok((
            vec![
                *child,
                spec::Expr::Literal(spec::Literal::Utf8 {
                    value: Some(tie_break.to_string()),
                }),
            ],
            None,
        ))
    }

    /// Spark's TIMESTAMPDIFF and TIMESTAMPADD grammar for a call of one of its names, written as
    /// `written_name`. A call the SQL analyzer marked with a datetime unit keyword reads the unit
    /// as a string. Any other call is ordinary: TIMESTAMPDIFF, TIMEDIFF and TIMESTAMPADD name no
    /// routine, as written, before their arguments resolve; DATEDIFF, DATE_DIFF, DATEADD and
    /// DATE_ADD take two arguments, so another count fails once the arguments resolve, with the
    /// returned error, which names the routine in lower case as Spark's registry does.
    fn resolve_datetime_unit_call(
        &self,
        written_name: &str,
        mut arguments: Vec<spec::Expr>,
    ) -> PlanResult<(Vec<spec::Expr>, Option<PlanError>)> {
        if let Some(spec::Expr::DatetimeUnitKeyword(unit)) = arguments.first() {
            arguments[0] = spec::Expr::Literal(spec::Literal::Utf8 {
                value: Some(unit.clone()),
            });
            return Ok((arguments, None));
        }
        let routine_name = written_name.to_ascii_lowercase();
        if DATETIME_UNIT_FUNCTIONS_WITHOUT_ROUTINE.contains(&routine_name.as_str()) {
            return Err(unresolved_routine_error(written_name, self.ctx));
        }
        let arity_error = (DATETIME_UNIT_FUNCTIONS_WITH_ROUTINE.contains(&routine_name.as_str())
            && arguments.len() != 2)
            .then(|| {
                PlanError::invalid(format!(
                    "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{routine_name}` requires 2 \
                     parameters but the actual number is {}.",
                    arguments.len()
                ))
            });
        Ok((arguments, arity_error))
    }
}

fn struct_alias_display_name(source: &str, alias: &str) -> String {
    let mut chars = alias.chars();
    let unquoted = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    let alias = if unquoted {
        alias.to_string()
    } else {
        format!("`{}`", alias.replace('`', "``"))
    };
    format!("{source} AS {alias}")
}

/// Extract metadata from a UDF by calling return_field_from_args with real argument information
fn extract_metadata_from_udf(
    udf: &std::sync::Arc<datafusion_expr::ScalarUDF>,
    args: &[expr::Expr],
) -> PlanResult<Vec<(String, String)>> {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ExprSchemable, ReturnFieldArgs};

    // Extract real field types from the resolved expressions
    let empty_schema = datafusion_common::DFSchema::empty();
    let arg_fields: Vec<Arc<Field>> = args
        .iter()
        .enumerate()
        .map(|(i, arg)| {
            let data_type = arg.get_type(&empty_schema).unwrap_or(DataType::Null);
            Arc::new(Field::new(format!("arg_{}", i), data_type, true))
        })
        .collect();

    // Extract literal scalar values from expressions
    let mut scalar_values = Vec::new();

    for arg in args {
        if let expr::Expr::Literal(scalar_value, _) = arg {
            scalar_values.push(scalar_value.clone());
        } else {
            scalar_values.push(ScalarValue::Null);
        }
    }

    let scalar_refs: Vec<Option<&ScalarValue>> = scalar_values
        .iter()
        .map(|v| if v.is_null() { None } else { Some(v) })
        .collect();

    let return_field_args = ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &scalar_refs,
    };

    // Try to extract metadata, but don't fail if it doesn't work
    match udf.return_field_from_args(return_field_args) {
        Ok(field) => Ok(field
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()),
        _ => Ok(vec![]),
    }
}

/// The predicates over STRING values that keep a dictionary-encoded STRING argument, as Delta's
/// provider gives a STRING partition column, so that the Delta scan can still prune by them.
const STRING_PREDICATES: [&str; 21] = [
    "=",
    "==",
    "!=",
    "<>",
    "<",
    "<=",
    ">",
    ">=",
    "<=>",
    "equal_null",
    "in",
    "like",
    "ilike",
    "rlike",
    "regexp",
    "regexp_like",
    "startswith",
    "endswith",
    "contains",
    "isnull",
    "isnotnull",
];

/// Whether a built-in function keeps its dictionary-encoded STRING arguments: a predicate whose
/// arguments are all STRING values or NULL, or STRUCT, which names its fields after its bare
/// column arguments and reads their values itself. Every other function reads such an argument as
/// its STRING values, as Spark reads a STRING (#1242).
fn keeps_dictionary_strings(
    name: &str,
    arguments: &[Expr],
    schema: &DFSchemaRef,
) -> PlanResult<bool> {
    if name == "struct" {
        return Ok(true);
    }
    if !STRING_PREDICATES.contains(&name) {
        return Ok(false);
    }
    for argument in arguments {
        let Ok(data_type) = argument.get_type(schema) else {
            return Ok(false);
        };
        let data_type = dictionary_string_type(&data_type).unwrap_or(&data_type);
        if !(data_type.is_string() || data_type.is_null()) {
            return Ok(false);
        }
    }
    Ok(true)
}
