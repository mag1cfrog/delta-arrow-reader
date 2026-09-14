// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableFunctionArgs;
use datafusion::datasource::{TableProvider, provider_as_source, source_as_provider};
use datafusion_common::{DFSchema, ScalarValue, TableReference};
use datafusion_expr::{Expr, LogicalPlan, TableScan, TableSource, UNNAMED_TABLE};
use rand::{RngExt, rng};
use sail_common::spec;
use sail_common_datafusion::datasource::{OptionLayer, SourceInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::rename::table_provider::RenameTableProvider;
use sail_common_datafusion::utils::items::ItemTaker;

use super::sample::SAMPLE_ROUNDING_EPSILON;
use crate::error::{PlanError, PlanResult};
use crate::function::{get_built_in_table_function, is_built_in_generator_function};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolves a named table or view reference into a logical plan node.
    /// Looks up the name in the catalog and produces the appropriate plan
    /// depending on whether it's a table, view, or temporary view.
    pub(super) async fn resolve_query_read_named_table(
        &self,
        table: spec::ReadNamedTable,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadNamedTable {
            name,
            temporal,
            sample,
            options,
        } = table;

        // ponytail: registered-table SELECT probe; format paths, options, time travel,
        // and sampling need a deliberate adapter before supporting them.
        if temporal.is_some() || sample.is_some() || !options.is_empty() {
            return Err(PlanError::unsupported("extraction probe: table modifiers"));
        }
        let table_reference = self.resolve_table_reference(&name)?;
        if let Some(cte) = state.get_cte(&table_reference) {
            return Ok(cte.clone());
        }
        let provider = self.ctx.table_provider(table_reference.clone()).await?;
        self.resolve_table_provider_with_rename(
            provider,
            table_reference,
            None,
            vec![],
            None,
            state,
        )
    }

    pub(super) async fn resolve_query_read_dynamic_table(
        &self,
        table: spec::ReadDynamicTable,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadDynamicTable {
            name,
            sample,
            options,
        } = table;
        let schema = Arc::new(DFSchema::empty());
        let resolved = self.resolve_expression(name, &schema, state).await?;
        let name_str = self.evaluate_identifier_expr(resolved, state)?;
        let name = sail_sql_analyzer::expression::from_ast_object_name(
            sail_sql_analyzer::parser::parse_object_name(&name_str)?,
        )?;
        self.resolve_query_read_named_table(
            spec::ReadNamedTable {
                name,
                temporal: None,
                sample,
                options,
            },
            state,
        )
        .await
    }

    /// Apply TABLESAMPLE clause to a LogicalPlan
    pub(super) async fn apply_table_sample(
        &self,
        plan: LogicalPlan,
        table_sample: spec::TableSample,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::TableSample { method, seed } = table_sample;

        // Convert TableSampleMethod to sample bounds
        let (lower_bound, upper_bound) = match method {
            spec::TableSampleMethod::Percent { value } => {
                let percent = self.evaluate_sample_expr_to_f64(value, state).await?;
                let fraction = percent / 100.0;
                if !(-SAMPLE_ROUNDING_EPSILON..=1.0 + SAMPLE_ROUNDING_EPSILON).contains(&fraction) {
                    return Err(PlanError::invalid(format!(
                        "Sampling fraction ({fraction}) must be on interval [0, 1]"
                    )));
                }
                (0.0, fraction)
            }
            spec::TableSampleMethod::Rows { value: _ } => {
                return Err(PlanError::todo("TABLESAMPLE with ROWS"));
            }
            spec::TableSampleMethod::Bucket {
                numerator,
                denominator,
            } => {
                let fraction = numerator as f64 / denominator as f64;
                if !(-SAMPLE_ROUNDING_EPSILON..=1.0 + SAMPLE_ROUNDING_EPSILON).contains(&fraction) {
                    return Err(PlanError::invalid(format!(
                        "Sampling fraction ({fraction}) must be on interval [0, 1]"
                    )));
                }
                (0.0, fraction)
            }
        };

        // Use random seed if not provided
        let seed: i64 = seed.unwrap_or_else(|| {
            let mut r = rng();
            r.random::<i64>()
        });

        // TABLESAMPLE is without replacement
        Self::apply_sample_to_plan(plan, lower_bound, upper_bound, false, seed, state)
    }

    /// Evaluate a sample expression to get a float value.
    /// Resolves the spec expression using an empty schema and uses [LiteralEvaluator]
    /// to support constant expressions beyond just literals.
    async fn evaluate_sample_expr_to_f64(
        &self,
        expr: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<f64> {
        let schema = Arc::new(DFSchema::empty());
        let resolved = self.resolve_expression(expr, &schema, state).await?;
        let cast_expr = Expr::Cast(datafusion_expr::expr::Cast::new(
            Box::new(resolved),
            DataType::Float64,
        ));
        let evaluator = LiteralEvaluator::new();
        let scalar = evaluator
            .evaluate(&cast_expr)
            .map_err(|e| PlanError::invalid(e.to_string()))?;
        match scalar {
            ScalarValue::Float64(Some(v)) => Ok(v),
            _ => Err(PlanError::invalid(
                "TABLESAMPLE requires a numeric expression",
            )),
        }
    }

    pub(super) async fn resolve_query_read_udtf(
        &self,
        udtf: spec::ReadUdtf,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::ReadUdtf {
            name,
            arguments,
            named_arguments,
            options,
        } = udtf;
        if !options.is_empty() {
            return Err(PlanError::todo("ReadType::UDTF options"));
        }
        let Ok(function_name) = <Vec<String>>::from(name).one() else {
            return Err(PlanError::unsupported("qualified table function name"));
        };
        let canonical_function_name = function_name.to_ascii_lowercase();
        if is_built_in_generator_function(&canonical_function_name) {
            let expr = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(function_name),
                arguments,
                named_arguments,
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: None,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            });
            self.resolve_query_project(None, vec![expr], state).await
        } else {
            if !named_arguments.is_empty() {
                return Err(PlanError::unsupported("named table function arguments"));
            }
            let schema = Arc::new(DFSchema::empty());
            let arguments = self.resolve_expressions(arguments, &schema, state).await?;
            let table_function = match get_built_in_table_function(&canonical_function_name) {
                Ok(f) => f,
                _ => match self.ctx.table_function(&canonical_function_name) {
                    Ok(f) => f,
                    _ => {
                        return Err(PlanError::unsupported(format!(
                            "unknown table function: {function_name}"
                        )));
                    }
                },
            };
            let session_state = self.ctx.state();
            let table_provider = table_function.create_table_provider_with_args(
                TableFunctionArgs::new(&arguments, &session_state),
            )?;
            self.resolve_table_provider_with_rename(
                table_provider,
                function_name,
                None,
                vec![],
                None,
                state,
            )
        }
    }

    pub(super) async fn resolve_query_read_data_source(
        &self,
        source: spec::ReadDataSource,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadDataSource {
            format,
            schema,
            options,
            paths,
            predicates,
        } = source;
        if !predicates.is_empty() {
            return Err(PlanError::todo("data source predicates"));
        }
        let Some(format) = format else {
            return Err(PlanError::invalid("missing data source format"));
        };
        let schema = match schema {
            Some(schema) => Some(self.resolve_schema(schema, state)?),
            None => None,
        };
        let info = SourceInfo {
            paths,
            lakehouse_table: None,
            schema,
            constraints: Default::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            // TODO: detect duplicated keys in the set of options
            options: vec![OptionLayer::OptionList {
                items: options.into_iter().collect(),
            }],
            read_case_sensitive: self.config.case_sensitive,
        };
        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        let table_source = registry
            .get(&format)?
            .create_source(&self.ctx.state(), info)
            .await?;
        self.resolve_table_source_with_rename(
            table_source,
            UNNAMED_TABLE,
            None,
            vec![],
            None,
            state,
        )
    }

    pub(super) fn resolve_table_provider_with_rename(
        &self,
        table_provider: Arc<dyn TableProvider>,
        table_reference: impl Into<TableReference>,
        projection: Option<Vec<usize>>,
        filters: Vec<datafusion_expr::expr::Expr>,
        fetch: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        self.resolve_table_source_with_rename(
            provider_as_source(table_provider),
            table_reference,
            projection,
            filters,
            fetch,
            state,
        )
    }

    pub(super) fn resolve_table_source_with_rename(
        &self,
        table_source: Arc<dyn TableSource>,
        table_reference: impl Into<TableReference>,
        projection: Option<Vec<usize>>,
        filters: Vec<datafusion_expr::expr::Expr>,
        fetch: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = table_source.schema();

        let has_duplicates = {
            let mut seen = HashSet::new();
            schema.fields().iter().any(|f| !seen.insert(f.name()))
        };

        let table_source: Arc<dyn TableSource> = if has_duplicates {
            // Preserve existing behavior by wrapping the underlying TableProvider with renaming,
            // but only if this TableSource is DataFusion's DefaultTableSource.
            // TODO: support duplicate column names for other `TableSource` implementations
            let provider = source_as_provider(&table_source).map_err(|e| {
                PlanError::unsupported(format!(
                    "duplicate column names require DefaultTableSource-backed TableProvider: {e}"
                ))
            })?;
            let names = state.register_fields(schema.fields());
            provider_as_source(Arc::new(RenameTableProvider::try_new(provider, names)?))
        } else {
            table_source
        };

        let table_scan = LogicalPlan::TableScan(TableScan::try_new(
            table_reference,
            table_source,
            projection,
            filters,
            fetch,
        )?);

        if !has_duplicates {
            let names = state.register_fields(table_scan.schema().fields());
            Ok(rename_logical_plan(table_scan, &names)?)
        } else {
            Ok(table_scan)
        }
    }
}
