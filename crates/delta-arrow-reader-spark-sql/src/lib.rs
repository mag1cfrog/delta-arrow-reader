//! Plan Spark SQL against tables registered in a DataFusion session.
//!
//! The host owns table discovery, storage credentials and execution resources.
//! This crate supplies SQL parsing, Spark expression semantics and planning for
//! the retained Spark execution nodes. Planning returns a reusable query; it
//! does not collect rows or open a separate storage reader.
//!
//! Configure the session before registering providers:
//!
//! ```no_run
//! use datafusion::execution::SessionStateBuilder;
//! use datafusion::prelude::SessionContext;
//! use delta_arrow_reader_spark_sql::{configure_spark_sql, plan_query, SparkSqlOptions};
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let state = configure_spark_sql(SessionStateBuilder::new().with_default_features()).build();
//! let session = SessionContext::new_with_state(state);
//! let query = plan_query(&session, "SELECT 1 AS value", &SparkSqlOptions::default()).await?;
//! let physical = query.create_physical_plan(&session).await?;
//! let stream = datafusion::physical_plan::execute_stream(physical, session.task_ctx())?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::Optimizer;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_plan::correlated_scalar::LowerSparkCorrelatedScalars;
use sail_plan::expression_errors::{PrepareSparkExpressions, SparkSqlExpressionSemantics};
use sail_plan::like_errors::{PreserveSparkLikeErrors, set_spark_query_start_time};
use sail_plan::physical_plan::SparkQueryPlanner;
use sail_plan::projection_row_errors::CaptureSparkInputSortBoundaries;
use sail_plan::relabel_pushdown::UnwrapSessionZoneRelabel;
use sail_plan::resolver::PlanResolver;

/// Interpretation of timestamp types without an explicit timezone qualifier.
pub use sail_plan::config::DefaultTimestampType;
/// Behavior when a map constructor receives duplicate keys.
pub use sail_plan::config::MapKeyDedupPolicy;
/// Spark SQL text and expression settings used while resolving a query.
pub use sail_plan::config::PlanConfig as SparkSqlOptions;

/// Install the Spark analyzer and physical planner on a host session builder.
///
/// The builder retains the host's configuration, runtime and other rules. This
/// replaces its query planner with one supporting native DataFusion plans and
/// the retained Spark nodes. When the builder has no optimizer of its own, it
/// also adds a rule for Spark queries right after filter pushdown in DataFusion's
/// default optimizer, ahead of rules added with `with_optimizer_rule`. An
/// optimizer the host set, with `with_optimizer_rules` or by building from an
/// existing state, is kept as it is, without that rule. Call once before building
/// the session.
pub fn configure_spark_sql(mut builder: SessionStateBuilder) -> SessionStateBuilder {
    if builder.optimizer().is_none() {
        let mut optimizer = Optimizer::new();
        UnwrapSessionZoneRelabel::install(&mut optimizer);
        *builder.optimizer() = Some(optimizer);
    }
    builder
        .with_query_planner(Arc::new(SparkQueryPlanner))
        .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
        .with_analyzer_rule(Arc::new(LowerSparkCorrelatedScalars))
        .with_analyzer_rule(Arc::new(PreserveSparkLikeErrors))
        .with_analyzer_rule(Arc::new(CaptureSparkInputSortBoundaries))
}

/// A resolved Spark query with its output names before optimizer renaming.
#[derive(Debug)]
pub struct SparkSqlQuery {
    /// Resolved DataFusion plan, before the session's analyzer and optimizer run.
    pub logical_plan: LogicalPlan,
    /// User-visible output names in result-column order.
    pub column_names: Vec<String>,
}

impl SparkSqlQuery {
    /// Create an executable plan while retaining the query's output names.
    ///
    /// Use the session that resolved this query. Its Spark analyzer and physical
    /// planner must have been installed with [`configure_spark_sql`]. Rows are
    /// read only when the caller executes the returned plan. Errors from query
    /// analysis, optimization and physical planning propagate to the caller.
    pub async fn create_physical_plan(
        &self,
        session: &SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut state = session.state();
        set_spark_query_start_time(&mut state);
        let extensions = &mut state.config_mut().options_mut().extensions;
        extensions.insert(SparkSqlExpressionSemantics::default());
        extensions.insert(PreserveAggregateStatisticsSchema::default());
        let frame = DataFrame::new(state, self.logical_plan.clone());
        let physical = frame.create_physical_plan().await?;
        rename_physical_plan(physical, &self.column_names)
    }
}

/// Resolve one read-only Spark SQL statement against the session's registry.
///
/// The session must be configured with [`configure_spark_sql`]. Syntax errors,
/// unsupported statements and resolution errors are returned without executing
/// the query. This function does not create or modify catalog objects.
/// SQL configuration references are expanded before parsing according to
/// [`SparkSqlOptions::variable_substitution_enabled`] and its explicit values.
/// This does not alter provider values or native `SessionContext::sql`.
pub async fn plan_query(
    session: &SessionContext,
    sql: &str,
    options: &SparkSqlOptions,
) -> Result<SparkSqlQuery> {
    let sql = sail_sql_analyzer::substitution::substitute_sql_variables(
        sql,
        options.variable_substitution_enabled,
        &options.variable_substitution_values,
    )
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let query = sail_sql_analyzer::statement::from_ast_statement(ast)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let named = PlanResolver::new(session, Arc::new(options.clone()))
        .resolve_named_plan(query)
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(SparkSqlQuery {
        logical_plan: named.plan,
        column_names: named.fields,
    })
}
