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

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_plan::expression_errors::PreserveSparkExpressionErrors;
use sail_plan::physical_plan::SparkQueryPlanner;
use sail_plan::resolver::PlanResolver;

/// Interpretation of timestamp types without an explicit timezone qualifier.
pub use sail_plan::config::DefaultTimestampType;
/// Behavior when a map constructor receives duplicate keys.
pub use sail_plan::config::MapKeyDedupPolicy;
/// Spark expression settings used while resolving a query.
pub use sail_plan::config::PlanConfig as SparkSqlOptions;

/// Install the Spark analyzer and physical planner on a host session builder.
///
/// The builder retains the host's configuration, runtime and other rules. This
/// replaces its query planner with one supporting native DataFusion plans and
/// the retained Spark nodes. Call once before building the session.
pub fn configure_spark_sql(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder
        .with_query_planner(Arc::new(SparkQueryPlanner))
        .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
        let frame = session
            .execute_logical_plan(self.logical_plan.clone())
            .await?;
        let physical = frame.create_physical_plan().await?;
        rename_physical_plan(physical, &self.column_names)
    }
}

/// Resolve one read-only Spark SQL statement against the session's registry.
///
/// The session must be configured with [`configure_spark_sql`]. Syntax errors,
/// unsupported statements and resolution errors are returned without executing
/// the query. This function does not create or modify catalog objects.
pub async fn plan_query(
    session: &SessionContext,
    sql: &str,
    options: &SparkSqlOptions,
) -> Result<SparkSqlQuery> {
    let ast = sail_sql_analyzer::parser::parse_one_statement(sql)
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
