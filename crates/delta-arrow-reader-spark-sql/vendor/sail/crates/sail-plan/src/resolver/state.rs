// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_common::arrow::datatypes::{Field, FieldRef};
use datafusion_common::{Column, DFSchemaRef, TableReference};
use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::interval_qualifier::IntervalQualifier;

/// The field information for fields in the logical plan.
#[derive(Debug, Clone)]
pub(super) struct FieldInfo {
    /// The user-facing name of the field.
    name: String,
    /// Whether this is a hidden field that should be excluded from the
    /// final logical plan.
    /// A hidden field is helpful for filtering or sorting plans using columns
    /// of its child plans (e.g. a key column of the left/right plans in an outer join).
    hidden: bool,
    /// The Spark qualifier of an interval field, where the resolver knows it (#1152).
    interval_qualifier: Option<IntervalQualifier>,
    /// The source qualifier of a visible USING key whose plan alias is unqualified.
    using_key_qualifier: Option<TableReference>,
}

impl FieldInfo {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn is_hidden(&self) -> bool {
        self.hidden
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct PlanResolverStateConfig {
    pub arrow_allow_large_var_types: bool,
    /// Nested names retain CAST; standalone casts of named columns keep the column name.
    pub include_cast_in_expression_name: bool,
}

#[derive(Debug, Default)]
pub(super) enum AggregateState {
    /// The expressions in the `GROUP BY` clause is being resolved.
    Grouping { projections: Vec<NamedExpr> },
    /// The expressions in the `HAVING` clause is being resolved.
    Having {
        projections: Vec<NamedExpr>,
        grouping: Vec<NamedExpr>,
    },
    /// An ORDER BY expression can reference both SELECT output and aggregate input.
    Sort {
        output: DFSchemaRef,
        merged: DFSchemaRef,
    },
    /// There is no aggregate being resolved.
    #[default]
    None,
}

#[derive(Debug)]
pub(super) struct PlanResolverState {
    next_id: usize,
    /// A map from the generated opaque field ID to field information.
    fields: HashMap<String, FieldInfo>,
    /// The outer query schema for the current subquery.
    outer_query_schema: Option<DFSchemaRef>,
    /// The aggregate state for the current query.
    aggregate_state: AggregateState,
    /// The input schema and pre-lowering aggregate references for an implicit PIVOT.
    pub(super) pivot_aggregate_references: Option<(DFSchemaRef, HashSet<Column>)>,
    /// The CTEs for the current query.
    ctes: HashMap<TableReference, Arc<LogicalPlan>>,
    config: PlanResolverStateConfig,
    /// Stack of in-scope lambda parameter frames (innermost last).
    /// Each frame holds the declared parameter names of one enclosing lambda
    /// function, along with the parameter field when the enclosing
    /// higher-order function provides it.
    lambda_param_scopes: Vec<Vec<(String, Option<FieldRef>)>>,
    /// The named windows defined in the current query, keyed by window name.
    windows: HashMap<String, spec::Window>,
}

impl Default for PlanResolverState {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanResolverState {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            fields: HashMap::new(),
            outer_query_schema: None,
            aggregate_state: AggregateState::default(),
            pivot_aggregate_references: None,
            ctes: HashMap::new(),
            config: PlanResolverStateConfig::default(),
            lambda_param_scopes: Vec::new(),
            windows: HashMap::new(),
        }
    }

    fn next_field_id(&mut self) -> String {
        let id = self.next_id;
        self.next_id += 1;
        format!("#{id}")
    }

    fn register_field_info(&mut self, name: impl Into<String>, hidden: bool) -> String {
        let field_id = self.next_field_id();
        let info = FieldInfo {
            name: name.into(),
            hidden,
            interval_qualifier: None,
            using_key_qualifier: None,
        };
        self.fields.insert(field_id.clone(), info);
        field_id
    }

    /// Registers a field and returns a generated opaque string ID for the field.
    /// The field ID is unique within the plan resolver state.
    /// No assumption should be made about the format of the field ID.
    pub fn register_field_name(&mut self, name: impl Into<String>) -> String {
        self.register_field_info(name, false)
    }

    /// Registers a hidden field and returns a generated opaque string ID for the field.
    /// This is similar to [`Self::register_field_name`] but the field is marked as hidden.
    pub fn register_hidden_field_name(&mut self, name: impl Into<String>) -> String {
        self.register_field_info(name, true)
    }

    pub fn set_using_key_qualifier(&mut self, field_id: &str, qualifier: Option<TableReference>) {
        if let Some(info) = self.fields.get_mut(field_id) {
            info.using_key_qualifier = qualifier;
        }
    }

    pub fn using_key_qualifier(&self, field_id: &str) -> Option<&TableReference> {
        self.fields.get(field_id)?.using_key_qualifier.as_ref()
    }

    /// Sets the display name of an already-registered field. Used to give a
    /// materialized column (e.g. an unnested `window` column) a referenceable name.
    pub fn set_field_name(&mut self, field_id: &str, name: impl Into<String>) {
        if let Some(info) = self.fields.get_mut(field_id) {
            info.name = name.into();
        }
    }

    /// Records the Spark qualifier of an interval field registered here.
    pub fn set_interval_qualifier(&mut self, field_id: &str, qualifier: Option<IntervalQualifier>) {
        if let Some(info) = self.fields.get_mut(field_id) {
            info.interval_qualifier = qualifier;
        }
    }

    /// The Spark qualifier of a registered interval field, where it is known.
    pub fn interval_qualifier(&self, field_id: &str) -> Option<IntervalQualifier> {
        self.fields
            .get(field_id)
            .and_then(|info| info.interval_qualifier)
    }

    pub fn register_field(&mut self, field: impl AsRef<Field>) -> String {
        self.register_field_info(field.as_ref().name(), false)
    }

    /// Registers each field of a scanned source and returns unique internal names to avoid
    /// column name collisions. Spark reads an Arrow interval column as `DAY TO SECOND` or `YEAR
    /// TO MONTH` (#1152).
    pub fn register_fields(
        &mut self,
        fields: impl IntoIterator<Item = impl AsRef<Field>>,
    ) -> Vec<String> {
        fields
            .into_iter()
            .map(|field| {
                let field_id = self.register_field(&field);
                let qualifier = IntervalQualifier::default_for_type(field.as_ref().data_type());
                self.set_interval_qualifier(&field_id, qualifier);
                field_id
            })
            .collect()
    }

    pub fn get_field_info(&self, field_id: &str) -> PlanResult<&FieldInfo> {
        self.fields
            .get(field_id)
            .ok_or_else(|| PlanError::internal(format!("unknown field: {field_id}")))
    }

    pub fn get_outer_query_schema(&self) -> Option<&DFSchemaRef> {
        self.outer_query_schema.as_ref()
    }

    pub fn get_projections_for_grouping(&self) -> &[NamedExpr] {
        match &self.aggregate_state {
            AggregateState::Grouping { projections } => projections.as_ref(),
            _ => &[],
        }
    }

    pub fn get_projections_for_having(&self) -> &[NamedExpr] {
        match &self.aggregate_state {
            AggregateState::Having { projections, .. } => projections.as_ref(),
            _ => &[],
        }
    }

    pub fn get_grouping_for_having(&self) -> &[NamedExpr] {
        match &self.aggregate_state {
            AggregateState::Having { grouping, .. } => grouping.as_ref(),
            _ => &[],
        }
    }

    pub fn sort_select_schema(&self, schema: &DFSchemaRef) -> Option<DFSchemaRef> {
        match &self.aggregate_state {
            // A subquery using a different schema has its own local names.
            AggregateState::Sort { output, merged } if Arc::ptr_eq(merged, schema) => {
                Some(Arc::clone(output))
            }
            _ => None,
        }
    }

    pub fn enter_query_scope(&mut self, schema: DFSchemaRef) -> QueryScope<'_> {
        QueryScope::new(self, schema)
    }

    pub fn enter_aggregate_scope(&mut self, aggregate_state: AggregateState) -> AggregateScope<'_> {
        AggregateScope::new(self, aggregate_state)
    }

    pub fn enter_cte_scope(&mut self) -> CteScope<'_> {
        CteScope::new(self)
    }

    pub fn get_cte(&self, table_ref: &TableReference) -> Option<&LogicalPlan> {
        self.ctes.get(table_ref).map(|cte| cte.as_ref())
    }

    pub fn insert_cte(&mut self, table_ref: TableReference, plan: LogicalPlan) {
        self.ctes.insert(table_ref, Arc::new(plan));
    }

    pub fn enter_config_scope(&mut self) -> ConfigScope<'_> {
        ConfigScope::new(self)
    }

    pub fn set_windows(
        &mut self,
        windows: HashMap<String, spec::Window>,
    ) -> HashMap<String, spec::Window> {
        std::mem::replace(&mut self.windows, windows)
    }

    pub fn get_window(&self, name: &str) -> Option<&spec::Window> {
        self.windows.get(name)
    }

    pub fn config(&self) -> &PlanResolverStateConfig {
        &self.config
    }

    pub fn config_mut(&mut self) -> &mut PlanResolverStateConfig {
        &mut self.config
    }

    /// Enters a scope where the given lambda function parameters are in scope.
    /// The frame is popped when the scope is dropped.
    pub fn enter_lambda_scope(
        &mut self,
        params: Vec<(String, Option<FieldRef>)>,
    ) -> LambdaScope<'_> {
        LambdaScope::new(self, params)
    }

    /// Resolves a name against the in-scope lambda parameters, innermost first.
    /// Returns the declared spelling of the parameter so that the emitted
    /// lambda variable matches the lambda parameter list exactly, along with
    /// the parameter field if known.
    pub fn resolve_lambda_parameter(&self, name: &str) -> Option<(&str, Option<&FieldRef>)> {
        self.lambda_param_scopes
            .iter()
            .rev()
            .find_map(|frame| frame.iter().find(|(p, _)| p.eq_ignore_ascii_case(name)))
            .map(|(p, f)| (p.as_str(), f.as_ref()))
    }

    pub fn lambda_parameter_names(&self) -> impl Iterator<Item = &str> {
        self.lambda_param_scopes
            .iter()
            .rev()
            .flat_map(|frame| frame.iter().map(|(name, _)| name.as_str()))
    }

    pub fn in_lambda_scope(&self) -> bool {
        !self.lambda_param_scopes.is_empty()
    }
}

pub(crate) struct QueryScope<'a> {
    state: &'a mut PlanResolverState,
    previous_outer_query_schema: Option<DFSchemaRef>,
}

impl<'a> QueryScope<'a> {
    fn new(state: &'a mut PlanResolverState, schema: DFSchemaRef) -> Self {
        let previous_outer_query_schema = state.outer_query_schema.replace(schema);
        Self {
            state,
            previous_outer_query_schema,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for QueryScope<'_> {
    fn drop(&mut self) {
        self.state.outer_query_schema = self.previous_outer_query_schema.take();
    }
}

pub(crate) struct AggregateScope<'a> {
    state: &'a mut PlanResolverState,
    previous_aggregate_state: AggregateState,
}

impl<'a> AggregateScope<'a> {
    fn new(state: &'a mut PlanResolverState, aggregate_state: AggregateState) -> Self {
        let previous_aggregate_state =
            std::mem::replace(&mut state.aggregate_state, aggregate_state);
        Self {
            state,
            previous_aggregate_state,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for AggregateScope<'_> {
    fn drop(&mut self) {
        self.state.aggregate_state = std::mem::take(&mut self.previous_aggregate_state);
    }
}

pub(crate) struct CteScope<'a> {
    state: &'a mut PlanResolverState,
    previous_ctes: HashMap<TableReference, Arc<LogicalPlan>>,
}

impl<'a> CteScope<'a> {
    fn new(state: &'a mut PlanResolverState) -> Self {
        let previous_ctes = state.ctes.clone();
        Self {
            state,
            previous_ctes,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for CteScope<'_> {
    fn drop(&mut self) {
        self.state.ctes = std::mem::take(&mut self.previous_ctes);
    }
}

pub(crate) struct ConfigScope<'a> {
    state: &'a mut PlanResolverState,
    previous_config: PlanResolverStateConfig,
}

impl<'a> ConfigScope<'a> {
    fn new(state: &'a mut PlanResolverState) -> Self {
        let previous_config = state.config.clone();
        Self {
            state,
            previous_config,
        }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for ConfigScope<'_> {
    fn drop(&mut self) {
        self.state.config = std::mem::take(&mut self.previous_config);
    }
}

/// Scope for the parameter names of a lambda function being resolved.
pub(crate) struct LambdaScope<'a> {
    state: &'a mut PlanResolverState,
}

impl<'a> LambdaScope<'a> {
    fn new(state: &'a mut PlanResolverState, params: Vec<(String, Option<FieldRef>)>) -> Self {
        state.lambda_param_scopes.push(params);
        Self { state }
    }

    pub(crate) fn state(&mut self) -> &mut PlanResolverState {
        self.state
    }
}

impl Drop for LambdaScope<'_> {
    fn drop(&mut self) {
        self.state.lambda_param_scopes.pop();
    }
}
