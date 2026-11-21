use std::sync::Arc;

use datafusion::common::DFSchema;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::optimizer::OptimizerRule;

pub(super) struct ScanRenameRules {
    pub analyzer: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    pub optimizer: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

pub(super) fn merge_scan_rename_projections(
    plan: &LogicalPlan,
    state: &SessionState,
) -> Option<LogicalPlan> {
    let LogicalPlan::Projection(output) = plan else {
        return None;
    };
    let LogicalPlan::Projection(table_rename) = output.input.as_ref() else {
        return None;
    };
    let LogicalPlan::TableScan(scan) = table_rename.input.as_ref() else {
        return None;
    };
    let scan_schema = &scan.projected_schema;
    let width = scan_schema.fields().len();
    if width == 0
        || output.expr.len() != width
        || output.schema.fields().len() != width
        || table_rename.expr.len() != width
        || table_rename.schema.fields().len() != width
        || !same_schema_metadata_and_dependencies(scan_schema, &table_rename.schema)
        || !same_schema_metadata_and_dependencies(scan_schema, &output.schema)
        || !uses_standard_rules(state)
    {
        return None;
    }

    let mut projections = Vec::with_capacity(width);
    for (index, (output_expr, table_expr)) in output.expr.iter().zip(&table_rename.expr).enumerate()
    {
        let (Expr::Alias(output_alias), Expr::Alias(table_alias)) = (output_expr, table_expr)
        else {
            return None;
        };
        let (Expr::Column(renamed_column), Expr::Column(scan_column)) =
            (output_alias.expr.as_ref(), table_alias.expr.as_ref())
        else {
            return None;
        };
        let (output_relation, output_field) = output.schema.qualified_field(index);
        let (renamed_relation, renamed_field) = table_rename.schema.qualified_field(index);
        let (scan_relation, scan_field) = scan_schema.qualified_field(index);
        if output_alias.metadata.is_some()
            || output_alias.relation.as_ref() != output_relation
            || output_alias.name != *output_field.name()
            || table_alias.metadata.is_some()
            || table_alias.relation.as_ref() != renamed_relation
            || table_alias.name != *renamed_field.name()
            || renamed_column.relation.as_ref() != renamed_relation
            || renamed_column.name != *renamed_field.name()
            || scan_column.relation.as_ref() != scan_relation
            || scan_column.name != *scan_field.name()
            || scan_field.as_ref().clone().with_name(renamed_field.name()) != **renamed_field
            || scan_field.as_ref().clone().with_name(output_field.name()) != **output_field
        {
            return None;
        }
        projections.push(
            Expr::Column(scan_column.clone())
                .alias_qualified(output_alias.relation.clone(), output_alias.name.clone()),
        );
    }
    Projection::try_new_with_schema(
        projections,
        Arc::clone(&table_rename.input),
        Arc::clone(&output.schema),
    )
    .ok()
    .map(LogicalPlan::Projection)
}

fn same_schema_metadata_and_dependencies(left: &DFSchema, right: &DFSchema) -> bool {
    left.metadata() == right.metadata()
        && left.functional_dependencies() == right.functional_dependencies()
}

fn uses_standard_rules(state: &SessionState) -> bool {
    // A replacement can share a rule's name while depending on the old shape.
    state.config_options().optimizer.max_passes > 0
        && state.analyzer().function_rewrites.is_empty()
        && state
            .config()
            .get_extension::<ScanRenameRules>()
            .is_some_and(|rules| {
                same_rules(&rules.analyzer, &state.analyzer().rules)
                    && same_rules(&rules.optimizer, &state.optimizer().rules)
            })
}

fn same_rules<R: ?Sized>(expected: &[Arc<R>], actual: &[Arc<R>]) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(left, right)| Arc::ptr_eq(left, right))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Int32Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::config::ConfigOptions;
    use datafusion::error::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::{SparkSqlOptions, configure_spark_sql, plan_query};

    #[derive(Debug)]
    struct HostCoercion;

    impl AnalyzerRule for HostCoercion {
        fn analyze(&self, plan: LogicalPlan, _options: &ConfigOptions) -> Result<LogicalPlan> {
            Ok(plan)
        }

        fn name(&self) -> &str {
            "type_coercion"
        }
    }

    #[tokio::test]
    async fn scan_aliases_merge_only_for_the_standard_rules() -> datafusion::error::Result<()> {
        let builder = || configure_spark_sql(SessionStateBuilder::new().with_default_features());
        let session = SessionContext::new_with_state(builder().build());
        session.register_batch(
            "inputs",
            RecordBatch::try_from_iter([
                ("value", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                ("other", Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef),
            ])?,
        )?;
        let resolved = plan_query(
            &session,
            "SELECT * FROM inputs",
            &SparkSqlOptions::default(),
        )
        .await?;
        let merged = merge_scan_rename_projections(&resolved.logical_plan, &session.state())
            .expect("full-width aliases over a table scan");
        assert_eq!(merged.schema(), resolved.logical_plan.schema());
        assert!(matches!(
            merged,
            LogicalPlan::Projection(projected)
                if matches!(projected.input.as_ref(), LogicalPlan::TableScan(_))
        ));
        let LogicalPlan::Projection(mut mismatched) = resolved.logical_plan.clone() else {
            return datafusion::common::internal_err!("expected the resolved output projection");
        };
        let Expr::Alias(alias) = &mut mismatched.expr[0] else {
            return datafusion::common::internal_err!("expected an output alias");
        };
        alias.name.push_str("_different");
        assert!(
            merge_scan_rename_projections(&LogicalPlan::Projection(mismatched), &session.state())
                .is_none()
        );

        let configured = builder().with_optimizer_rules(Vec::new()).build();
        assert!(merge_scan_rename_projections(&resolved.logical_plan, &configured).is_none());

        let mut custom = SessionStateBuilder::new_from_existing(session.state());
        custom.analyzer().as_mut().expect("cloned analyzer").rules[1] = Arc::new(HostCoercion);
        assert!(merge_scan_rename_projections(&resolved.logical_plan, &custom.build()).is_none());

        let mut disabled = session.state();
        disabled.config_mut().options_mut().optimizer.max_passes = 0;
        assert!(merge_scan_rename_projections(&resolved.logical_plan, &disabled).is_none());

        let reordered = plan_query(
            &session,
            "SELECT other, value FROM inputs",
            &SparkSqlOptions::default(),
        )
        .await?;
        assert!(merge_scan_rename_projections(&reordered.logical_plan, &session.state()).is_none());

        let computed = plan_query(
            &session,
            "SELECT value + 1 FROM inputs",
            &SparkSqlOptions::default(),
        )
        .await?;
        assert!(merge_scan_rename_projections(&computed.logical_plan, &session.state()).is_none());
        Ok(())
    }
}
