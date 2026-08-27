//! Dynamic physical filter classification for Delta scan planning.
//!
//! Static pushdown is decided earlier at the logical `TableProvider` boundary.
//! This module is only for DataFusion physical filters that arrive after scan
//! planning, such as `DynamicFilterPhysicalExpr` values produced by a join or
//! top-k operator. The classifier is intentionally conservative: it only
//! retains dynamic filters whose referenced provider output columns all resolve
//! to Delta partition columns for this scan.
//!
//! Retaining a filter here does not mean the provider has enforced it. The
//! execution adapter evaluates the retained state at file-admission time.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion::physical_plan::PhysicalExpr;

/// Dynamic filter accepted by the Delta scan for later partition pruning.
///
/// The original physical expression is kept rather than normalized into a
/// provider expression because `DynamicFilterPhysicalExpr` is stateful: its
/// producer updates the expression at runtime. Keeping the same `Arc` preserves
/// the connection between DataFusion's producer and this scan consumer.
#[derive(Clone, Debug)]
pub(crate) struct RetainedDynamicFilter {
    /// Original physical filter pushed by DataFusion, including any dynamic state.
    pub(crate) physical_expr: Arc<dyn PhysicalExpr>,
    /// Provider output partition columns referenced by this filter.
    pub(crate) partition_columns: Vec<DynamicFilterColumn>,
    /// Provider logical output schema used to validate indexes during retention.
    pub(crate) provider_schema: SchemaRef,
}

/// Provider output partition column referenced by a dynamic filter.
///
/// DataFusion physical expressions identify columns by both name and index.
/// Later partition-value evaluation needs both so it can validate that the
/// retained expression still lines up with the provider output schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DynamicFilterColumn {
    /// Provider output field name.
    pub(crate) name: String,
    /// Provider output field index.
    pub(crate) index: usize,
}

#[derive(Clone, Debug)]
/// Retention decision for one physical filter, preserving input order.
pub(crate) enum DynamicFilterDecision {
    /// The filter is dynamic and references only Delta partition columns.
    Accepted(RetainedDynamicFilter),
    /// The filter is unsupported by this hook and must remain a residual concern.
    Rejected,
}

#[derive(Clone, Debug, Default)]
/// Batch classification for the filters offered to one Delta scan node.
pub(crate) struct DynamicFilterClassification {
    /// One decision per input filter, preserving input order for diagnostics.
    pub(crate) decisions: Vec<DynamicFilterDecision>,
}

impl DynamicFilterClassification {
    /// Classifies DataFusion physical filters against this scan's output schema.
    ///
    /// `partition_columns` must be the Delta table partition columns retained
    /// during logical scan planning. A physical filter is accepted only when all
    /// referenced columns resolve to those retained partition columns.
    #[must_use]
    pub(crate) fn from_filters(
        filters: &[Arc<dyn PhysicalExpr>],
        provider_schema: &SchemaRef,
        partition_columns: &[String],
    ) -> Self {
        Self {
            decisions: filters
                .iter()
                .map(|filter| classify_dynamic_filter(filter, provider_schema, partition_columns))
                .collect(),
        }
    }

    /// Returns accepted filters in input order.
    pub(crate) fn accepted_filters(&self) -> impl Iterator<Item = &RetainedDynamicFilter> {
        self.decisions.iter().filter_map(|decision| match decision {
            DynamicFilterDecision::Accepted(filter) => Some(filter),
            DynamicFilterDecision::Rejected => None,
        })
    }
}

fn classify_dynamic_filter(
    filter: &Arc<dyn PhysicalExpr>,
    provider_schema: &SchemaRef,
    partition_columns: &[String],
) -> DynamicFilterDecision {
    if !contains_dynamic_filter(filter.as_ref()) {
        return DynamicFilterDecision::Rejected;
    }

    let references = collect_column_references(filter.as_ref(), provider_schema);
    if references.has_internal_column {
        return DynamicFilterDecision::Rejected;
    }
    if references.has_unknown_column {
        return DynamicFilterDecision::Rejected;
    }
    if references.columns.is_empty() {
        return DynamicFilterDecision::Rejected;
    }

    let partition_column_set = partition_columns
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    // `references.columns` is a BTreeMap keyed by physical index and name, so
    // retained column mappings are deterministic and match provider output order.
    let (partition_columns, data_column_count): (Vec<_>, usize) =
        references.columns.into_values().fold(
            (Vec::new(), 0),
            |(mut partition_columns, data_count), column| {
                if partition_column_set.contains(column.name.as_str()) {
                    partition_columns.push(column);
                    (partition_columns, data_count)
                } else {
                    (partition_columns, data_count + 1)
                }
            },
        );

    match (partition_columns.is_empty(), data_column_count == 0) {
        (false, true) => DynamicFilterDecision::Accepted(RetainedDynamicFilter {
            physical_expr: Arc::clone(filter),
            partition_columns,
            provider_schema: Arc::clone(provider_schema),
        }),
        _ => DynamicFilterDecision::Rejected,
    }
}

/// Returns whether this expression tree contains DataFusion dynamic state.
///
/// This handles common boolean wrappers around a `DynamicFilterPhysicalExpr`
/// without trying to prove full predicate semantics. Semantic evaluation remains
/// at the file-admission boundary.
fn contains_dynamic_filter(expr: &dyn PhysicalExpr) -> bool {
    expr.is::<DynamicFilterPhysicalExpr>()
        || expr
            .children()
            .into_iter()
            .any(|child| contains_dynamic_filter(child.as_ref()))
}

#[derive(Default)]
struct ColumnReferences {
    /// Resolved provider output columns, keyed for deterministic de-duplication.
    columns: BTreeMap<(usize, String), DynamicFilterColumn>,
    /// Whether any column reference targets provider-owned synthetic state.
    has_internal_column: bool,
    /// Whether any column reference fails strict provider schema validation.
    has_unknown_column: bool,
}

fn collect_column_references(
    expr: &dyn PhysicalExpr,
    provider_schema: &SchemaRef,
) -> ColumnReferences {
    let mut references = ColumnReferences::default();
    collect_column_references_into(expr, provider_schema, &mut references);
    references
}

fn collect_column_references_into(
    expr: &dyn PhysicalExpr,
    provider_schema: &SchemaRef,
    references: &mut ColumnReferences,
) {
    if let Some(column) = expr.downcast_ref::<Column>() {
        collect_column_reference(column, provider_schema, references);
    }

    for child in expr.children() {
        collect_column_references_into(child.as_ref(), provider_schema, references);
    }
}

fn collect_column_reference(
    column: &Column,
    provider_schema: &SchemaRef,
    references: &mut ColumnReferences,
) {
    if column.name().starts_with("__delta_arrow_reader_")
        || column.name().starts_with("__delta_funnel_")
    {
        references.has_internal_column = true;
        return;
    }

    let Some(field) = provider_schema.fields().get(column.index()) else {
        references.has_unknown_column = true;
        return;
    };

    // Physical expressions can carry a valid-looking name with a stale or
    // rewritten index. Require both to match so later partition-value evaluation
    // cannot silently read the wrong provider output field.
    if field.name() != column.name() {
        references.has_unknown_column = true;
        return;
    }

    references.columns.insert(
        (column.index(), column.name().to_owned()),
        DynamicFilterColumn {
            name: column.name().to_owned(),
            index: column.index(),
        },
    );
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, lit};

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_name", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("event_date", DataType::Date32, true),
        ]))
    }

    fn column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn dynamic_filter(children: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(children, lit(true)))
    }

    fn classify(filter: Arc<dyn PhysicalExpr>) -> DynamicFilterClassification {
        DynamicFilterClassification::from_filters(
            &[filter],
            &test_schema(),
            &["region".to_owned(), "event_date".to_owned()],
        )
    }

    fn assert_rejected(classification: &DynamicFilterClassification) {
        assert!(classification.accepted_filters().next().is_none());
        assert!(matches!(
            classification.decisions.first(),
            Some(DynamicFilterDecision::Rejected)
        ));
    }

    #[test]
    fn partition_dynamic_filter_is_accepted() {
        let classification = classify(dynamic_filter(vec![column("region", 2)]));

        assert!(matches!(
            classification.decisions.first(),
            Some(DynamicFilterDecision::Accepted(_))
        ));
        assert_eq!(
            classification
                .accepted_filters()
                .next()
                .expect("filter must be accepted")
                .partition_columns,
            vec![DynamicFilterColumn {
                name: "region".to_owned(),
                index: 2,
            }]
        );
    }

    #[test]
    fn no_column_dynamic_filter_is_rejected() {
        let classification = classify(dynamic_filter(Vec::new()));

        assert_rejected(&classification);
    }

    #[test]
    fn multi_partition_dynamic_filter_retains_sorted_column_mappings() {
        let classification = classify(dynamic_filter(vec![
            column("event_date", 3),
            column("region", 2),
        ]));

        assert_eq!(
            classification
                .accepted_filters()
                .next()
                .expect("filter must be accepted")
                .partition_columns,
            vec![
                DynamicFilterColumn {
                    name: "region".to_owned(),
                    index: 2,
                },
                DynamicFilterColumn {
                    name: "event_date".to_owned(),
                    index: 3,
                },
            ]
        );
    }

    #[test]
    fn data_column_dynamic_filter_is_rejected() {
        let classification = classify(dynamic_filter(vec![column("id", 0)]));

        assert_rejected(&classification);
    }

    #[test]
    fn unknown_column_dynamic_filter_is_rejected() {
        let classification = classify(dynamic_filter(vec![column("ghost", 99)]));

        assert_rejected(&classification);
    }

    #[test]
    fn mixed_partition_and_data_dynamic_filter_is_rejected() {
        let classification = classify(dynamic_filter(vec![column("region", 2), column("id", 0)]));

        assert_rejected(&classification);
    }

    #[test]
    fn dynamic_filter_wrapped_with_data_column_is_rejected_as_mixed() {
        let dynamic = dynamic_filter(vec![column("region", 2)]);
        let wrapped = Arc::new(BinaryExpr::new(dynamic, Operator::And, column("id", 0)));

        let classification = classify(wrapped);

        assert_rejected(&classification);
    }

    #[test]
    fn non_dynamic_filter_is_rejected() {
        let filter = Arc::new(BinaryExpr::new(
            column("region", 2),
            Operator::Eq,
            lit("us-west"),
        ));

        let classification = classify(filter);

        assert_rejected(&classification);
    }

    #[test]
    fn internal_column_dynamic_filter_is_rejected() {
        let classification = classify(dynamic_filter(vec![column("__delta_funnel_row_index", 0)]));

        assert_rejected(&classification);
    }
}
