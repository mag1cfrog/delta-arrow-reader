use super::*;
use arrow_schema::Schema;
use datafusion::physical_plan::{coalesce_partitions::CoalescePartitionsExec, empty::EmptyExec};

#[test]
fn unknown_and_estimated_statistics_never_appear_exact() {
    assert_eq!(
        describe_precision(&Precision::Absent),
        json!({"precision": "unknown"})
    );
    assert_eq!(
        describe_precision(&Precision::Inexact(0)),
        json!({"precision": "estimated", "value": "0"})
    );
    assert_eq!(
        describe_precision(&Precision::Exact(0)),
        json!({"precision": "exact", "value": "0"})
    );
}

#[test]
fn node_positions_follow_preorder_even_for_deep_plans() {
    let mut plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
    for _ in 0..128 {
        plan = Arc::new(CoalescePartitionsExec::new(plan));
    }
    let nodes = index_physical_nodes(plan.as_ref());
    assert_eq!(nodes.len(), 129);
    for (index, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.children,
            if index < 128 { vec![index + 1] } else { vec![] }
        );
    }
}
