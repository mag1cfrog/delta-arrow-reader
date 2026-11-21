//! Disposable counters for the untimed wide-plan trace.

use std::cell::Cell;

use datafusion_common::Result;
use datafusion_common::tree_node::TreeNodeRecursion;

use crate::LogicalPlan;

thread_local! {
    static VISITS: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

pub fn record_root() {
    VISITS.with(|visits| {
        if let Some((roots, children)) = visits.get() {
            visits.set(Some((roots + 1, children)));
        }
    });
}

pub fn record_child() {
    VISITS.with(|visits| {
        if let Some((roots, children)) = visits.get() {
            visits.set(Some((roots, children + 1)));
        }
    });
}

pub fn begin(plan: &LogicalPlan) -> Result<(usize, usize)> {
    let mut projections = 0;
    let mut expressions = 0;
    plan.apply_with_subqueries(|node| {
        if matches!(node, LogicalPlan::Projection(_)) {
            projections += 1;
        }
        node.apply_expressions(|_| {
            expressions += 1;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    VISITS.with(|visits| visits.set(Some((0, 0))));
    Ok((projections, expressions))
}

pub fn finish(stage: &str, pass: usize, rule: &str, input: (usize, usize)) {
    let (roots, children) = VISITS.with(|visits| visits.replace(None)).expect("active rule");
    let (projections, expressions) = input;
    eprintln!("DAR_WIDE\t{stage}\t{pass}\t{rule}\t{projections}\t{expressions}\t{roots}\t{children}");
}
