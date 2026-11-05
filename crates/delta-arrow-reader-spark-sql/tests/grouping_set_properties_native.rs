//! Grouping-set masks must preserve only properties true of emitted native rows.

use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Constraint, ScalarValue};
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{Column, lit};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    Distribution, ExecutionPlan, InputOrderMode, Partitioning, PhysicalExpr, collect, displayable,
};
use datafusion::prelude::SessionContext;
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, encode_value, validate_batch};

fn column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

fn memory_input(
    rows: &[(i32, i32)],
    batch_size: usize,
    ordered: &[usize],
) -> TestResult<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("a", DataType::Int32, false)
                .with_metadata([("origin".into(), "group-a".into())].into()),
            Field::new("b", DataType::Int32, false)
                .with_metadata([("origin".into(), "group-b".into())].into()),
        ],
        [("source".into(), "grouping-set-properties".into())].into(),
    ));
    let batches = rows
        .chunks(batch_size)
        .map(|chunk| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(chunk.iter().map(|row| row.0))),
                    Arc::new(Int32Array::from_iter_values(chunk.iter().map(|row| row.1))),
                ],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut source = TestMemoryExec::try_new(&[batches], schema.clone(), None)?;
    if !ordered.is_empty() {
        let ordering = LexOrdering::new(ordered.iter().map(|index| {
            PhysicalSortExpr::new_default(column(schema.field(*index).name(), *index))
        }))
        .ok_or("missing ordering")?;
        source = source.try_with_sort_information(vec![ordering])?;
    }
    Ok(Arc::new(TestMemoryExec::update_cache(&Arc::new(source))))
}

fn grouping(expr: Vec<(Arc<dyn PhysicalExpr>, String)>, masks: Vec<Vec<bool>>) -> PhysicalGroupBy {
    let nulls = expr
        .iter()
        .map(|(_, name)| (lit(ScalarValue::Int32(None)), name.clone()))
        .collect();
    PhysicalGroupBy::new(expr, nulls, masks, true)
}

fn aggregate(
    mode: AggregateMode,
    groups: PhysicalGroupBy,
    input: Arc<dyn ExecutionPlan>,
) -> TestResult<Arc<AggregateExec>> {
    let schema = input.schema();
    let count = Arc::new(
        AggregateExprBuilder::new(count_udaf(), vec![lit(1_i64)])
            .schema(schema.clone())
            .alias("n")
            .build()?,
    );
    Ok(Arc::new(AggregateExec::try_new(
        mode,
        groups,
        vec![count],
        vec![None],
        input,
        schema,
    )?))
}

fn log_aggregate_properties(id: &str, plan: &Arc<AggregateExec>) {
    eprintln!(
        "GROUPING_SET_NATIVE {}",
        json!({
            "id":id,"plan":displayable(plan.as_ref()).indent(true).to_string(),
            "schema":format!("{:?}",plan.schema()),
            "equivalence_properties":plan.properties().equivalence_properties().to_string(),
            "constraints":format!("{:?}",plan.properties().equivalence_properties().constraints()),
            "partitioning":format!("{:?}",plan.properties().output_partitioning()),
            "input_order_mode":format!("{:?}",plan.input_order_mode()),
        })
    );
}

async fn collect_encoded_rows(plan: Arc<dyn ExecutionPlan>) -> TestResult<Value> {
    let schema = plan.schema();
    let batches = collect(plan, SessionContext::new().task_ctx()).await?;
    let mut rows = Vec::new();
    for batch in batches {
        validate_batch(schema.as_ref(), &batch)?;
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode_value(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!(rows))
}

#[test]
fn masked_constants_do_not_choose_a_final_sorted_prefix() -> TestResult {
    let input = memory_input(&[(1, 10), (2, 10)], 1, &[])?;
    let groups = grouping(
        vec![(lit(7_i32), "key".into())],
        vec![vec![false], vec![true]],
    );
    let partial = aggregate(AggregateMode::Partial, groups.clone(), input)?;
    let repartition: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        partial.clone(),
        Partitioning::Hash(partial.output_group_expr(), 2),
    )?);
    let final_aggregate = aggregate(
        AggregateMode::FinalPartitioned,
        groups.as_final(),
        repartition,
    )?;
    log_aggregate_properties("masked_constant_partial", &partial);
    log_aggregate_properties("masked_constant_final", &final_aggregate);
    assert!(
        partial
            .properties()
            .equivalence_properties()
            .is_expr_constant(&column("key", 0))
            .is_none()
    );
    assert!(
        final_aggregate
            .properties()
            .equivalence_properties()
            .is_expr_constant(&column("key", 0))
            .is_none()
    );
    assert_eq!(final_aggregate.input_order_mode(), &InputOrderMode::Linear);
    assert!(
        partial
            .properties()
            .equivalence_properties()
            .constraints()
            .is_empty()
    );
    Ok(())
}

#[test]
fn mask_filtering_keeps_original_alias_indexes_and_unmasked_equalities() -> TestResult {
    let input = memory_input(&[(1, 10), (2, 10)], 1, &[])?;
    let groups = grouping(
        ["masked", "kept", "also_kept"]
            .into_iter()
            .map(|name| (lit(7_i32), name.into()))
            .collect(),
        vec![vec![false, false, false], vec![true, false, false]],
    );
    let aggregate = aggregate(AggregateMode::Partial, groups, input)?;
    log_aggregate_properties("shared_constant_source", &aggregate);
    let eq = aggregate.properties().equivalence_properties();
    let masked = column("masked", 0);
    let kept = column("kept", 1);
    let also_kept = column("also_kept", 2);
    assert!(eq.is_expr_constant(&masked).is_none());
    assert!(eq.is_expr_constant(&kept).is_some());
    assert!(eq.is_expr_constant(&also_kept).is_some());
    let normalized_kept = eq.eq_group().normalize_expr(kept);
    let normalized_also_kept = eq.eq_group().normalize_expr(also_kept);
    let normalized_masked = eq.eq_group().normalize_expr(masked);
    assert!(normalized_kept.eq(&normalized_also_kept));
    assert!(!normalized_masked.eq(&normalized_kept));
    Ok(())
}

#[test]
fn masks_invalidate_hash_keys_only_when_their_values_change() -> TestResult {
    let mut observations = Vec::new();
    for (index, name) in [(0, "a"), (1, "b")] {
        let input = memory_input(&[(1, 10), (2, 10)], 1, &[])?;
        let input: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![column(name, index)], 2),
        )?);
        let input_schema = input.schema();
        let groups = grouping(
            vec![(column("a", 0), "a".into()), (column("b", 1), "b".into())],
            vec![vec![false, false], vec![true, false]],
        );
        let aggregate = aggregate(AggregateMode::Partial, groups, input)?;
        log_aggregate_properties(&format!("input_hash_{name}"), &aggregate);
        let satisfies = aggregate
            .properties()
            .output_partitioning()
            .satisfaction(
                &Distribution::HashPartitioned(vec![column(name, index)]),
                aggregate.properties().equivalence_properties(),
                false,
            )
            .is_satisfied();
        assert_eq!(aggregate.schema().metadata(), input_schema.metadata());
        for field_index in 0..2 {
            assert_eq!(
                aggregate.schema().field(field_index).metadata(),
                input_schema.field(field_index).metadata()
            );
        }
        observations.push((name, satisfies));
    }
    assert_eq!(observations, vec![("a", false), ("b", true)]);
    Ok(())
}

#[tokio::test]
async fn duplicate_sets_drop_input_key_uniqueness_and_keep_packed_ids() -> TestResult {
    let input = memory_input(&[(1, 10), (2, 10), (2, 10), (3, 10)], 2, &[])?;
    let distinct = aggregate(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![(column("a", 0), "a".into())]),
        input,
    )?;
    assert!(
        distinct
            .properties()
            .equivalence_properties()
            .constraints()
            .iter()
            .any(|constraint| matches!(constraint, Constraint::Unique(indices) if indices == &[0]))
    );
    let groups = grouping(
        vec![(column("a", 0), "a".into())],
        vec![vec![false], vec![false]],
    );
    let grouped = aggregate(AggregateMode::Single, groups, distinct)?;
    log_aggregate_properties("duplicate_sets_on_unique_input", &grouped);
    let actual = collect_encoded_rows(grouped.clone()).await?;
    eprintln!(
        "GROUPING_SET_NATIVE_ROWS {}",
        json!({"id":"packed_duplicate_ids","actual":actual})
    );
    assert_eq!(
        actual,
        json!([
            ["1", "0", "1"],
            ["1", "2", "1"],
            ["2", "0", "1"],
            ["2", "2", "1"],
            ["3", "0", "1"],
            ["3", "2", "1"],
        ])
    );
    assert!(grouped.properties().equivalence_properties().constraints().iter().all(|constraint|
        !matches!(constraint, Constraint::Unique(indices) | Constraint::PrimaryKey(indices)
            if !indices.contains(&1))));
    Ok(())
}

#[tokio::test]
async fn sorted_multiple_sets_keep_counts_when_a_prefix_reappears_next_batch() -> TestResult {
    let input = memory_input(&[(1, 10), (2, 10), (2, 10), (3, 10)], 2, &[0])?;
    let groups = grouping(
        vec![(column("a", 0), "a".into()), (column("b", 1), "b".into())],
        vec![vec![false, false], vec![false, true]],
    );
    let grouped = aggregate(AggregateMode::Single, groups, input)?;
    log_aggregate_properties("sorted_multi_set_boundary_revisit", &grouped);
    let actual = collect_encoded_rows(grouped).await?;
    eprintln!(
        "GROUPING_SET_NATIVE_ROWS {}",
        json!({"id":"sorted_multi_set_boundary_revisit","actual":actual})
    );
    let mut expected = json!([
        ["1", "10", "0", "1"],
        ["1", null, "1", "1"],
        ["2", "10", "0", "2"],
        ["2", null, "1", "2"],
        ["3", "10", "0", "1"],
        ["3", null, "1", "1"],
    ])
    .as_array()
    .ok_or("missing rows")?
    .clone();
    expected.sort_by_key(Value::to_string);
    assert_eq!(actual, json!(expected));
    Ok(())
}

#[tokio::test]
async fn masked_leading_sort_key_does_not_leave_a_dependent_sorted_suffix() -> TestResult {
    let input = memory_input(&[(1, 1), (1, 2), (2, 1), (2, 2)], 2, &[0, 1])?;
    let groups = grouping(
        vec![(column("a", 0), "a".into()), (column("b", 1), "b".into())],
        vec![vec![false, false], vec![true, false]],
    );
    let grouped = aggregate(AggregateMode::Single, groups, input)?;
    log_aggregate_properties("masked_leading_sort_key", &grouped);
    let actual = collect_encoded_rows(grouped.clone()).await?;
    eprintln!(
        "GROUPING_SET_NATIVE_ROWS {}",
        json!({"id":"masked_leading_sort_key","actual":actual})
    );
    assert_eq!(grouped.input_order_mode(), &InputOrderMode::Linear);
    assert!(grouped.properties().output_ordering().is_none());
    let mut expected = json!([
        ["1", "1", "0", "1"],
        ["1", "2", "0", "1"],
        ["2", "1", "0", "1"],
        ["2", "2", "0", "1"],
        [null, "1", "2", "2"],
        [null, "2", "2", "2"],
    ])
    .as_array()
    .ok_or("missing rows")?
    .clone();
    expected.sort_by_key(Value::to_string);
    assert_eq!(actual, json!(expected));
    Ok(())
}

#[tokio::test]
async fn ordinary_single_set_and_final_ordering_remain_valid() -> TestResult {
    for grouping_set in [false, true] {
        let input = memory_input(&[(1, 10), (2, 10), (2, 10), (3, 10)], 2, &[0, 1])?;
        let expr = vec![(column("a", 0), "a".into()), (column("b", 1), "b".into())];
        let groups = if grouping_set {
            grouping(expr, vec![vec![false, false]])
        } else {
            PhysicalGroupBy::new_single(expr)
        };
        let partial = aggregate(AggregateMode::Partial, groups.clone(), input)?;
        let final_aggregate = aggregate(AggregateMode::Final, groups.as_final(), partial.clone())?;
        log_aggregate_properties(
            &format!("valid_partial_grouping_set_{grouping_set}"),
            &partial,
        );
        log_aggregate_properties(
            &format!("valid_final_grouping_set_{grouping_set}"),
            &final_aggregate,
        );
        assert_eq!(partial.input_order_mode(), &InputOrderMode::Sorted);
        assert_ne!(final_aggregate.input_order_mode(), &InputOrderMode::Linear);
        assert!(partial.properties().output_ordering().is_some());
        assert!(final_aggregate.properties().output_ordering().is_some());
        let actual = collect_encoded_rows(final_aggregate).await?;
        assert_eq!(
            actual,
            if grouping_set {
                json!([
                    ["1", "10", "0", "1"],
                    ["2", "10", "0", "2"],
                    ["3", "10", "0", "1"]
                ])
            } else {
                json!([["1", "10", "1"], ["2", "10", "2"], ["3", "10", "1"]])
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn empty_group_key_lists_do_not_make_duplicate_ids_constant() -> TestResult {
    let input = memory_input(&[(1, 10), (2, 10)], 1, &[])?;
    let ordinary = aggregate(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![]),
        input.clone(),
    )?;
    let groups = PhysicalGroupBy::new(vec![], vec![], vec![vec![], vec![], vec![]], true);
    let grouped = aggregate(AggregateMode::Single, groups, input)?;
    log_aggregate_properties("duplicate_empty_group_key_lists", &grouped);
    assert!(
        ordinary
            .properties()
            .equivalence_properties()
            .is_expr_constant(&column("n", 0))
            .is_some()
    );
    let actual = collect_encoded_rows(grouped.clone()).await?;
    eprintln!(
        "GROUPING_SET_NATIVE_ROWS {}",
        json!({"id":"duplicate_empty_group_key_lists","actual":actual})
    );
    assert_eq!(actual, json!([["0", "2"], ["1", "2"], ["2", "2"]]));
    assert!(
        grouped
            .properties()
            .equivalence_properties()
            .is_expr_constant(&column("__grouping_id", 0))
            .is_none()
    );
    assert!(!grouped.schema().field(0).is_nullable());
    Ok(())
}
