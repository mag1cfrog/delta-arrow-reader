//! Physical-plan callbacks report only the operation they own.

use super::{datafusion_execution::plan, *};
use ::datafusion::{
    common::{DataFusionError, config::ConfigOptions},
    physical_plan::collect,
};

#[test]
fn rejected_children_preserve_native_errors_and_the_original_plan() -> TestResult {
    let runtime = runtime()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let fixture = LogTable::new()?;
        fixture.write_commit(1, &[fixture.parquet("secret-data.parquet", &[1, 2])?])?;
        let (context, plan, plan_id) = runtime.block_on(plan(&fixture, backend, 1))?;
        let mut identities = std::collections::HashSet::new();
        for level in [Level::WARN, Level::ERROR, Level::DEBUG] {
            let collector = TraceCollector::new(level);
            let error = with_default(collector.clone(), || {
                Arc::clone(&plan).with_new_children(vec![Arc::clone(&plan)])
            })
            .unwrap_err();
            assert!(matches!(error, DataFusionError::Internal(ref message)
                if message == "DeltaScanExec does not accept child execution plans"));
            let events = collector.events();
            let failures: Vec<_> = events
                .iter()
                .filter(|event| event.level == Level::ERROR)
                .collect();
            assert_eq!(failures.len(), 1, "{events:?}");
            let failure = failures[0];
            assert_eq!(
                failure.fields["event"],
                "datafusion_replace_children.failed"
            );
            assert_eq!(failure.fields["error_phase"], "datafusion");
            assert_eq!(failure.fields["error_code"], "datafusion_adapter");
            assert_eq!(
                failure.fields["error_reason"],
                "scan_children_not_supported"
            );
            assert_eq!(failure.fields["error_source_kind"], "datafusion_internal");
            assert_eq!(failure.fields["plan_id"], plan_id);
            assert_eq!(failure.fields["snapshot_version"], 1);
            assert_eq!(failure.fields["backend"], format!("{backend:?}"));
            assert_eq!(failure.fields["partition_count"], 1);
            assert!(identities.insert(failure.fields["operation_id"].as_u64().unwrap()));
            assert!(
                !events
                    .iter()
                    .any(|event| event.fields.get("outcome") == Some(&json!("completed")))
            );
            assert!(!format!("{:?}", collector.records()).contains("secret-"));
        }
        let batches = runtime.block_on(collect(plan, context.task_ctx()))?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
    }
    Ok(())
}

#[test]
fn callback_milestones_keep_plan_identity_and_capability_results() -> TestResult {
    let runtime = runtime()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let fixture = LogTable::new()?;
        fixture.write_commit(
            1,
            &[
                fixture.parquet("secret-first.parquet", &[1, 2])?,
                fixture.parquet("secret-second.parquet", &[3, 4])?,
            ],
        )?;
        let (context, plan, plan_id) = runtime.block_on(plan(&fixture, backend, 4))?;
        let mut config = ConfigOptions::default();
        config.optimizer.repartition_file_min_size = 0;
        let collector = TraceCollector::new(Level::INFO);
        let mut identities = std::collections::HashSet::new();
        let mut candidates = vec![Arc::clone(&plan)];
        for callback in [
            "replace_children",
            "reset_state",
            "repartition",
            "repartition_again",
        ] {
            let operation = if callback.starts_with("repartition") {
                "datafusion_repartition"
            } else {
                "datafusion_replace_children"
            };
            let original = candidates.last().unwrap();
            let before = original
                .properties()
                .output_partitioning()
                .partition_count();
            let updated = with_default(collector.clone(), || match callback {
                "replace_children" => Arc::clone(original).with_new_children(vec![]).map(Some),
                "reset_state" => Arc::clone(original).reset_state().map(Some),
                _ => original.repartitioned(4, &config),
            })?;
            if callback == "repartition" && backend == ParquetReaderBackend::Direct {
                assert_eq!(
                    updated
                        .as_ref()
                        .unwrap()
                        .properties()
                        .output_partitioning()
                        .partition_count(),
                    4
                );
            } else if callback.starts_with("repartition") {
                assert!(
                    updated.is_none(),
                    "unsupported and repeated attempts are successful probes"
                );
            } else {
                assert!(Arc::ptr_eq(original, updated.as_ref().unwrap()));
            }
            let after = updated.as_ref().map_or(before, |plan| {
                plan.properties().output_partitioning().partition_count()
            });
            let events = collector.events();
            assert_eq!(events.len(), 2, "{events:?}");
            for (event, outcome) in events.iter().zip(["started", "completed"]) {
                assert_eq!(event.level, Level::INFO);
                assert_eq!(event.fields["event"], format!("{operation}.{outcome}"));
                assert_eq!(event.fields["plan_id"], plan_id);
                assert_ne!(event.fields["operation_id"], plan_id);
            }
            assert_eq!(
                events[0].fields["operation_id"],
                events[1].fields["operation_id"]
            );
            assert_eq!(events[0].fields["partition_count"], before);
            assert_eq!(events[1].fields["partition_count"], after);
            assert!(identities.insert(events[0].fields["operation_id"].as_u64().unwrap()));
            collector.clear_records();
            if let Some(updated) = updated {
                candidates.push(updated);
            }
        }
        for candidate in candidates {
            let batches = runtime.block_on(collect(candidate, context.task_ctx()))?;
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                4
            );
        }
    }
    Ok(())
}

#[test]
fn inherited_statistics_failures_remain_owned_by_datafusion() -> TestResult {
    let runtime = runtime()?;
    let fixture = LogTable::new()?;
    let (_, plan, _) = runtime.block_on(plan(&fixture, ParquetReaderBackend::Direct, 1))?;
    let collector = TraceCollector::new(Level::DEBUG);
    with_default(collector.clone(), || -> TestResult {
        assert!(plan.partition_statistics(None).is_ok());
        assert!(plan.partition_statistics(Some(0)).is_ok());
        assert!(matches!(
            plan.partition_statistics(Some(1)),
            Err(DataFusionError::Internal(_))
        ));
        assert!(collector.records().is_empty());
        Ok(())
    })
}
