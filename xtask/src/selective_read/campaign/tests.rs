use super::*;

fn inventory(revision: u64, mask: usize) -> Vec<(String, Value)> {
    let readers = roster(&json!({"comparison_revision": revision})).unwrap();
    let entries: Map<_, _> = readers.iter().enumerate().map(|(i, reader)| {
        let runnable = mask & (1 << i) != 0;
        ((*reader).into(), json!({"runnable": runnable, "status": if runnable { "success" } else { "unsupported" }}))
    }).collect();
    ["wide.case", "li.case", "reuse.wide.case"]
        .into_iter()
        .map(|job| (job.into(), json!(entries)))
        .collect()
}

#[test]
fn all_reader_subsets_keep_samples_balanced_and_diagnostics_after_timing() -> Result<()> {
    for revision in [2, 4, 6] {
        let readers = [
            "delta-arrow-reader",
            "delta-rs",
            "duckdb",
            "polars",
            if revision == 6 { "spark" } else { "daft" },
        ];
        for mask in 0..32 {
            let enabled: Vec<_> = readers
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << index) != 0)
                .map(|(_, reader)| *reader)
                .collect();
            let jobs = inventory(revision, mask);
            let slots = schedule(
                &jobs,
                "reference",
                &json!({"comparison_revision": revision}),
                None,
                None,
                None,
            )?;
            let slots = array(&slots)?;
            if enabled.is_empty() {
                assert!(slots.is_empty());
                continue;
            }
            let samples = if revision == 2 {
                [0, 10, 12, 12, 16, 10][enabled.len()]
            } else {
                5
            };
            let phases = [
                ("warmup", if revision == 2 { 2 } else { 1 }),
                ("timing", samples),
                ("plan", 1),
                ("diagnostic_warmup", if revision == 2 { 2 } else { 0 }),
                ("diagnostic", if revision == 2 { 2 } else { 1 }),
                ("observer_baseline", if revision == 2 { 2 } else { 0 }),
            ];
            assert_eq!(
                slots.len(),
                jobs.len() * enabled.len() * phases.iter().map(|(_, n)| n).sum::<usize>()
            );
            for (index, slot) in slots.iter().enumerate() {
                assert_eq!(slot["run_id"], format!("reference-{index:05}"));
                assert_eq!(slot["comparison_revision"], revision);
                assert!(jobs.iter().any(|(job, _)| slot["job_id"] == *job));
                assert!(enabled.contains(&text(&slot["reader_id"])?));
                assert_eq!(
                    slot["traced"],
                    matches!(text(&slot["stage"])?, "diagnostic" | "diagnostic_warmup")
                );
            }
            assert!(
                slots.iter().rposition(|s| s["stage"] == "timing")
                    < slots.iter().position(|s| s["stage"] == "plan")
            );
            for (job, _) in &jobs {
                let stage = |name: &str| {
                    slots
                        .iter()
                        .filter(|s| s["job_id"] == *job && s["stage"] == name)
                        .collect::<Vec<_>>()
                };
                for reader in &enabled {
                    for (phase, count) in phases {
                        assert_eq!(
                            stage(phase)
                                .iter()
                                .filter(|s| s["reader_id"] == *reader)
                                .count(),
                            count,
                            "revision {revision}, subset {mask}, {job}, {reader}, {phase}"
                        );
                    }
                }
                let warmups = stage("warmup");
                let expected: Vec<_> = enabled
                    .iter()
                    .copied()
                    .chain(enabled.iter().rev().copied().take(if revision == 2 {
                        enabled.len()
                    } else {
                        0
                    }))
                    .collect();
                assert_eq!(
                    warmups
                        .iter()
                        .map(|s| text(&s["reader_id"]))
                        .collect::<Result<Vec<_>>>()?,
                    expected
                );
                let timing = stage("timing");
                assert!(
                    slots
                        .iter()
                        .rposition(|s| s["job_id"] == *job && s["stage"] == "warmup")
                        < slots
                            .iter()
                            .position(|s| s["job_id"] == *job && s["stage"] == "timing")
                );
                let mut positions = BTreeMap::new();
                for (repetition, round) in timing.chunks(enabled.len()).enumerate() {
                    let actual: BTreeSet<_> = round
                        .iter()
                        .map(|s| text(&s["reader_id"]))
                        .collect::<Result<_>>()?;
                    assert_eq!(actual, enabled.iter().copied().collect());
                    for (position, slot) in round.iter().enumerate() {
                        assert_eq!(slot["repetition"], repetition);
                        assert_eq!(slot["order"], position);
                        *positions
                            .entry((text(&slot["reader_id"])?, position))
                            .or_insert(0) += 1;
                    }
                }
                for reader in &enabled {
                    let counts: Vec<_> = (0..enabled.len())
                        .map(|position| *positions.get(&(*reader, position)).unwrap_or(&0))
                        .collect();
                    assert!(
                        counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1,
                        "unbalanced reader positions: {reader}: {counts:?}"
                    );
                }
                if revision == 2 {
                    let diagnostics: Vec<_> = slots
                        .iter()
                        .filter(|s| {
                            s["job_id"] == *job
                                && matches!(
                                    s["stage"].as_str(),
                                    Some("diagnostic" | "observer_baseline")
                                )
                        })
                        .collect();
                    for (index, pair) in diagnostics.chunks_exact(2).enumerate() {
                        let repetition = index / enabled.len();
                        let position = index % enabled.len();
                        let reader = enabled[if repetition == 0 {
                            position
                        } else {
                            enabled.len() - 1 - position
                        }];
                        for slot in pair {
                            assert_eq!(slot["reader_id"], reader);
                            assert_eq!(slot["repetition"], repetition);
                            assert_eq!(slot["order"], position);
                        }
                        assert_eq!(pair[0]["traced"], repetition == 1);
                        assert_eq!(pair[1]["traced"], repetition == 0);
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn observation(value: u64, reuse: bool) -> Value {
    let queries: Vec<_> = (0..if reuse { 2 } else { 1 })
        .map(|index| {
            json!({
                "query_index": index, "output_rows": 3, "output_batches": 1,
                "completion_ns": value, "first_batch_ns": 1, "first_batch_unavailable_reason": null,
            })
        })
        .collect();
    json!({
        "open_query_ns": if reuse { None } else { Some(value) },
        "initialization_ns": if reuse { Some(5) } else { None },
        "initialization_plus_query1_ns": if reuse { Some(value + 5) } else { None },
        "initialization_plus_all_queries_ns": if reuse { Some(2 * value + 5) } else { None },
        "session_elapsed_ns": if reuse { 2 * value + 5 } else { value }, "cleanup_ns": 1,
        "external_metrics": {"process_user_cpu_ns": 1, "process_system_cpu_ns": 1,
            "process_cpu_ns": 1, "peak_rss_bytes": 1, "process_elapsed_ns": 1},
        "queries": queries,
    })
}

#[test]
fn failed_or_missing_slots_cannot_improve_reported_timings() -> Result<()> {
    let jobs = inventory(6, 31);
    let inventory: Map<_, _> = jobs.iter().cloned().collect();
    let inventory = json!(inventory);
    let slots = schedule(
        &jobs,
        "reference",
        &json!({"comparison_revision": 6}),
        None,
        None,
        None,
    )?;
    let slots = array(&slots)?;
    let rows: Vec<_> = slots
        .iter()
        .map(|slot| {
            let mut row = slot.clone();
            let value = if slot["stage"] == "warmup" {
                900
            } else if slot["reader_id"] == "delta-arrow-reader" {
                100
            } else {
                400
            };
            row["status"] = json!("success");
            row["observation"] = observation(
                value,
                slot["job_id"].as_str().unwrap().starts_with("reuse."),
            );
            row
        })
        .collect();
    let report = summarize(&inventory, slots, &rows, 1.0, true)?;
    assert_eq!(
        report["wide.case"]["delta-arrow-reader"]["metrics"]["open_query_ns"]["median"],
        100.0
    );
    assert_eq!(
        report["wide.case"]["spark"]["speedup_vs_dar"]["open_query_ns"]["value"],
        4.0
    );
    assert_eq!(
        report["reuse.wide.case"]["spark"]["metrics"]["query_1.completion_ns"]["samples"],
        5
    );
    for stage in ["warmup", "timing"] {
        for status in ["missing", "timeout", "operational_failure"] {
            let mut changed = rows.clone();
            let index = changed
                .iter()
                .position(|row| row["stage"] == stage && row["reader_id"] == "spark")
                .unwrap();
            if status == "missing" {
                changed.remove(index);
            } else {
                changed[index]["status"] = json!(status);
            }
            let report = summarize(&inventory, slots, &changed, 1.0, true)?;
            let spark = &report["wide.case"]["spark"];
            assert_eq!(spark["eligible"], false);
            assert_eq!(spark["scheduled_samples"], 5);
            assert_eq!(spark["metrics"], json!({}));
            assert_eq!(
                spark["speedup_vs_dar"]["open_query_ns"]["value"],
                Value::Null
            );
        }
    }
    let below_resolution = summarize(&inventory, slots, &rows, 101.0, true)?;
    assert_eq!(
        below_resolution["wide.case"]["spark"]["speedup_vs_dar"]["open_query_ns"]["value"],
        Value::Null
    );
    assert_eq!(
        distribution(vec![1.0, 2.0, 3.0, 4.0])?,
        json!({"samples": 4, "median": 2.5, "q1": 1.75, "q3": 3.25, "iqr": 1.5})
    );
    assert!(distribution(vec![]).is_err());
    Ok(())
}

#[test]
fn incompatible_rosters_and_inconsistent_diagnostic_modes_are_rejected() -> Result<()> {
    let mut jobs = inventory(6, 31);
    let comparison = json!({"comparison_revision": 6});
    assert!(
        schedule(
            &jobs,
            "reference",
            &comparison,
            Some(true),
            Some(true),
            Some(true)
        )
        .is_err()
    );
    for (_, entries) in &mut jobs {
        entries["spark"]["gate_warmup"] = json!(true);
    }
    assert!(schedule(&jobs, "reference", &comparison, None, None, None).is_err());
    let jobs = inventory(2, 31);
    assert!(schedule(&jobs, "reference", &comparison, None, None, None).is_err());
    assert!(
        schedule(
            &jobs,
            "reference",
            &json!({"comparison_revision": true}),
            None,
            None,
            None
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn production_stages_keep_each_reader_sample_count_and_one_warmup() -> Result<()> {
    for revision in [5, 6] {
        for (stage, expected) in [("pilot", 2), ("formal", 5)] {
            let jobs = inventory(revision, 31);
            let slots = schedule(
                &jobs,
                "check",
                &json!({"comparison_revision":revision,"sampling_stage":stage}),
                None,
                None,
                None,
            )?;
            for (job, _) in &jobs {
                for reader in roster(&json!({"comparison_revision":revision}))? {
                    for (phase, count) in [("timing", expected), ("warmup", 1)] {
                        assert_eq!(
                            array(&slots)?
                                .iter()
                                .filter(|s| s["job_id"] == *job
                                    && s["reader_id"] == reader
                                    && s["stage"] == phase)
                                .count(),
                            count
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
