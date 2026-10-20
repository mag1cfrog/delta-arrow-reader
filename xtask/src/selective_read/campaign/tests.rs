use super::*;
use sha2::{Digest, Sha256};

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
fn all_reader_subsets_match_the_python_schedule() -> Result<()> {
    // SHA-256 of canonical schedules from the Python controller at 73f455f.
    for (revision, expected) in [
        (
            2,
            "9cd84fa9c0336146c1573dc3cd6d7d26fb37a93beb52680698eda0213c5682c3",
        ),
        (
            4,
            "8ce9a9eccec8bc50cdbf1f60b7973b636c013a0622f950476072eb70d857d07a",
        ),
        (
            6,
            "39c82c4c99c1510cd4d1550ff455752e2764cb7a0e8507523e1cd3694547ae7b",
        ),
    ] {
        let mut hash = Sha256::new();
        for mask in 0..32 {
            let slots = schedule(
                &inventory(revision, mask),
                "reference",
                &json!({"comparison_revision": revision}),
                None,
                None,
                None,
            )?;
            hash.update(serde_json::to_vec(&slots)?);
            hash.update(b"\n");
        }
        assert_eq!(
            format!("{:x}", hash.finalize()),
            expected,
            "revision {revision}"
        );
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
