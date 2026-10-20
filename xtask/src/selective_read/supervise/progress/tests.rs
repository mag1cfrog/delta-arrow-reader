use super::*;
use serde_json::json;

fn message(phase: &str, index: Option<u64>, timed: bool) -> Value {
    json!({
        "phase": phase, "query_index": index, "initialization_ns": 1,
        "query": if phase == "query_end" { json!({
            "query_index": index, "output_rows": 894, "output_batches": 2,
            "completion_ns": if timed { Some(10) } else { None },
            "first_batch_ns": if timed { Some(3) } else { None },
        }) } else { Value::Null },
    })
}

#[test]
fn concurrent_queries_share_one_deadline_and_finish_in_index_order() -> io::Result<()> {
    let payload = json!({"execution_mode": "reuse", "comparison_revision": 6, "purpose": "timing", "concurrent_queries": 4});
    let mut progress = Progress::new(&payload, 30.0, 2.0, 0)?;
    progress.receive(&message("initialization", None, true), 1)?;
    progress.receive(&message("concurrent_queries", None, true), 2)?;
    let deadline = progress.deadline;
    progress.receive(&message("query_end", Some(2), true), 3)?;
    assert_eq!(progress.deadline, deadline);
    assert_eq!(progress.phase, "concurrent_queries");
    assert!(
        progress
            .receive(&message("query_end", Some(2), true), 4)
            .is_err()
    );
    for index in [0, 3, 1] {
        progress.receive(&message("query_end", Some(index), true), 5)?;
    }
    assert_eq!(progress.phase, "cleanup");
    assert_eq!(
        progress
            .queries
            .iter()
            .map(|query| query["query_index"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    let deadline = progress.deadline;
    progress.receive(&message("cleanup", None, true), 10)?;
    assert_eq!(progress.deadline, deadline);
    for count in [json!(0), json!(1), json!(3), json!(true), json!("4")] {
        let mut invalid = payload.clone();
        invalid["concurrent_queries"] = count;
        assert!(Progress::new(&invalid, 30.0, 2.0, 0).is_err());
    }
    for (key, value) in [
        ("execution_mode", json!("open")),
        ("comparison_revision", json!(5)),
    ] {
        let mut invalid = payload.clone();
        invalid[key] = value;
        assert!(Progress::new(&invalid, 30.0, 2.0, 0).is_err());
    }
    Ok(())
}

#[test]
fn sequential_lifecycle_preserves_deadlines_and_rejects_invalid_clocks() -> io::Result<()> {
    for (mode, revision, count) in [
        ("open", 2, 1),
        ("reuse", 3, 10),
        ("reuse", 4, 2),
        ("reuse", 6, 2),
    ] {
        for timed in [false, true] {
            let payload = json!({"execution_mode": mode, "comparison_revision": revision, "purpose": if timed { "timing" } else { "validation" }});
            let mut progress = Progress::new(&payload, 30.0, 2.0, 0)?;
            assert_eq!(progress.count, count);
            assert!(progress.receive(&json!({"phase": "open"}), 0).is_err());
            progress.receive(
                &message(
                    if count == 1 { "open" } else { "initialization" },
                    None,
                    timed,
                ),
                1,
            )?;
            for index in 0..count as u64 {
                if count > 1 {
                    progress.receive(&message("query", Some(index), timed), 2)?;
                }
                for value in [json!(-1), json!(true), json!("3")] {
                    let mut invalid = message("query_end", Some(index), timed);
                    invalid["query"]["output_rows"] = value;
                    assert!(progress.receive(&invalid, 3).is_err());
                }
                let mut invalid = message("query_end", Some(index), timed);
                invalid["query"]["first_batch_ns"] = json!(99);
                assert!(progress.receive(&invalid, 3).is_err());
                progress.receive(&message("query_end", Some(index), timed), 3)?;
                assert_eq!(progress.deadline, 2_000_000_003);
            }
            assert_eq!(progress.phase, "cleanup");
            assert_eq!(progress.cleanup_started_ns, Some(3));
        }
    }
    let payload = json!({"execution_mode": "open", "purpose": "timing"});
    let mut progress = Progress::new(&payload, 1.0, 1.0, 0)?;
    progress.receive(&message("open", None, true), 0)?;
    let mut too_slow = message("query_end", Some(0), true);
    too_slow["query"]["completion_ns"] = json!(1_000_000_001u64);
    assert_eq!(
        progress.receive(&too_slow, 1).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    for duration in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        assert!(Progress::new(&payload, duration, 1.0, 0).is_err());
    }
    Ok(())
}
