use super::*;

#[test]
fn reader_checks_reject_mixed_clocks_wrong_counts_and_incomplete_reuse() -> Result<()> {
    for reuse in [false, true] {
        let queries: Vec<_> = (0..if reuse { 10 } else { 1 })
            .map(|_| json!({"output_rows":3,"completion_ns":20,"first_batch_ns":5}))
            .collect();
        let record = json!({"status":"success","correctness":{"status":"passed"},"cleanup_ns":1,"queries":queries,
            "initialization_ns":10,"initialization_plus_all_queries_ns":210,"session_elapsed_ns":220,
            "initialization_plus_query1_ns":30,"open_query_ns":20});
        timing(&record, &json!(3), reuse)?;
        for (pointer, value) in [
            ("/queries/0/output_rows", json!(2)),
            ("/queries/0/completion_ns", json!(0)),
            ("/queries/0/first_batch_ns", json!(21)),
            ("/correctness/status", json!("failed")),
            ("/cleanup_ns", json!(-1)),
        ] {
            let mut changed = record.clone();
            *changed.pointer_mut(pointer).unwrap() = value;
            assert!(timing(&changed, &json!(3), reuse).is_err(), "{pointer}");
        }
        let mut changed = record.clone();
        changed["queries"].as_array_mut().unwrap().pop();
        assert!(timing(&changed, &json!(3), reuse).is_err());
        let mut empty = record.clone();
        for query in empty["queries"].as_array_mut().unwrap() {
            query["output_rows"] = json!(0);
            query["first_batch_ns"] = Value::Null;
            query["first_batch_unavailable_reason"] = json!("empty result");
        }
        timing(&empty, &json!(0), reuse)?;
        empty["queries"][0]["first_batch_ns"] = json!(5);
        assert!(timing(&empty, &json!(0), reuse).is_err());
    }
    Ok(())
}

#[test]
fn io_checks_require_complete_ordered_lifecycle_and_separate_evidence() -> Result<()> {
    for reuse in [false, true] {
        let queries: Vec<_> = (0..if reuse { 10 } else { 1 })
            .map(|_| json!({"physical_plan":null,"completion_ns":null}))
            .collect();
        let mut events = vec!["snapshot_open"];
        for _ in &queries {
            events.extend(["query_start", "stream_complete"]);
        }
        events.push("cleanup_complete");
        let events: Vec<_> = events
            .iter()
            .enumerate()
            .map(|(index, event)| json!({"event":event,"time_ns":index}))
            .collect();
        let record = json!({"status":"success","diagnostic_session_ns":100,"provider_evidence":null,"open_query_ns":null,
            "queries":queries,"diagnostic_events":events,"diagnostic_process":{"exited_ns":100}});
        io(&record, reuse)?;
        for (pointer, value) in [
            ("/diagnostic_events/1/event", json!("wrong")),
            ("/diagnostic_events/0/time_ns", json!(999)),
            ("/diagnostic_process/exited_ns", json!(1)),
            ("/queries/0/physical_plan", json!("plan.txt")),
            ("/provider_evidence", json!({})),
            ("/open_query_ns", json!(1)),
        ] {
            let mut changed = record.clone();
            *changed.pointer_mut(pointer).unwrap() = value;
            assert!(io(&changed, reuse).is_err(), "{pointer}");
        }
    }
    Ok(())
}
