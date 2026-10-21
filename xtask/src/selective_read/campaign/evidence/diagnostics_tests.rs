use super::tests::{identity, protocols};
use super::*;
use crate::selective_read::campaign::{roster, tests::observation};

#[test]
fn diagnostic_modes_require_valid_dependencies_independently_of_documentation() -> Result<()> {
    let comparison = identity(6)?;
    for (mode, key, hash) in [
        (
            Mode::CombinedDiagnostics,
            "combined_diagnostics",
            "diagnostics_amendment_sha256",
        ),
        (Mode::GateWarmup, "gate_warmup", "warmup_amendment_sha256"),
        (
            Mode::GateDiagnostics,
            "gate_diagnostics",
            "gate_diagnostics_amendment_sha256",
        ),
    ] {
        assert!(!mode.enabled(&comparison, &protocols())?);
        let mut config = comparison.clone();
        config["combined_diagnostics"] = json!(true);
        config["gate_warmup"] = json!(true);
        config[key] = json!(true);
        assert!(mode.enabled(&config, &protocols())?);
        for (key, value) in [(key, json!(1)), ("comparison_revision", json!(5))] {
            let mut changed = config.clone();
            changed[key] = value;
            assert!(mode.enabled(&changed, &protocols()).is_err());
        }
        for provenance in [Value::Null, json!("older documentation")] {
            let mut changed = config.clone();
            changed[hash] = provenance;
            assert!(mode.enabled(&changed, Path::new("no-documents"))?);
            changed[key] = json!(false);
            assert!(!mode.enabled(&changed, Path::new("no-documents"))?);
        }
        if matches!(mode, Mode::GateDiagnostics) {
            for key in ["combined_diagnostics", "gate_warmup"] {
                let mut changed = config.clone();
                changed[key] = json!(false);
                assert!(mode.enabled(&changed, &protocols()).is_err());
            }
        }
    }
    Ok(())
}

#[test]
fn merged_diagnostics_preserve_timings_and_reject_missing_or_changed_evidence() -> Result<()> {
    let execution_contract = |reader: &str| {
        json!({"comparison_revision":2,
        "reader_id":reader,"reader_artifact":{"executable_sha256":"binary"},"reader_config":{"threads":8},
        "canonical_sql":"SELECT id FROM bench","native_expression":null,
        "fixture_manifest_sha256":"fixture","case_id":"case","snapshot_version":0})
    };
    for (combined, warmup, merged, count) in [
        (false, false, false, 80),
        (true, false, false, 72),
        (false, true, false, 70),
        (true, true, false, 62),
        (true, true, true, 52),
    ] {
        let root = tempfile::tempdir()?;
        let mut config = identity(6)?;
        config["campaign_id"] = json!("check");
        for (enabled, key) in [
            (combined, "combined_diagnostics"),
            (warmup, "gate_warmup"),
            (merged, "gate_diagnostics"),
        ] {
            config[key] = json!(enabled);
        }
        let readers = roster(&config)?;
        let inventory: Vec<(String,Value)> = ["production.q2.scattered","reuse.production.q2.scattered"].iter().map(|job| {
            (job.to_string(),Value::Object(readers.iter().map(|reader| (reader.to_string(),json!({
                "status":"success","runnable":true,"combined_diagnostics":combined,"gate_warmup":warmup,"gate_diagnostics":merged,
                "run_id":format!("{job}.gate.{reader}"),"identity":execution_contract(reader)
            }))).collect()))
        }).collect();
        let comparison = comparison(&config, &protocols())?;
        let slots = schedule(&inventory, "check", &comparison, None, None, None)?;
        let slots = array(&slots)?;
        assert_eq!(slots.len(), count);
        let baseline = schedule(
            &inventory,
            "check",
            &comparison,
            Some(false),
            Some(false),
            Some(false),
        )?;
        let timings = |slots: &[Value]| {
            slots
                .iter()
                .filter(|s| s["stage"] == "timing")
                .map(|slot| {
                    let mut slot = slot.clone();
                    slot.as_object_mut().unwrap().remove("run_id");
                    slot
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(timings(slots), timings(array(&baseline)?));
        assert_eq!(
            slots.iter().filter(|s| s["stage"] == "warmup").count(),
            if warmup { 0 } else { 10 }
        );
        let mut rows = Vec::new();
        for (job, entries) in &inventory {
            for (reader, gate) in object(entries)? {
                rows.push(json!({"run_id":gate["run_id"],"job_id":job,"reader_id":reader,"stage":"gate","traced":merged}));
            }
        }
        rows.extend_from_slice(slots);
        for row in &mut rows {
            let stage = text(&row["stage"])?;
            let diagnostic = purpose(row, combined);
            let mut record = observation(
                if stage == "gate" {
                    1_000_000_000_000
                } else {
                    100
                },
                text(&row["job_id"])?.starts_with("reuse."),
            );
            record.as_object_mut().unwrap().extend(object(&json!({
                "status":"success","identity":execution_contract(text(&row["reader_id"])?),
                "correctness":{"status":"passed"},"cleanup":{"status":"passed"},
                "validation_diagnostics":merged && stage=="gate",
                "storage_capture":{"status":"passed"},"storage_io":{"requests":2,"response_bytes":100},
                "storage_environment":{"trace_enabled":row["traced"]}
            }))?.clone());
            if diagnostic == "diagnostic"
                || merged && stage == "gate" && row["reader_id"] != "duckdb"
            {
                let destination = root.path().join(text(&row["run_id"])?);
                fs::create_dir_all(destination.join("reader"))?;
                for query in record["queries"].as_array_mut().unwrap() {
                    let name = format!("query-{}.plan", query["query_index"]);
                    fs::write(destination.join("reader").join(&name), "native plan\n")?;
                    query["physical_plan"] = json!(name);
                }
                record["plan_artifacts"] = plans(&record, &destination)?;
                row["artifacts"] = json!(destination);
            }
            row["request"] = json!({"purpose":diagnostic,"validation_diagnostics":merged && row["stage"]=="gate"});
            row["observation"] = record;
            row["status"] = json!("success");
        }
        diagnostics(&config, &inventory, slots, &rows, &protocols())?;
        let inventory_map = Value::Object(inventory.iter().cloned().collect());
        let summary = summarize(&inventory_map, slots, &rows, 1.0, true)?;
        for entries in object(&summary)?.values() {
            for entry in object(entries)?.values() {
                assert_eq!(entry["eligible"], true);
                assert_eq!(entry["scheduled_samples"], 5);
                assert_eq!(entry["metrics"]["process_elapsed_ns"]["samples"], 5);
                if merged {
                    assert_eq!(
                        entry["diagnostics"]["diagnostic"][0]["source"],
                        "exact-validation-gate"
                    );
                }
            }
        }
        if combined {
            assert!(
                diagnostics(
                    &config,
                    &inventory,
                    slots,
                    &rows[..rows.len() - 1],
                    &protocols()
                )
                .is_err()
            );
            let row = rows
                .iter()
                .find(|r| r["observation"].get("plan_artifacts").is_some())
                .unwrap();
            let path = text(&row["observation"]["plan_artifacts"][0]["path"])?;
            fs::write(path, "changed native plan\n")?;
            assert!(diagnostics(&config, &inventory, slots, &rows, &protocols()).is_err());
            fs::write(path, "native plan\n")?;
        }
        if warmup {
            for pointer in [
                "/observation/identity",
                "/observation/status",
                "/observation/cleanup",
                "/observation/correctness",
            ] {
                let mut changed = rows.clone();
                *changed[0].pointer_mut(pointer).unwrap() = Value::Null;
                assert!(
                    diagnostics(&config, &inventory, slots, &changed, &protocols()).is_err(),
                    "{pointer}"
                );
                assert_eq!(
                    summarize(&inventory_map, slots, &changed, 1.0, true)?[&inventory[0].0]
                        [readers[0]]["eligible"],
                    false
                );
            }
            assert!(diagnostics(&config, &inventory, slots, &rows[1..], &protocols()).is_err());
            assert!(
                schedule(
                    &inventory[..1],
                    "one-profile",
                    &comparison,
                    None,
                    None,
                    None
                )
                .is_err()
            );
        }
        if merged {
            for pointer in [
                "/observation/storage_capture",
                "/observation/storage_io",
                "/observation/plan_artifacts",
                "/traced",
                "/request/validation_diagnostics",
            ] {
                let mut changed = rows.clone();
                *changed[0].pointer_mut(pointer).unwrap() = Value::Null;
                assert!(
                    diagnostics(&config, &inventory, slots, &changed, &protocols()).is_err(),
                    "{pointer}"
                );
                assert_eq!(
                    summarize(&inventory_map, slots, &changed, 1.0, true)?[&inventory[0].0]
                        [readers[0]]["eligible"],
                    false
                );
            }
        }
    }
    Ok(())
}

#[test]
fn plan_artifacts_must_be_local_regular_nonempty_and_unchanged() -> Result<()> {
    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("reader"))?;
    let path = root.path().join("reader/query.plan");
    let mut record = json!({"queries":[{"query_index":0,"physical_plan":"query.plan"}]});
    assert!(plans(&record, root.path()).is_err());
    fs::write(&path, "")?;
    assert!(plans(&record, root.path()).is_err());
    fs::write(&path, "native plan")?;
    let first = plans(&record, root.path())?;
    fs::write(&path, "changed native plan")?;
    assert_ne!(first, plans(&record, root.path())?);
    for name in ["../query.plan", "/tmp/query.plan", ".", "..", ""] {
        record["queries"][0]["physical_plan"] = json!(name);
        assert!(plans(&record, root.path()).is_err());
    }
    record["queries"][0]["physical_plan"] = json!("query.plan");
    fs::remove_file(&path)?;
    std::os::unix::fs::symlink(protocols().join("selective-read-protocol.md"), &path)?;
    assert!(plans(&record, root.path()).is_err());
    Ok(())
}
