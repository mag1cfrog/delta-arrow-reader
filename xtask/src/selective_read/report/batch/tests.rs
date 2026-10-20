use super::*;
use crate::selective_read::campaign::{self, Request as CampaignRequest};

fn write(path: &Path, value: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec(value)?)?;
    Ok(())
}

fn fixture(root: &Path, config: &Value, inventory: &Value, identity: &Value) -> Result<Vec<Value>> {
    let ordered = object(inventory)?
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    let slots = schedule(&ordered, "check", identity, None, None, None)?;
    let rows = array(&slots)?.iter().map(|slot| {
        let mut row = slot.clone();
        row["status"] = json!("success");
        row["artifacts"] = json!("plan,\"quoted\"");
        row["observation"] = json!({"open_query_ns":100, "initialization_ns":null, "initialization_plus_query1_ns":null,
            "initialization_plus_all_queries_ns":null, "session_elapsed_ns":100, "cleanup_ns":1,
            "external_metrics":{"process_user_cpu_ns":1,"process_system_cpu_ns":1,"process_cpu_ns":2,"peak_rss_bytes":1,"process_elapsed_ns":100},
            "queries":[{"query_index":0,"output_rows":3,"output_batches":1,"completion_ns":100,"first_batch_ns":1,"first_batch_unavailable_reason":null}],
            "diagnostic_events":[{"event":"snapshot_open","time_ns":5},{"event":"query_start","time_ns":12}]});
        if row["reader_id"] == "polars" && row["stage"] == "timing" && row["repetition"] == 0 {
            row["status"] = json!("timeout");
        }
        row
    }).collect::<Vec<_>>();
    let mut summary = identity.clone();
    summary.as_object_mut().unwrap().extend(object(&json!({"status":"incomplete","campaign_id":"check","integrity_passed":true,
        "timer_resolution":{"ratio_floor_ns":1},"jobs":campaign::run(CampaignRequest::Summarize {
            inventory:inventory.clone(),slots:array(&slots)?.to_vec(),rows:rows.clone(),resolution:1.0,integrity:true,
        })?}))?.clone());
    for (name, value) in [
        ("campaign.json", config),
        ("inventory.json", inventory),
        ("schedule.json", &slots),
        ("summary.json", &summary),
    ] {
        write(&root.join(name), value)?;
    }
    let frozen = ["campaign.json", "inventory.json", "schedule.json"]
        .into_iter()
        .map(|name| Ok((name.to_owned(), json!(digest(&root.join(name))?))))
        .collect::<Result<Map<_, _>>>()?;
    write(&root.join("frozen.json"), &json!(frozen))?;
    fs::write(
        root.join("observations.jsonl"),
        rows.iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    )?;
    Ok(rows)
}

#[test]
fn matrix_and_file_reports_keep_failed_slots_and_reject_changed_evidence() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let mut cases = Vec::new();
    let mut jobs = Vec::new();
    for files in [64, 4096] {
        for query in ["empty", "eq2-in20"] {
            let id = format!("files{files}.{query}");
            cases.push(json!({"case_id":id,"layout":"clustered","scale":"0.01","predicate_columns":["p,\"quoted\""],
                "projection_columns":2,"predicate_only_columns":["filter"],"active_files":files,
                "candidate_files":["part-0"],"matching_files":["part-0"],"reference_metadata_sha256":"metadata",
                "oracle":{"qualifying_rows":3,"output_rows":3,"qualifying_selectivity":0.5,"active_files":files,
                    "candidate_files":["part-0"],"matching_files":["part-0"]}}));
            jobs.push(json!({"id":id,"case_id":id,"execution_mode":"open"}));
        }
    }
    jobs.push(
        json!({"id":"reuse.files4096","case_id":"files4096.eq2-in20","execution_mode":"reuse"}),
    );
    let inventory = json!(jobs.iter().map(|job| Ok((text(&job["id"])?.to_owned(),
        json!(roster(&json!({}))?.into_iter().map(|reader| (reader.to_owned(),
            json!({"status":if reader=="daft" {"unsupported"} else {"success"}, "runnable":reader!="daft",
                "reference_sha256":"metadata"}))).collect::<Map<_,_>>())))).collect::<Result<Map<_,_>>>()?);
    write(&root.join("matrix.json"), &json!({}))?;
    let config = json!({"jobs":jobs,"matrix":{"path":root.join("matrix.json"),"sha256":digest(&root.join("matrix.json"))?}});
    let rows = fixture(root, &config, &inventory, &json!({}))?;
    let prepared = json!({"profile":"smoke","cases":cases});
    let request = |format, name| Request {
        campaign: root.into(),
        output: root.join(name),
        format,
    };
    let result = run(request(
        Format::Matrix {
            prepared: prepared.clone(),
        },
        "matrix",
    ))?;
    assert_eq!(result, json!({"status":"incomplete","entries":20}));
    let matrix = read_json(&root.join("matrix/matrix-report.json"))?;
    let failed = array(&matrix["rows"])?
        .iter()
        .find(|row| row["reader_id"] == "polars")
        .unwrap();
    assert_eq!(failed["timing"]["eligible"], false);
    assert_eq!(failed["timing"]["sample_statuses"]["timeout"], 1);
    assert!(failed["timing"]["speedup_vs_dar"]["open_query_ns"]["value"].is_null());
    assert_eq!(
        array(&matrix["rows"])?
            .iter()
            .filter(|row| row["gate"]["status"] == "unsupported")
            .count(),
        4
    );
    let records = csv::Reader::from_path(root.join("matrix/matrix-report.csv"))?
        .records()
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 20);
    assert_eq!(
        serde_json::from_str::<Value>(&records[0][5])?,
        cases[0]["predicate_columns"]
    );
    let checked = json!({"profile":"smoke","cases":cases,"tables":[]});
    write(&root.join("file-organizations.json"), &checked)?;
    let files = || Format::Files {
        references: root.into(),
        checked: checked.clone(),
    };
    assert_eq!(
        run(request(files(), "files"))?,
        json!({"status":"incomplete","entries":25})
    );
    let report = read_json(&root.join("files/file-organizations-report.json"))?;
    let successful = array(&report["rows"])?
        .iter()
        .find(|row| row["reader_id"] == "delta-arrow-reader")
        .unwrap();
    assert_eq!(
        successful["planning"]["plans"][0]["diagnostic_snapshot_open_ns"],
        7
    );
    assert!(
        run(request(files(), "files"))
            .unwrap_err()
            .to_string()
            .contains("already exists")
    );
    let mut bad = checked.clone();
    bad["cases"][0]["reference_metadata_sha256"] = json!("changed");
    assert!(
        run(request(
            Format::Files {
                references: root.into(),
                checked: bad
            },
            "bad-reference"
        ))
        .unwrap_err()
        .to_string()
        .contains("different reference")
    );
    let mut summary = read_json(&root.join("summary.json"))?;
    summary["jobs"]["files64.empty"]["polars"]["metrics"] = json!({"invented":{}});
    write(&root.join("summary.json"), &summary)?;
    assert!(
        run(request(files(), "bad-summary"))
            .unwrap_err()
            .to_string()
            .contains("raw observations")
    );
    fixture(root, &config, &inventory, &json!({}))?;
    write(&root.join("observations.jsonl"), &json!(rows))?;
    assert!(run(request(files(), "bad-journal")).is_err());
    Ok(())
}

#[test]
fn workload_reports_keep_unrunnable_readers_and_audit_queries_and_reference_identity() -> Result<()>
{
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let protocols = Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/content/benchmarks");
    for revision in [3, 4, 5, 6] {
        let directory = root.join(revision.to_string());
        fs::create_dir(&directory)?;
        let mut identity = json!({"comparison_revision":revision,"workload_manifest_sha256":"1".repeat(64),
            "protocol_sha256":digest(&protocols.join(format!("selective-read-{}.md",match revision {
                6=>"spark-matrix",5=>"production-workloads",_=>"large-workloads"})))?,
            "base_protocol_sha256":digest(&protocols.join("selective-read-protocol.md"))?});
        if revision >= 4 {
            identity["sampling_sha256"] =
                json!(digest(&protocols.join("selective-read-sampling.md"))?);
        }
        if revision >= 5 {
            identity["sampling_stage"] = json!("formal");
        }
        identity["workload_contract"] = json!({"comparison_revision":revision,
            "sampling_stage":identity["sampling_stage"], "cases":[]});
        let mut metadata = identity.clone();
        metadata["output_rows"] = json!(3);
        write(&directory.join("reference.json"), &metadata)?;
        let inventory = json!({"case":roster(&identity)?.into_iter().map(|reader| (reader.to_owned(),
            json!({"status":"unsupported","runnable":false,"reference":directory,"reference_sha256":digest(&directory.join("reference.json")).unwrap()})))
            .collect::<Map<_,_>>()});
        let mut config = identity.clone();
        config["campaign_id"] = json!("check");
        config["jobs"] = json!([{"id":"case","case_id":"case","execution_mode":"open"}]);
        fixture(&directory, &config, &inventory, &identity)?;
        let mut frozen = identity.clone();
        frozen["cases"] = json!([{"case_id":"case"}]);
        frozen["scope"] = json!("formal");
        frozen["inventory"] = json!({});
        let request = |name| Request {
            campaign: directory.clone(),
            output: directory.join(name),
            format: Format::Workload {
                frozen: frozen.clone(),
                identity: identity.clone(),
                protocols: protocols.clone(),
                bindings: Map::from_iter([(
                    "case".into(),
                    json!({"canonical_sql":"SELECT correct"}),
                )]),
            },
        };
        let result = run(request("report"))?;
        assert_eq!(array(&result["rows"])?.len(), 5);
        assert!(
            array(&result["rows"])?
                .iter()
                .all(|row| row["oracle"]["output_rows"] == 3
                    && row["gate"]["status"] == "unsupported")
        );
        assert_eq!(result.get("core_inventory").is_some(), revision >= 5);
        let mut row = identity.clone();
        row.as_object_mut().unwrap().extend(
            object(&json!({"run_id":"extra","stage":"gate","job_id":"case",
            "reader_id":"delta-arrow-reader","status":"unsupported","request":identity}))?
            .clone(),
        );
        row["request"]["canonical_sql"] = json!("SELECT changed");
        row["request"]["execution_mode"] = json!("open");
        fs::write(directory.join("observations.jsonl"), format!("{row}\n"))?;
        assert!(
            run(request("bad-query"))
                .unwrap_err()
                .to_string()
                .contains("observation query changed")
        );
        fs::write(directory.join("observations.jsonl"), "")?;
        metadata["output_rows"] = json!(4);
        write(&directory.join("reference.json"), &metadata)?;
        assert!(
            run(request("bad-reference"))
                .unwrap_err()
                .to_string()
                .contains("reference metadata changed")
        );
    }
    Ok(())
}
