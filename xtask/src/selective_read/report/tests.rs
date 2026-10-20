use super::*;

fn fixture(root: &Path, revision: u64) -> Result<Request> {
    let protocols = Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/content/benchmarks");
    let protocol = if revision == 6 {
        "spark-matrix"
    } else {
        "production-workloads"
    };
    let identity = json!({"comparison_revision":revision, "sampling_stage":"formal", "workload_manifest_sha256":"1".repeat(64),
        "workload_contract":{"comparison_revision":revision,"sampling_stage":"formal","cases":[]},
        "protocol_sha256":digest(&protocols.join(format!("selective-read-{protocol}.md")))?,
        "base_protocol_sha256":digest(&protocols.join("selective-read-protocol.md"))?,
        "sampling_sha256":digest(&protocols.join("selective-read-sampling.md"))?});
    let readers = roster(&identity)?;
    let mut config = identity.clone();
    config.as_object_mut().unwrap().extend(object(&json!({
        "reader_builds":readers.iter().map(|reader| (reader.to_string(),json!({"reader_id":reader,
            "executable_sha256":"1".repeat(64),"lockfile_sha256":"2".repeat(64)}))).collect::<Map<_,_>>(),
        "server":{"build_sha256":"minio", "cpus":{"reader":[0,1,2,3,4,5,6,7]}, "topology":[],
            "kernel":"fixed", "cpu_info":"model name: check-cpu\ncpu MHz: 1000\n"},
        "cache_policy":"fresh clients; reused caches"
    }))?.clone());
    let mut cases = Vec::new();
    let mut campaigns = Vec::new();
    for shape in ["q2", "q4"] {
        for layout in ["localized", "scattered"] {
            for suffix in ["", ".dv"] {
                let case = format!("production.{shape}.{layout}{suffix}");
                let definition = json!({"case_id":case, "geometry":{"files":if shape=="q2" {130} else {60},
                    "stored_columns":if shape=="q2" {416} else {90}, "physical_bytes":1073741824},
                    "projection_columns":if shape=="q2" {69} else {71}});
                let path = root.join(&case);
                fs::create_dir(&path)?;
                save(&path.join("campaign.json"), &config)?;
                save(&path.join("frozen.json"), &json!({}))?;
                fs::write(path.join("observations.jsonl"), "")?;
                let mut rows = Vec::new();
                for mode in MODES {
                    for reader in readers {
                        let unsupported = reader == "daft" && suffix == ".dv";
                        let metrics = if unsupported {
                            json!({})
                        } else if mode == "open" {
                            json!({"open_query_ns":{"samples":5,"median":2e9,"q1":1e9,"q3":3e9}})
                        } else {
                            json!({"initialization_plus_query1_ns":{"samples":5,"median":4e9,"q1":3e9,"q3":5e9},
                                "query_1.completion_ns":{"samples":5,"median":0.5e9,"q1":0,"q3":1e9}})
                        };
                        rows.push(json!({"case":definition, "job":{"id":if mode=="open" {case.clone()} else {format!("reuse.{case}")},
                            "case_id":case,"execution_mode":mode}, "reader_id":reader,
                            "gate":{"status":if unsupported {"unsupported"} else {"success"}, "identity":{
                                "reader_build_sha256":reader, "reader_artifact":config["reader_builds"][reader]}},
                            "measurements":{"eligible":!unsupported,"scheduled_samples":if unsupported {0} else {5},"metrics":metrics},
                            "oracle":{"output_rows":895},"native_phases":{"planning_ns":null}}));
                    }
                }
                campaigns.push((
                    path,
                    json!({"campaign_id":case,"status":"complete","rows":rows,
                    "observations_sha256":"journal","summary_sha256":"summary"}),
                ));
                cases.push(definition);
            }
        }
    }
    let frozen = json!({"scope":"formal","inventory":cases.iter().map(|case| (case["case_id"].as_str().unwrap().to_owned(),json!({}))).collect::<Map<_,_>>(),"cases":cases});
    let definition = root.join("definition.json");
    save(&definition, &frozen)?;
    Ok(Request {
        definition,
        frozen,
        identity,
        campaigns,
        output: root.join("report"),
        protocols,
    })
}

fn aggregate(request: &Request) -> Result<Value> {
    let destination = tempfile::tempdir_in(request.output.parent().unwrap())?;
    let mut request = request.clone();
    request.output = destination.path().join("report");
    let result = run(request.clone())?;
    assert_eq!(
        read_json(&request.output.join("production-report.json"))?,
        result
    );
    let rendered = fs::read_to_string(request.output.join("README.md"))?;
    assert!(rendered.contains("median [Q1, Q3]"));
    if result["rows"][0]["status"] == "success" {
        assert!(rendered.contains("| success | 2.000 [1.000, 3.000] | success | 4.000 [3.000, 5.000] | 0.500 [0.000, 1.000] |"));
    }
    Ok(result)
}

#[test]
fn formal_coverage_keeps_missing_unsupported_and_incomplete_results_visible() -> Result<()> {
    for revision in [5, 6] {
        let temporary = tempfile::tempdir()?;
        let request = fixture(temporary.path(), revision)?;
        let mut partial = request.clone();
        partial.campaigns.truncate(1);
        let result = aggregate(&partial)?;
        let rows = array(&result["rows"])?;
        assert_eq!(rows.len(), 80);
        assert_eq!(array(&result["complete_cases"])?.len(), 1);
        assert_eq!(rows.iter().filter(|r| r["status"] == "not_run").count(), 70);
        assert_eq!(rows[0]["native_phases"], json!({"planning_ns":null}));
        let result = aggregate(&request)?;
        assert_eq!(result["formal_coverage_complete"], true);
        assert_eq!(result["publication_ready"], false);
        assert_eq!(
            array(&result["rows"])?
                .iter()
                .filter(|r| r["status"] == "unsupported")
                .count(),
            if revision == 5 { 8 } else { 0 }
        );
        let mut incomplete = request.clone();
        incomplete.campaigns[0].1["status"] = json!("incomplete");
        assert_eq!(aggregate(&incomplete)?["formal_coverage_complete"], false);
        let mut ineligible = request.clone();
        ineligible.campaigns[0].1["rows"][0]["measurements"]["eligible"] = json!(false);
        let result = aggregate(&ineligible)?;
        assert_eq!(result["formal_coverage_complete"], false);
        assert_eq!(result["rows"][0]["status"], "incomplete");
        let mut duplicate = partial.clone();
        duplicate.campaigns.push(partial.campaigns[0].clone());
        assert!(
            aggregate(&duplicate)
                .unwrap_err()
                .to_string()
                .contains("do not pool retries")
        );
    }
    Ok(())
}

#[test]
fn altered_cases_samples_builds_and_formal_conditions_are_rejected() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let request = fixture(temporary.path(), 6)?;
    for (pointer, value) in [
        ("/case/projection_columns", json!(1)),
        ("/measurements/scheduled_samples", json!(4)),
        ("/measurements/metrics/open_query_ns/samples", json!(4)),
        (
            "/gate/identity/reader_artifact/executable_sha256",
            json!("changed"),
        ),
        ("/gate/identity/reader_artifact", Value::Null),
    ] {
        let mut changed = request.clone();
        *changed.campaigns[0].1["rows"][0]
            .pointer_mut(pointer)
            .unwrap() = value;
        assert!(aggregate(&changed).is_err(), "{pointer}");
    }
    let config_path = request.campaigns[0].0.join("campaign.json");
    let original = read_json(&config_path)?;
    for (pointer, value) in [
        ("/sampling_stage", json!("pilot")),
        (
            "/reader_builds/spark/executable_sha256",
            json!("3".repeat(64)),
        ),
        ("/server/cpus/reader", json!([1])),
        ("/server/cpu_info", json!("model name: different")),
    ] {
        let mut changed = original.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        fs::write(&config_path, serde_json::to_vec(&changed)?)?;
        assert!(aggregate(&request).is_err(), "{pointer}");
    }
    let mut config = original.clone();
    config["reader_builds"]["spark"]["harness_git_commit"] = json!("different audit record");
    config["server"]["cpu_info"] = json!("cpu MHz: 3000\nmodel name: check-cpu\n");
    fs::write(&config_path, serde_json::to_vec(&config)?)?;
    assert_eq!(aggregate(&request)?["formal_coverage_complete"], true);
    let network = json!({"profile":{"latency_ms":200,"jitter_ms":20,"mbps":150,"seed":"fixed"},
        "cpu":9,"binary_sha256":"proxy","source_sha256":{"proxy":"source"},"byte_boundary":"body",
        "chunk_bytes":65536,"jitter":"deterministic","go_version":"pinned",
        "limits":{"MemoryMax":"536870912","MemorySwapMax":"0"}});
    config["server"]["network"] = network;
    let mut partial = request.clone();
    partial.campaigns.truncate(2);
    for (path, _) in &partial.campaigns {
        fs::write(path.join("campaign.json"), serde_json::to_vec(&config)?)?;
    }
    assert_eq!(array(&aggregate(&partial)?["complete_cases"])?.len(), 2);
    config["server"]["network"]["profile"]["mbps"] = json!(75);
    fs::write(&config_path, serde_json::to_vec(&config)?)?;
    assert!(aggregate(&partial).is_err());
    Ok(())
}

#[test]
fn successful_observations_require_effective_limits_and_full_watchdog_deadlines() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let mut request = fixture(temporary.path(), 6)?;
    request.campaigns.truncate(1);
    let journal = request.campaigns[0].0.join("observations.jsonl");
    let observation = json!({"request":{"resource_budget":budget()}, "observation":{
        "status":"success","external_resource_limits":{"cpu_affinity":[0,1,2,3,4,5,6,7],
            "process_memory_bytes":8589934592u64,"swap_bytes":0},
        "supervision":{"query_deadline_seconds":1800.0,"cleanup_deadline_seconds":60.0}}});
    fs::write(&journal, serde_json::to_vec(&observation)?)?;
    assert_eq!(array(&aggregate(&request)?["complete_cases"])?.len(), 1);
    for (pointer, value) in [
        ("/request/resource_budget/worker_threads", json!(1)),
        (
            "/observation/supervision/query_deadline_seconds",
            json!(600),
        ),
        (
            "/observation/external_resource_limits/cpu_affinity",
            json!([0]),
        ),
        (
            "/observation/external_resource_limits/process_memory_bytes",
            json!(1),
        ),
        ("/observation/external_resource_limits/swap_bytes", json!(1)),
    ] {
        let mut changed = observation.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        fs::write(&journal, serde_json::to_vec(&changed)?)?;
        assert!(aggregate(&request).is_err(), "{pointer}");
    }
    Ok(())
}
