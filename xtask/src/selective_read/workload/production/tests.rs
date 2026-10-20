use super::*;

#[test]
fn expression_hashes_match_every_frozen_adapter_description() -> Result<()> {
    let harness = crate::repository::repository_root().join("benches/selective_read");
    for line in fs::read_to_string(harness.join("native-expressions.jsonl"))?.lines() {
        let row: Value = serde_json::from_str(line)?;
        assert_eq!(row["sha256"], expression_hash(&row["native_expression"])?);
    }
    Ok(())
}

#[test]
fn frozen_workloads_reject_changed_sources_translations_scope_and_fixture_bindings() -> Result<()> {
    let harness =
        fs::canonicalize(crate::repository::repository_root().join("benches/selective_read"))?;
    let protocols = protocols(&harness)?;
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let shape = shapes(512, 20000, 1048576, 1024)?["q4"].clone();
    let fields: Vec<_> = ORIGINAL
        .into_iter()
        .map(str::to_owned)
        .chain(payloads())
        .chain((0..10).map(|n| format!("metric_{n:03}")))
        .map(|name| json!({"name":name}))
        .collect();
    let case = "production.q4.localized";
    let manifest = json!({"status":"complete","protocol":"selective-read-production-pairs-v1","mode":"probe",
        "contract_sha256":digest(&protocols.join("selective-read-production-shapes.md"))?, "source_parent_manifest_sha256":"parent",
        "sources":[{"scale_factor":10,"rows":100}],"shape_definitions":{"q4":shape},"writer":{"status":"complete","tables":[{
            "id":case,"path":case,"deletion_vectors":false,"snapshot_version":0,"layout":"localized","scale_factor":10,"file_target_mib":512,
            "schema":{"fields":fields},"file_count":1,"files":[{"rows":10,"bytes":100,"row_groups":[{}]}],
            "file_evidence":[{}],"rows":10,"bytes":100,"writer":shape}]}});
    save(&root.join("manifest.json"), &manifest)?;
    let mut row = query(case, &manifest, &protocols)?;
    let expression = json!({"projection":["l_orderkey"],"filter":null});
    let hash = expression_hash(&expression)?;
    row["fixtures"] = json!(root);
    row["fixture_manifest_sha256"] = json!(digest(&root.join("manifest.json"))?);
    row["native_expression_sha256"] = json!({"polars":hash});
    let selected = BTreeSet::from([case.to_owned()]);
    let readers = campaign::roster(&json!({"comparison_revision":6}))?;
    let value = json!({"comparison_revision":6,"family":"production","definition_sha256":digest(&protocols.join("selective-read-production-shapes.md"))?,
        "definition_controller_sha256":digest(&std::env::current_exe()?)?,"source_sha256":sources(&harness)?,
        "cases":[row],"readers":readers,"sessions":sessions(&selected),"inventory":inventory(&selected,&readers),
        "scope":"probe","sampling_stage":"pilot","publication_ready":false,"oracle_limits":limits(),
        "translations":{"polars":{"lock_sha256":digest(&harness.join("runners/polars/lock.json"))?,
            "expressions":{case:{"native_expression":expression,"sha256":hash}}}}});
    assert_eq!(load(value.clone(), &harness)?, value);
    assert_eq!(binding(&value, root, case, &harness)?, row);
    for (pointer, replacement) in [
        ("/definition_controller_sha256", json!("changed")),
        ("/source_sha256/oracle.py", json!("changed")),
        ("/cases/0/canonical_sql", json!("SELECT 1")),
        ("/cases/0/native_expression_sha256/polars", json!("changed")),
        ("/oracle_limits/disk_bytes", json!(1)),
        ("/publication_ready", json!(true)),
        ("/sampling_stage", json!("formal")),
        ("/sessions", json!({})),
    ] {
        let mut changed = value.clone();
        *changed.pointer_mut(pointer).ok_or("missing test field")? = replacement;
        assert!(load(changed, &harness).is_err(), "{pointer}");
    }
    let mut changed = value.clone();
    changed["cases"].as_array_mut().unwrap().push(row);
    assert!(
        load(changed, &harness)
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
    let mut changed = value.clone();
    changed["translations"]["polars"]["expressions"][case]["native_expression"]["filter"] =
        json!("changed");
    assert!(
        load(changed, &harness)
            .unwrap_err()
            .to_string()
            .contains("expression changed")
    );
    fs::write(root.join("manifest.json"), b"{}")?;
    assert!(
        binding(&value, root, case, &harness)
            .unwrap_err()
            .to_string()
            .contains("fixture identity")
    );
    Ok(())
}
