use super::*;

#[test]
fn frozen_workloads_check_queries_and_scope_independently_of_source_provenance() -> Result<()> {
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
    row["fixtures"] = json!(root);
    row["fixture_manifest_sha256"] = json!(digest(&root.join("manifest.json"))?);
    let selected = BTreeSet::from([case.to_owned()]);
    let readers = campaign::roster(&json!({"comparison_revision":6}))?;
    let value = json!({"format":"selective-read-production-workload-v1","comparison_revision":6,"family":"production","definition_sha256":digest(&protocols.join("selective-read-production-shapes.md"))?,
        "definition_controller_sha256":digest(&std::env::current_exe()?)?,"source_sha256":sources(&harness)?,
        "cases":[row],"readers":readers,"sessions":sessions(&selected),"inventory":inventory(&selected,&readers),
        "scope":"probe","sampling_stage":"pilot","publication_ready":false,"oracle_limits":limits(),
        "translations":{"polars":{"lock_sha256":digest(&harness.join("runners/polars/lock.json"))?,
            "expressions":{case:{"native_expression":expression,"canonical_sql":row["canonical_sql"]}}}}});
    assert_eq!(load(value.clone(), &harness)?, value);
    assert_eq!(binding(&value, root, case, &harness)?, row);
    let path = root.join("workload.json");
    let resolve = || {
        super::super::run(serde_json::from_value(
            json!({"action":"resolve","path":path,
            "fixtures":root,"case":case,"harness":harness}),
        )?)
    };
    fs::write(&path, serde_json::to_vec(&value)?)?;
    let context = resolve()?;
    assert_eq!(context["workload"], value);
    assert_eq!(context["case"], row);
    assert_eq!(context["path"], json!(path.canonicalize()?));
    assert_eq!(
        context["identity"]["workload_contract"],
        crate::selective_read::contract::workload(&value)?
    );
    assert!(super::super::resolve(&path, root, "missing-case", &harness).is_err());
    for (pointer, replacement) in [
        ("/definition_sha256", json!("different documentation")),
        ("/definition_controller_sha256", json!("changed")),
        ("/source_sha256/oracle_io.py", json!("changed")),
    ] {
        let mut changed = value.clone();
        *changed.pointer_mut(pointer).ok_or("missing test field")? = replacement;
        fs::write(&path, serde_json::to_vec(&changed)?)?;
        let current = resolve()?;
        assert_eq!(current["workload"], changed);
        assert!(crate::selective_read::contract::same_comparison(
            &context["identity"],
            &current["identity"]
        ));
    }
    for (pointer, replacement) in [
        ("/format", json!("unsupported")),
        ("/cases/0/canonical_sql", json!("SELECT 1")),
        ("/oracle_limits/disk_bytes", json!(1)),
        ("/publication_ready", json!(true)),
        ("/sampling_stage", json!("formal")),
        ("/sessions", json!({})),
    ] {
        let mut changed = value.clone();
        *changed.pointer_mut(pointer).ok_or("missing test field")? = replacement;
        fs::write(&path, serde_json::to_vec(&changed)?)?;
        assert!(resolve().is_err(), "{pointer}");
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
    changed["translations"]["polars"]["expressions"][case]["canonical_sql"] =
        json!("SELECT other FROM bench");
    assert!(
        load(changed, &harness)
            .unwrap_err()
            .to_string()
            .contains("SQL binding")
    );
    fs::write(&path, serde_json::to_vec(&value)?)?;
    fs::write(root.join("manifest.json"), b"{}")?;
    assert!(
        resolve()
            .unwrap_err()
            .to_string()
            .contains("fixture identity")
    );
    Ok(())
}
