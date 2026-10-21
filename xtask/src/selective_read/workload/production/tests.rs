use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn translation_bridge_binds_sql_to_sdk_expressions_and_rejects_stale_results() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let binary = root.join("adapter");
    fs::write(&binary, b"adapter")?;
    let adapter_hash = digest(&binary)?;
    save(
        &root.join("build.json"),
        &json!({"reader_id":"polars", "executable_sha256":adapter_hash,
        "lockfile_sha256":"lock", "runtime":{"polars":"pinned"}}),
    )?;
    let python = root.join("venv/bin/python");
    fs::create_dir_all(python.parent().unwrap())?;
    fs::write(
        &python,
        r#"#!/bin/sh
set -eu
test "$#" = 4
test "$1" = -I
test "$2" = -B
cat > "$(dirname "$0")/../../received.json"
cat "$(dirname "$0")/../../probe.json"
"#,
    )?;
    fs::set_permissions(&python, fs::Permissions::from_mode(0o700))?;
    let sql = json!({"all":"SELECT id FROM bench", "filtered":"SELECT id FROM bench WHERE id = 1"});
    let rows: Vec<_> = object(&sql)?
        .iter()
        .map(|(case, sql)| json!({"case_id":case,"canonical_sql":sql}))
        .collect();
    let probe = json!({"runtime":{"polars":"pinned"},"expressions":{
        "all":{"projection":["id"],"predicate":null},
        "filtered":{"projection":["id"],"predicate":"native id = 1"}}});
    save(&root.join("probe.json"), &probe)?;
    let invoke = |name: &str| {
        let output = root.join(name);
        fs::create_dir(&output)?;
        translate(&rows, std::slice::from_ref(&binary), &output, root)
    };
    let result = invoke("success")?;
    assert_eq!(read_json(&root.join("received.json"))?, sql);
    let translation = &result["polars"];
    assert_eq!(translation["reader"], "polars");
    assert_eq!(translation["lock_sha256"], "lock");
    assert!(translation.get("adapter_sha256").is_none());
    for (case, sql) in object(&sql)? {
        assert_eq!(translation["expressions"][case]["canonical_sql"], *sql);
        assert_eq!(
            translation["expressions"][case]["native_expression"],
            probe["expressions"][case]
        );
    }
    assert_eq!(
        read_json(&root.join("success/polars-translations.json"))?,
        *translation
    );
    for (name, pointer, replacement, message) in [
        (
            "wrong-runtime",
            "/runtime/polars",
            json!("changed"),
            "stale native runtime",
        ),
        (
            "missing-case",
            "/expressions",
            json!({}),
            "missing native translations",
        ),
        (
            "empty-expression",
            "/expressions/all",
            json!({}),
            "missing native translations",
        ),
    ] {
        let mut changed = probe.clone();
        *changed.pointer_mut(pointer).unwrap() = replacement;
        fs::write(root.join("probe.json"), serde_json::to_vec(&changed)?)?;
        assert!(invoke(name).unwrap_err().to_string().contains(message));
        assert!(!root.join(name).join("polars-translations.json").exists());
    }
    fs::write(&binary, b"changed adapter")?;
    assert!(
        invoke("changed-adapter")
            .unwrap_err()
            .to_string()
            .contains("stale adapter executable")
    );
    fs::write(&binary, b"adapter")?;
    fs::write(root.join("probe.json"), b"not JSON")?;
    assert!(invoke("failed-probe").is_err());
    assert!(!root.join("failed-probe/polars-translations.json").exists());
    Ok(())
}

#[test]
fn frozen_workloads_check_queries_and_scope_independently_of_source_provenance() -> Result<()> {
    let harness =
        fs::canonicalize(crate::repository::repository_root().join("benches/selective_read"))?;
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
        "source_parent_manifest_sha256":"parent",
        "sources":[{"scale_factor":10,"rows":100}],"shape_definitions":{"q4":shape},"writer":{"status":"complete","tables":[{
            "id":case,"path":case,"deletion_vectors":false,"snapshot_version":0,"layout":"localized","scale_factor":10,"file_target_mib":512,
            "schema":{"fields":fields},"file_count":1,"files":[{"rows":10,"bytes":100,"row_groups":[{}]}],
            "file_evidence":[{}],"rows":10,"bytes":100,"writer":shape}]}});
    save(&root.join("manifest.json"), &manifest)?;
    let mut row = query(case, &manifest)?;
    let expression = json!({"projection":["l_orderkey"],"filter":null});
    row["fixtures"] = json!(root);
    row["fixture_manifest_sha256"] = json!(digest(&root.join("manifest.json"))?);
    let selected = BTreeSet::from([case.to_owned()]);
    let readers = campaign::roster(&json!({"comparison_revision":6}))?;
    let value = json!({"format":"selective-read-production-workload-v1","comparison_revision":6,"family":"production",
        "cases":[row],"readers":readers,"sessions":sessions(&selected),"inventory":inventory(&selected,&readers)?,
        "scope":"probe","sampling_stage":"pilot","publication_ready":false,"oracle_limits":limits(),
        "translations":{"polars":{"lock_sha256":digest(&harness.join("runners/polars/lock.json"))?,
            "expressions":{case:{"native_expression":expression,"canonical_sql":row["canonical_sql"]}}}}});
    assert_eq!(load(value.clone(), &harness)?, value);
    assert_eq!(binding(&value, root, case)?, row);
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
    for (key, replacement) in [
        ("definition_sha256", json!("historical documentation")),
        (
            "definition_controller_sha256",
            json!("historical controller"),
        ),
        ("source_sha256", json!({"oracle_io.py":"historical source"})),
    ] {
        let mut changed = value.clone();
        changed[key] = replacement;
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
