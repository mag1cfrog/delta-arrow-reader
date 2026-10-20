use super::*;
use std::os::unix::fs::{PermissionsExt, symlink};

fn table(root: &Path, name: &str, size: u64) -> Result<Value> {
    let directory = root.join(name);
    fs::create_dir_all(&directory)?;
    fs::File::create_new(directory.join("part.parquet"))?.set_len(size)?;
    fs::write(directory.join("part.geometry.json"), "{}")?;
    fs::write(directory.join("delta.json"), "{}")?;
    Ok(json!({"id":name,"path":name,"deletion_vectors":false,
        "files":[{"path":"part.parquet","bytes":size,"geometry":{"path":"part.geometry.json","bytes":2}}],
        "delta_log":{"path":"delta.json","bytes":2}}))
}

#[test]
fn capacity_counts_shared_inodes_and_reserves_cross_device_copies() -> Result<()> {
    let root = tempfile::tempdir()?;
    let gib = 1024u64.pow(3);
    let mut roots = Vec::new();
    let mut transfers = Vec::new();
    for layout in ["localized", "scattered"] {
        let path = root.path().join(layout);
        let table = table(&path, layout, 66 * gib)?;
        roots.push(path.clone());
        transfers.push((path, table));
    }
    fs::hard_link(
        roots[0].join("localized/part.parquet"),
        roots[1].join("shared-input"),
    )?;
    let device = fs::metadata(root.path())?.dev();
    let phase = capacity(&roots, &transfers, device)?;
    assert!((132 * gib..133 * gib).contains(&phase.retained_input_bytes));
    phase.check(192 * gib, 400 * gib)?;
    assert_eq!(phase.copied_input_bytes, 0);
    assert_eq!(phase.additional_disk_bytes, phase.metadata_reserve_bytes);
    assert!(phase.immutable_hardlinks);
    assert!(phase.check(131 * gib, 400 * gib).is_err());
    assert!(
        phase
            .check(192 * gib, phase.additional_disk_bytes - 1)
            .is_err()
    );
    assert!(
        capacity(&roots, &transfers, device + 1)?
            .check(192 * gib, 400 * gib)
            .is_err()
    );
    for (root, table) in &mut transfers {
        fs::OpenOptions::new()
            .write(true)
            .open(root.join(text(&table["path"])?).join("part.parquet"))?
            .set_len(1024)?;
        table["files"][0]["bytes"] = json!(1024);
    }
    let copied = capacity(&roots, &transfers, device + 1)?;
    assert_eq!(copied.copied_input_bytes, 2 * (1024 + 2 + 2));
    assert_eq!(
        copied.additional_disk_bytes,
        copied.copied_input_bytes + copied.metadata_reserve_bytes
    );
    assert!(!copied.immutable_hardlinks);
    assert!(free_space(root.path())? > 0);
    Ok(())
}

#[test]
fn copying_verifies_hashes_contains_paths_and_never_overwrites() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = root.path().join("input");
    let mut table = table(&input, "table", 4)?;
    table["files"][0]["sha256"] = json!(digest(&input.join("table/part.parquet"))?);
    table["files"][0]["geometry"]["sha256"] =
        json!(digest(&input.join("table/part.geometry.json"))?);
    table["delta_log"]["sha256"] = json!(digest(&input.join("table/delta.json"))?);
    let output = root.path().join("output");
    copy(&input, &table, &output, &mut 0)?;
    assert_eq!(
        fs::metadata(input.join("table/part.parquet"))?.ino(),
        fs::metadata(output.join("table/part.parquet"))?.ino()
    );
    assert!(copy(&input, &table, &output, &mut 0).is_err());
    let fresh = root.path().join("fresh");
    table["files"][0]["sha256"] = json!("00".repeat(32));
    assert!(copy(&input, &table, &fresh, &mut 0).is_err());
    assert!(!fresh.exists());
    for path in ["../../outside", "/absolute"] {
        table["files"][0]["path"] = json!(path);
        assert!(copy(&input, &table, &fresh, &mut 0).is_err());
    }
    fs::write(root.path().join("outside"), b"abcd")?;
    symlink(root.path().join("outside"), input.join("table/link"))?;
    table["files"][0]["path"] = json!("link");
    assert!(copy(&input, &table, &fresh, &mut 0).is_err());
    assert_eq!(fs::read(output.join("table/part.parquet"))?, b"\0\0\0\0");
    Ok(())
}

fn fixture(root: &Path, layout: &str) -> Result<Fixture> {
    let path = root.join(layout);
    let mut base = table(&path, &format!("production.q4.{layout}"), 4)?;
    base["layout"] = json!(layout);
    base["files"][0]["sha256"] = json!(digest(
        &path.join(text(&base["path"])?).join("part.parquet")
    )?);
    base["files"][0]["geometry"]["sha256"] = json!(digest(
        &path.join(text(&base["path"])?).join("part.geometry.json")
    )?);
    base["delta_log"]["sha256"] =
        json!(digest(&path.join(text(&base["path"])?).join("delta.json"))?);
    let original = json!({"path":"source","files":[],"delta_log":null});
    let manifest = json!({"protocol":"selective-read-production-fixtures-v1","status":"complete","mode":"probe",
        "contract_sha256":"contract","source_parent_manifest_sha256":"source","shape":"q4",
        "shape_definition":{"canonical_sql":"SELECT id FROM bench"},"sources":[original],
        "writer":{"status":"complete","generator":{"executable_sha256":"source-writer"},"tables":[base]}});
    storage::save(&path.join("manifest.json"), &manifest)?;
    Ok(Fixture {
        manifest_sha256: digest(&path.join("manifest.json"))?,
        path,
    })
}

#[test]
fn pairing_requires_complete_inputs_and_preserves_writer_identity_and_layout() -> Result<()> {
    let root = tempfile::tempdir()?;
    let localized = fixture(root.path(), "localized")?;
    let scattered = fixture(root.path(), "scattered")?;
    let binary = root.path().join("writer");
    fs::write(
        &binary,
        r#"#!/bin/sh
set -eu
request=$(sha256sum "$2"); request=${request%% *}
binary=$(sha256sum "$0"); binary=${binary%% *}
sed -e "s/REQUEST_HASH/$request/g" -e "s/BINARY_HASH/$binary/g" "$(dirname "$0")/result.json" > "$3/dv-result.json"
echo writer output
"#,
    )?;
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o700))?;
    let result = json!({"status":"complete","request_sha256":"REQUEST_HASH",
        "generator":{"executable_sha256":"BINARY_HASH"},"base_tables":[{"id":"base"}],
        "tables":[{"id":"dv"}],"extra_logical_keys":[[1,1]]});
    storage::save(&root.path().join("result.json"), &result)?;
    let input = json!({"fixtures":[{"path":localized.path,"manifest_sha256":localized.manifest_sha256},
        {"path":scattered.path,"manifest_sha256":scattered.manifest_sha256}],
        "binary":binary,"output":root.path().join("paired"),"disk_gib":1,"seconds":10,
        "layout":null,"contract_sha256":"contract","driver_sha256":"driver"});
    let manifest = run(serde_json::from_value(input.clone())?)?;
    assert_eq!(
        manifest["production_dv"]["base_case_ids"],
        json!(["production.q4.localized", "production.q4.scattered"])
    );
    assert_eq!(
        manifest["writer"]["tables"],
        json!([{"id":"base"},{"id":"dv"}])
    );
    assert_eq!(
        manifest["writer"]["generator"]["executable_sha256"],
        digest(&binary)?
    );
    assert_eq!(
        manifest["production_dv"]["extra_logical_keys"],
        json!([[1, 1]])
    );
    assert_eq!(manifest["native_campaign_ready"], false);
    assert_eq!(manifest["publication_ready"], false);
    assert_eq!(
        fs::read_to_string(root.path().join("paired/writer.stdout.log"))?,
        "writer output\n"
    );
    assert_eq!(
        storage::read_json(&root.path().join("paired/dv-request.json"))?["tables"][0]["queries"],
        json!({"production.q4.localized":"SELECT id FROM bench"})
    );

    let mut staged = input.clone();
    staged["fixtures"] = json!([input["fixtures"][0]]);
    staged["output"] = json!(root.path().join("staged"));
    assert!(run(serde_json::from_value(staged.clone())?).is_err());
    assert!(!root.path().join("staged").exists());
    staged["layout"] = json!("localized");
    assert_eq!(
        run(serde_json::from_value(staged.clone())?)?["production_dv"]["layout"],
        "localized"
    );
    for (key, value) in [
        ("disk_gib", json!(0)),
        ("disk_gib", json!(193)),
        ("seconds", json!(0)),
        ("layout", json!("unknown")),
        ("contract_sha256", json!("wrong")),
    ] {
        let mut bad = input.clone();
        bad[key] = value;
        bad["output"] = json!(root.path().join("rejected"));
        assert!(run(serde_json::from_value(bad)?).is_err());
        assert!(!root.path().join("rejected").exists());
    }
    let mut bad = input.clone();
    bad["fixtures"][0]["manifest_sha256"] = json!("changed");
    assert!(run(serde_json::from_value(bad)?).is_err());
    let mut bad = input.clone();
    bad["fixtures"] = json!([input["fixtures"][0], input["fixtures"][0]]);
    assert!(run(serde_json::from_value(bad)?).is_err());
    let mut wrong = result;
    wrong["generator"]["executable_sha256"] = json!("wrong");
    fs::write(root.path().join("result.json"), serde_json::to_vec(&wrong)?)?;
    staged["output"] = json!(root.path().join("bad-writer"));
    assert!(run(serde_json::from_value(staged)?).is_err());
    assert!(!root.path().join("bad-writer/manifest.json").exists());
    Ok(())
}
