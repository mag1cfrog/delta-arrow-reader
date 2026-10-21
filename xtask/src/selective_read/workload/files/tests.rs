use super::*;

#[test]
fn repacked_boundaries_and_pruning_reject_lost_rows_or_skipping() -> Result<()> {
    let mut table = json!({"rows":10,"file_count":3,"files":[
        {"source_ordinal_range":[0,3],"rows":3,"delta_stats":{"numRecords":3},"row_groups":[{"rows":3}]},
        {"source_ordinal_range":[3,6],"rows":3,"delta_stats":{"numRecords":3},"row_groups":[{"rows":3}]},
        {"source_ordinal_range":[6,10],"rows":4,"delta_stats":{"numRecords":4},"row_groups":[{"rows":2},{"rows":2}]}]});
    geometry(&table, 3)?;
    assert!(geometry(&table, 4).is_err());
    table["files"][1]["source_ordinal_range"] = json!([4, 6]);
    assert!(geometry(&table, 3).is_err());
    table["files"][1]["source_ordinal_range"] = json!([3, 6]);
    table["files"][2]["row_groups"][0]["rows"] = json!(1);
    assert!(geometry(&table, 3).is_err());
    let good = json!({"case_id":"files4096.eq2-in20","output_rows":20,"active_files":4096,
        "candidate_files":["part-0"],"matching_files":["part-0"]});
    pruning(&good)?;
    for (field, value) in [
        ("candidate_files", json!([])),
        ("candidate_files", json!(vec!["part-0"; 4096])),
        ("output_rows", json!(0)),
    ] {
        let mut bad = good.clone();
        bad[field] = value;
        assert!(pruning(&bad).is_err());
    }
    let mut empty = good;
    empty["case_id"] = json!("files64.empty");
    assert!(
        pruning(&empty)
            .unwrap_err()
            .to_string()
            .contains("exclude every file")
    );
    empty["output_rows"] = json!(0);
    empty["candidate_files"] = json!([]);
    empty["matching_files"] = json!([]);
    pruning(&empty)?;
    Ok(())
}

#[test]
fn file_report_rejects_changed_fixtures_references_and_counts() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let fixtures = root.join("fixtures");
    fs::create_dir(&fixtures)?;
    fs::write(fixtures.join("manifest.json"), "{}")?;
    let mut rows = Vec::new();
    for case in cases() {
        let path = root.join(&case);
        fs::create_dir(&path)?;
        let empty = case.ends_with(".empty");
        let mut row = json!({"case_id":case,"active_files":4096,"output_rows":if empty {0} else {20},
            "candidate_files":if empty {vec![]} else {vec!["part-0"]},"matching_files":if empty {vec![]} else {vec!["part-0"]}});
        save(&path.join("reference.json"), &row)?;
        row["reference_metadata_sha256"] = json!(digest(&path.join("reference.json"))?);
        rows.push(row);
    }
    let checked = json!({"status":"complete","cases":rows,"fixture_manifest_sha256":digest(&fixtures.join("manifest.json"))?});
    let path = root.join("file-organizations.json");
    save(&path, &checked)?;
    assert_eq!(checked_references(root, &fixtures)?, checked);
    let mut changed = checked.clone();
    changed["cases"][0]["output_rows"] = json!(1);
    fs::write(&path, serde_json::to_vec(&changed)?)?;
    assert!(
        checked_references(root, &fixtures)
            .unwrap_err()
            .to_string()
            .contains("counts differ")
    );
    fs::write(&path, serde_json::to_vec(&checked)?)?;
    fs::write(root.join("files64.empty/reference.json"), "{}")?;
    assert!(
        checked_references(root, &fixtures)
            .unwrap_err()
            .to_string()
            .contains("metadata changed")
    );
    fs::write(fixtures.join("manifest.json"), "{\"changed\":true}")?;
    assert!(
        checked_references(root, &fixtures)
            .unwrap_err()
            .to_string()
            .contains("campaign fixtures")
    );
    Ok(())
}
