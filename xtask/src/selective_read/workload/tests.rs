use super::*;

#[test]
fn production_controls_keep_the_same_queries_columns_and_case_inventory() -> Result<()> {
    let baseline = shapes(512, 20000, 1048576, 1024)?;
    assert_eq!(baseline["q2"]["files"], 130);
    assert_eq!(baseline["q4"]["files"], 60);
    for target in [256, 512] {
        for rows in [2048, 20000] {
            for bytes in [8192, 65536, 1048576] {
                for batch in [128, 1024] {
                    let control = shapes(target, rows, bytes, batch)?;
                    for (name, count, columns) in [("q2", 69, 416), ("q4", 71, 90)] {
                        let shape = &control[name];
                        assert_eq!(shape["canonical_sql"], baseline[name]["canonical_sql"]);
                        assert_eq!(array(&shape["projection"])?.len(), count);
                        assert_eq!(shape["stored_columns"], columns);
                        assert_eq!(shape["row_group_rows"], 131072);
                        assert_eq!(shape["dictionary"], false);
                        assert_eq!(
                            shape["files"].as_u64().unwrap(),
                            baseline[name]["files"].as_u64().unwrap() * (512 / target)
                        );
                        assert_eq!(shape["data_page_rows"], rows);
                        assert_eq!(shape["data_page_bytes"], bytes);
                        assert_eq!(shape["write_batch_rows"], batch);
                        assert_eq!(
                            array(&shape["projection"])?
                                .iter()
                                .map(|v| text(v).unwrap())
                                .collect::<BTreeSet<_>>()
                                .len(),
                            count
                        );
                    }
                }
            }
        }
    }
    for options in [
        (128, 20000, 1048576, 1024),
        (512, 20000, 0, 1024),
        (512, 20000, 16384, 1024),
        (512, 20000, 1048576, 0),
    ] {
        assert!(shapes(options.0, options.1, options.2, options.3).is_err());
    }
    assert_eq!(object(&cases())?.len(), 8);
    Ok(())
}

#[test]
fn formal_sampling_requires_every_selected_full_pair() -> Result<()> {
    let all = cases();
    let bases: Vec<_> = object(&all)?
        .keys()
        .filter(|c| !c.ends_with(".dv"))
        .cloned()
        .collect();
    for mask in 1..16 {
        let selected: BTreeSet<_> = bases
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .flat_map(|(_, case)| [case.clone(), format!("{case}.dv")])
            .collect();
        sampling("formal", &selected, &BTreeSet::from(["generate".into()]))?;
        for missing in &selected {
            let mut changed = selected.clone();
            changed.remove(missing);
            assert!(sampling("formal", &changed, &BTreeSet::from(["generate".into()])).is_err());
        }
        assert!(sampling("formal", &selected, &BTreeSet::from(["probe".into()])).is_err());
    }
    for mode in ["probe", "generate"] {
        sampling(
            "pilot",
            &BTreeSet::from([bases[0].clone()]),
            &BTreeSet::from([mode.into()]),
        )?;
    }
    for (stage, selected, modes) in [
        (
            "formal",
            BTreeSet::new(),
            BTreeSet::from(["generate".into()]),
        ),
        (
            "formal",
            BTreeSet::from(["unknown".into(), "unknown.dv".into()]),
            BTreeSet::from(["generate".into()]),
        ),
        (
            "formal",
            object(&all)?.keys().cloned().collect(),
            BTreeSet::from(["probe".into(), "generate".into()]),
        ),
        (
            "formla",
            object(&all)?.keys().cloned().collect(),
            BTreeSet::from(["generate".into()]),
        ),
    ] {
        assert!(sampling(stage, &selected, &modes).is_err());
    }
    Ok(())
}

#[test]
fn production_metadata_binds_geometry_legacy_pages_and_unchanged_dv_data() -> Result<()> {
    let shape = shapes(512, 20000, 1048576, 1024)?["q4"].clone();
    let columns: Vec<_> = ORIGINAL
        .into_iter()
        .map(str::to_owned)
        .chain(payloads())
        .chain((0..10).map(|n| format!("metric_{n:03}")))
        .map(|name| json!({"name":name}))
        .collect();
    let base = json!({"id":"production.q4.localized", "path":"production.q4.localized", "deletion_vectors":false,
        "snapshot_version":0, "layout":"localized", "scale_factor":10.0, "file_target_mib":512,
        "schema":{"fields":columns}, "file_count":1, "files":[{"rows":10,"bytes":100,"row_groups":[{}],"delta_stats":{}}],
        "file_evidence":[{}], "rows":10, "bytes":100, "writer":shape});
    let mut manifest = json!({"status":"complete", "protocol":"selective-read-production-pairs-v1", "mode":"probe",
        "source_parent_manifest_sha256":"parent",
        "sources":[{"scale_factor":10.0,"rows":100}], "shape_definitions":{"q4":shape}, "writer":{"status":"complete","tables":[base]}});
    let case = "production.q4.localized";
    let expected = query(case, &manifest)?;
    assert_eq!(expected["writer"]["data_page_rows"], 20000);
    let mut legacy = manifest.clone();
    legacy["shape_definitions"]["q4"]
        .as_object_mut()
        .unwrap()
        .remove("data_page_bytes");
    assert_eq!(query(case, &legacy)?, expected);
    assert!(
        legacy["shape_definitions"]["q4"]
            .get("data_page_bytes")
            .is_none()
    );
    for bytes in [Value::Null, json!(8192), json!(65536)] {
        let mut changed = legacy.clone();
        changed["writer"]["tables"][0]["writer"]["data_page_bytes"] = bytes;
        assert!(
            query(case, &changed)
                .unwrap_err()
                .to_string()
                .contains("recorded 1 MiB")
        );
    }
    for key in [
        "data_page_rows",
        "data_page_bytes",
        "write_batch_rows",
        "row_group_rows",
        "dictionary",
    ] {
        let mut changed = manifest.clone();
        changed["writer"]["tables"][0]["writer"][key] = json!(123);
        assert!(
            query(case, &changed)
                .unwrap_err()
                .to_string()
                .contains("page/group")
        );
    }
    let mut changed = manifest.clone();
    changed["shape_definitions"]["q4"]["files"] = json!(61);
    assert!(
        query(case, &changed)
            .unwrap_err()
            .to_string()
            .contains("shape definition")
    );
    let mut dv = base.clone();
    dv["id"] = json!(format!("{case}.dv"));
    dv["path"] = dv["id"].clone();
    dv["snapshot_version"] = json!(1);
    dv["deletion_vectors"] = json!(true);
    dv["files"][0]["deletion_vector"] = json!({"path":"sidecar.bin"});
    dv["files"][0]["delta_stats"]["tightBounds"] = json!(false);
    manifest["writer"]["tables"]
        .as_array_mut()
        .unwrap()
        .push(dv);
    query(&format!("{case}.dv"), &manifest)?;
    manifest["writer"]["tables"][1]["files"][0]["path"] = json!("changed.parquet");
    assert!(
        query(&format!("{case}.dv"), &manifest)
            .unwrap_err()
            .to_string()
            .contains("DV pair changed")
    );
    Ok(())
}
