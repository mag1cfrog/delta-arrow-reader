use super::*;

#[test]
fn catalog_requires_sql_and_complete_native_descriptions() -> Result<()> {
    let harness = crate::repository::repository_root().join("benches/selective_read");
    let frozen = catalog(&harness)?;
    for (field, value) in [
        ("sql", json!(" ")),
        (
            "native_expressions",
            json!({"polars":{"projection":["id"]}}),
        ),
        (
            "native_expressions",
            json!({"polars":null,"daft":{"projection":["id"]}}),
        ),
    ] {
        let temporary = tempfile::tempdir()?;
        let mut changed = frozen.clone();
        changed["scales"]["0.01"]["queries"]["li"]["eq1"][field] = value;
        save(&temporary.path().join("query-matrix.json"), &changed)?;
        assert!(catalog(temporary.path()).is_err(), "{field}");
    }
    Ok(())
}

#[test]
fn oracle_selectivities_survive_json_round_trips_exactly() -> Result<()> {
    for number in [0.00048192771084337347f64, 0.019792272538429582] {
        let value: Value = serde_json::from_str(&number.to_string())?;
        assert_eq!(value.as_f64().unwrap().to_bits(), number.to_bits());
        let decoded: f64 = serde_json::from_str(&value.to_string())?;
        assert_eq!(decoded.to_bits(), number.to_bits());
    }
    Ok(())
}

fn manifest(frozen: &Value) -> Result<Value> {
    let source = &frozen["scales"]["0.01"];
    let queries = |family: &str| -> Result<Value> {
        Ok(json!(
            object(&source["queries"][family])?
                .iter()
                .map(|(name, query)| (name.clone(), query["sql"].clone()))
                .collect::<Map<_, _>>()
        ))
    };
    let tables = [
        "li.clustered",
        "li.shuffled",
        "wide.clustered",
        "wide.shuffled",
    ]
    .map(|id| json!({"id":id,"path":id,"scale_factor":0.01,"snapshot_version":0}));
    Ok(
        json!({"profile":"smoke","sources":[{"scale_factor":0.01,"rows":source["source_rows"],
        "in_literals":source["in_literals"],"queries":queries("li")?,"wide_queries":queries("wide")?}],
        "tables":tables}),
    )
}

#[test]
fn public_matrix_binds_all_shapes_literals_and_projection_pairs() -> Result<()> {
    let harness = crate::repository::repository_root().join("benches/selective_read");
    let frozen = catalog(&harness)?;
    let mut manifest = manifest(&frozen)?;
    assert_eq!(cases(&frozen)?.len(), 30);
    for layout in ["clustered", "shuffled"] {
        let narrow = format!("wide.{layout}.eq2-in20-keys");
        let wide = format!("wide.{layout}.eq2-in20");
        let a = shape(&narrow, &frozen)?;
        let b = shape(&wide, &frozen)?;
        assert_eq!(a["predicate"], b["predicate"]);
        assert_eq!(a["projection_columns"], 2);
        assert_eq!(b["projection_columns"], 69);
        assert_eq!(array(&a["predicate_only_columns"])?.len(), 3);
        assert_eq!(
            query(&narrow, &frozen, &manifest)?["fixture_path"],
            query(&wide, &frozen, &manifest)?["fixture_path"]
        );
        assert_eq!(
            array(&query(&format!("li.{layout}.eq2-in1"), &frozen, &manifest)?["in_literals"])?
                .len(),
            1
        );
    }
    manifest["sources"][0]["in_literals"] = json!([1]);
    assert!(query("li.clustered.eq2-in1", &frozen, &manifest).is_err());
    assert!(shape("wide.invalid.eq1", &frozen).is_err());
    Ok(())
}

#[test]
fn loaded_matrix_rejects_missing_cases_changed_references_counts_and_shapes() -> Result<()> {
    let harness = crate::repository::repository_root().join("benches/selective_read");
    let frozen = catalog(&harness)?;
    let manifest = manifest(&frozen)?;
    let temporary = tempfile::tempdir()?;
    let mut rows = Vec::new();
    for case in cases(&frozen)? {
        let reference = temporary.path().join(&case);
        fs::create_dir(&reference)?;
        let mut row = shape(&case, &frozen)?;
        row.as_object_mut()
            .unwrap()
            .extend(object(&query(&case, &frozen, &manifest)?)?.clone());
        let mut metadata = row.clone();
        metadata.as_object_mut().unwrap().extend(object(&json!({"format":"selective-read-reference-v2",
            "comparison_revision":2,"status":"complete","oracle_sha256":digest(&harness.join("oracle.py"))?,
            "oracle_dependencies_sha256":digest(&harness.join("oracle-requirements.txt"))?,
            "pyarrow":"25.0.1","duckdb_sort":"1.5.5","fixture_manifest_sha256":"fixture",
            "protocol_sha256":frozen["protocol_sha256"],"source_rows":100,"qualifying_rows":3,
            "output_rows":3,"qualifying_selectivity":0.03,"predicate_steps":[],
            "active_files":2,"candidate_files":["part"],"matching_files":["part"]}))?.clone());
        save(&reference.join("reference.json"), &metadata)?;
        row.as_object_mut().unwrap().extend(object(&json!({"status":"prepared","reference":reference,
            "reference_metadata_sha256":digest(&reference.join("reference.json"))?,"oracle":selected(&metadata, &ORACLE_FIELDS)?}))?.clone());
        rows.push(row);
    }
    let value = json!({"format":"selective-read-prepared-matrix-v1","status":"complete",
        "catalog_sha256":digest(&harness.join("query-matrix.json"))?,"protocol_sha256":frozen["protocol_sha256"],
        "fixture_manifest_sha256":"fixture","profile":"smoke","translation_locks":frozen["translation_locks"],"cases":rows});
    check_loaded(&value, &frozen, &manifest, "fixture")?;
    let mut audit = value.clone();
    audit["catalog_sha256"] = json!("reformatted catalog");
    check_loaded(&audit, &frozen, &manifest, "fixture")?;
    for (pointer, replacement) in [
        (
            "/cases/0/native_expressions/polars",
            json!({"projection":["other"]}),
        ),
        ("/cases/0/oracle/output_rows", json!(4)),
        ("/cases/0/canonical_sql", json!("SELECT 1")),
        ("/cases/0/reference_metadata_sha256", json!("changed")),
        ("/cases/0/projection", json!(["wrong"])),
        ("/cases/0/status", json!("unknown")),
    ] {
        let mut changed = value.clone();
        *changed.pointer_mut(pointer).ok_or("missing test field")? = replacement;
        assert!(
            check_loaded(&changed, &frozen, &manifest, "fixture").is_err(),
            "{pointer}"
        );
    }
    let mut changed = value.clone();
    changed["cases"].as_array_mut().unwrap().pop();
    assert!(check_loaded(&changed, &frozen, &manifest, "fixture").is_err());
    let mut changed = value.clone();
    changed["cases"][0].as_object_mut().unwrap().remove("limit");
    assert!(check_loaded(&changed, &frozen, &manifest, "fixture").is_err());
    let mut failed = value.clone();
    failed["cases"][0]["status"] = json!("preparation_failed");
    assert!(check_loaded(&failed, &frozen, &manifest, "fixture").is_err());
    failed["cases"][0]["failure_reason"] = json!("reader unavailable");
    check_loaded(&failed, &frozen, &manifest, "fixture")?;
    fs::write(
        temporary
            .path()
            .join(text(&value["cases"][0]["case_id"])?)
            .join("reference.json"),
        b"{}",
    )?;
    assert!(check_loaded(&value, &frozen, &manifest, "fixture").is_err());
    Ok(())
}
