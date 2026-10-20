use std::fs;

use super::*;

#[test]
fn declared_families_keep_pilots_and_adjacent_scale_controls() -> Result<()> {
    let catalog = read_json(
        &crate::repository::repository_root().join("benches/selective_read/query-matrix.json"),
    )?;
    for (name, shape) in object(&shapes())? {
        if let Some(retained) = catalog["shapes"]["wide"].get(name) {
            assert_eq!(
                shape,
                &json!([
                    retained["projection"],
                    retained["predicate"],
                    retained["limit"]
                ]),
                "{name}"
            );
        }
    }
    assert_eq!(object(&definitions("data", None)?)?.len(), 18);
    assert_eq!(object(&definitions("files", None)?)?.len(), 4);
    assert_eq!(object(&definitions("production", None)?)?.len(), 8);
    assert_eq!(object(&definitions("pilot", None)?)?.len(), 1);
    for case in [
        "scale-control.wide.clustered.eq2-in20",
        "large.wide.clustered.date30-wide.dv",
        "unknown",
    ] {
        assert!(definitions("pilot", Some(case)).is_err());
    }
    assert!(definitions("files", Some("large.files4096.eq2-in20")).is_err());
    for (data, control) in [(10, 1), (30, 10), (100, 30), (300, 100)] {
        scales(
            &json!({"scope":"candidate","scales":{"data":data,"control":control}}),
            "data",
        )?;
        assert!(
            scales(
                &json!({"scope":"candidate","scales":{"data":data,"control":data}}),
                "data"
            )
            .is_err()
        );
    }
    scales(&json!({"scope":"pilot","scales":{"data":1}}), "pilot")?;
    assert!(scales(&json!({"scope":"candidate","scales":{"files":1}}), "files").is_err());
    Ok(())
}

#[test]
fn legacy_load_checks_limits_sessions_and_translations_without_current_sources() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let harness = temporary.path();
    for reader in ["polars", "daft"] {
        let path = harness.join("runners").join(reader);
        fs::create_dir_all(&path)?;
        fs::write(path.join("lock.json"), "{}")?;
    }
    for family in ["data", "files", "pilot"] {
        let expected = definitions(family, None)?;
        let expression_hash = production::expression_hash(&json!("unit"))?;
        let rows: Vec<_> = object(&expected)?.iter().map(|(case, definition)| {
            let shape = shapes()[definition[2].as_str().unwrap()].clone();
            json!({"case_id":case,"role":definition[0],"fixture_id":definition[1],"scale_factor":0.01,
                "projection":shape[0],"predicate":shape[1],"limit":shape[2],
                "native_expression_sha256":{"polars":expression_hash,"daft":expression_hash}})
        }).collect();
        let expressions = object(&expected)?
            .keys()
            .map(|case| {
                (
                    case.clone(),
                    json!({"native_expression":"unit","sha256":expression_hash}),
                )
            })
            .collect::<Map<_, _>>();
        let translations = ["polars","daft"].into_iter().map(|reader| Ok((reader.to_owned(),json!({
            "expressions":expressions,"lock_sha256":digest(&harness.join("runners").join(reader).join("lock.json"))?})))).collect::<Result<Map<_,_>>>()?;
        let scales = match family {
            "pilot" => json!({"data":0.01}),
            "files" => json!({"files":0.01}),
            _ => json!({"data":0.01,"control":0.01}),
        };
        let value = json!({"format":"selective-read-large-workload-v1","comparison_revision":4,"scope":"smoke","family":family,
            "publication_ready":false,"source_sha256":{"oracle.py":"old source"},"readers":campaign::roster(&json!({}))?,
            "sessions":sessions(family,&expected),"scales":scales,"cases":rows,"translations":translations,
            "oracle_limits":{"memory_bytes":16 * 1024u64.pow(3),"disk_bytes":1024 * 1024 * 1024,"elapsed_seconds":1800}});
        assert_eq!(load(value.clone(), harness)?, value);
        for (pointer, replacement) in [
            ("/sessions", json!({})),
            ("/cases/0/predicate", json!("all wrong")),
            ("/cases/0/native_expression_sha256/polars", json!("changed")),
            ("/oracle_limits/disk_bytes", json!(1)),
            ("/oracle_limits/elapsed_seconds", json!(2147483648u64)),
            ("/translations/daft/lock_sha256", json!("changed")),
            ("/translations/polars/expressions", json!({})),
        ] {
            let mut altered = value.clone();
            *altered.pointer_mut(pointer).ok_or("missing test field")? = replacement;
            assert!(load(altered, harness).is_err(), "{family}: {pointer}");
        }
    }
    Ok(())
}
