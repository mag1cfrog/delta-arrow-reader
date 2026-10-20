use super::*;

fn fixture() -> Value {
    let fields: Vec<_> = ORIGINAL
        .into_iter()
        .map(str::to_owned)
        .chain(payloads())
        .map(|name| json!({"name":name}))
        .collect();
    let files: Vec<_> = (0..4096)
        .map(|i| {
            let bounds = json!({"l_shipdate":if i == 0 {"1995-03-15"} else {"1995-02-28"},
            "l_shipmode":"AIR","l_partkey":1});
            json!({"path":format!("part-{i:05}.parquet"),"bytes":64 * 1024u64.pow(2),
            "rows":1,"source_ordinal_range":[i,i+1],"row_groups":[{"rows":1}],
            "delta_stats":{"numRecords":1,"minValues":bounds,"maxValues":bounds,
                "nullCount":{"l_shipdate":0,"l_shipmode":0,"l_partkey":0}}})
        })
        .collect();
    let tables: Vec<_> = ["wide.clustered", "wide.files4096", "wide.clustered.dv", "wide.files4096.dv"]
        .into_iter().map(|id| json!({"id":id,"scale_factor":10,"rows":4096,"schema":{"fields":fields},
            "deletion_vectors":id.ends_with(".dv"),"snapshot_version":u64::from(id.ends_with(".dv")),
            "files":files,"file_count":4096})).collect();
    json!({"wide_file_pair":{"kind":"large"},"sources":[{"scale_factor":10,"rows":4096,"in_literals":[1]}],"tables":tables})
}

#[test]
fn wide_geometry_requires_matching_scales_snapshots_boundaries_and_large_file_sizes() -> Result<()>
{
    let manifest = fixture();
    let shape = geometry(&manifest, true, false)?;
    assert_eq!(shape["candidate_files"], json!(["part-00000.parquet"]));
    assert_eq!(array(&shape["excluded_files"])?.len(), 4095);
    assert_eq!(shape["median_file_bytes"], 64.0 * 1024.0f64.powi(2));
    for (pointer, value) in [
        ("/tables/0/scale_factor", json!(30)),
        ("/tables/0/snapshot_version", json!(1)),
        ("/tables/0/schema/fields/0/name", json!("wrong")),
        ("/tables/1/files/0/source_ordinal_range", json!([1, 2])),
        ("/tables/1/file_count", json!(4095)),
        ("/sources/0/scale_factor", json!(1)),
        ("/wide_file_pair/kind", json!("legacy")),
    ] {
        let mut changed = manifest.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        assert!(geometry(&changed, true, false).is_err(), "{pointer}");
    }
    let mut small = manifest.clone();
    for file in small["tables"][1]["files"].as_array_mut().unwrap() {
        file["bytes"] = json!(64 * 1024u64.pow(2) - 1);
    }
    assert!(
        geometry(&small, true, false)
            .unwrap_err()
            .to_string()
            .contains("64 MiB")
    );
    for table in small["tables"].as_array_mut().unwrap() {
        table["scale_factor"] = json!(0.01);
    }
    small["sources"][0]["scale_factor"] = json!(0.01);
    assert_eq!(
        geometry(&small, true, true)?["scope"],
        "smoke geometry; not large-data acceptance"
    );
    small["wide_file_pair"]["kind"] = json!("legacy");
    small["tables"].as_array_mut().unwrap().remove(2);
    geometry(&small, false, true)?;
    for table in small["tables"].as_array_mut().unwrap() {
        table["scale_factor"] = json!(1);
    }
    small["sources"][0]["scale_factor"] = json!(1);
    assert_eq!(
        geometry(&small, false, false)?["scope"],
        "SF1 mechanism control"
    );
    Ok(())
}

#[test]
fn wide_pruning_checks_inclusive_bounds_nulls_and_calendar_dates() -> Result<()> {
    let manifest = fixture();
    let stats = &manifest["tables"][1]["files"][0]["delta_stats"];
    assert!(candidate(stats, &[json!(1), json!(1)])?);
    assert!(!candidate(stats, &[json!(2)])?);
    let mut changed = stats.clone();
    changed["nullCount"]["l_shipmode"] = json!(1);
    assert!(!candidate(&changed, &[json!(1)])?);
    changed = stats.clone();
    changed["minValues"]["l_shipdate"] = json!("1995-03-16");
    changed["maxValues"]["l_shipdate"] = json!("1995-03-16");
    assert!(!candidate(&changed, &[json!(1)])?);
    for invalid in [
        "1995-02-29",
        "1900-02-29",
        "1995-00-01",
        "1995-13-01",
        "0000-01-01",
        "bad",
    ] {
        assert!(date(&json!(invalid)).is_err(), "{invalid}");
    }
    assert_eq!(date(&json!("2000-02-29"))?, "2000-02-29");
    Ok(())
}
