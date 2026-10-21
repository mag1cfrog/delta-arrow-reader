use super::super::{
    rows::Cell,
    tests::{fixtures as fixture, save, state},
};
use super::*;
use crate::selective_read_build::digest;

#[test]
#[ignore = "requires the pinned PyArrow environment"]
fn production_pair_reuses_scans_but_rechecks_dv_keys_metrics_and_union_scope() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let mut state = state()?;
    let mut originals = Vec::new();
    let template = fixture::originals()?.remove(0);
    for n in 1..=600 {
        let mut row = template.clone();
        let line = if n <= 300 { 1 } else { 2 };
        row.key = [n, line];
        row.values[0] = Cell::Int(n);
        row.values[1] = Cell::Int(7);
        row.values[2] = Cell::Int(3);
        row.values[3] = Cell::Int(line);
        row.values[10] = rules::date(if n <= 400 { "1995-03-15" } else { "1995-03-16" })?;
        row.values[14] = Cell::Text(if n <= 350 { "AIR" } else { "SHIP" }.into());
        originals.push(row);
    }
    let input = Layout::new(&rules::original())?;
    let wide = Layout::new(&rules::wide())?;
    let mut source = fixture::group(root, "source", &input, &originals, false)?;
    source["scale_factor"] = json!(10);
    let stored = Layout::new(
        &rules::wide()
            .into_iter()
            .chain((0..336).map(|j| format!("metric_{j:03}")))
            .collect::<Vec<_>>(),
    )?;
    let mut records = Vec::new();
    for row in &originals {
        let mut row = rules::project(row, &input, &wide, true)?;
        row.values.extend((0..336).map(|j| {
            if (row.key[0] + row.key[1] + j) % 17 == 0 {
                Cell::Null
            } else {
                Cell::Int((7 + 3 * (j + 1) + row.key[1]) % 1024)
            }
        }));
        records.push(row);
    }
    let base = "production.q2.localized";
    let variant = format!("{base}.dv");
    fs::create_dir(root.join(base))?;
    fs::create_dir(root.join(&variant))?;
    let extras = BTreeSet::from([[1, 1], [301, 2], [351, 2]]);
    let mut files = Vec::new();
    let mut dvs = Vec::new();
    let mut evidence = Vec::new();
    for (index, rows) in [&records[..350], &records[350..]].into_iter().enumerate() {
        let name = format!("part-{index}.parquet");
        let path = root.join(base).join(&name);
        fixture::write(&path, &stored, rows)?;
        fs::hard_link(&path, root.join(&variant).join(&name))?;
        let stats = json!({"numRecords":rows.len(),"nullCount":{"l_shipdate":0,"l_shipmode":0,"l_linenumber":0},
            "minValues":{"l_shipdate":"1995-03-15","l_shipmode":if index==0 {"AIR"} else {"SHIP"},"l_linenumber":if index==0 {1} else {2}},
            "maxValues":{"l_shipdate":if index==0 {"1995-03-15"} else {"1995-03-16"},"l_shipmode":if index==0 {"AIR"} else {"SHIP"},"l_linenumber":2}});
        let file = json!({"path":name,"rows":rows.len(),"bytes":fs::metadata(&path)?.len(),"sha256":digest(&path)?,"delta_stats":stats});
        let deleted = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| rules::deleted(r.key) || extras.contains(&r.key))
            .collect::<Vec<_>>();
        let mut dv = file.clone();
        dv["deletion_vector"] = json!({"physical_ordinals":deleted.iter().map(|(i,_)|i).collect::<Vec<_>>(),"logical_ids":deleted.iter().map(|(_,r)|r.key).collect::<Vec<_>>()});
        files.push(file);
        dvs.push(dv);
        evidence.push(json!({"matching_ordinals":rows.iter().enumerate().filter(|(_,r)|r.key[0]<=300).map(|(i,_)|i).collect::<Vec<_>>()}));
    }
    let base_table = json!({"id":base,"path":base,"files":files,"file_evidence":evidence,"deletion_vectors":false,"snapshot_version":0,"rows":600,"file_count":2});
    let mut dv_table = base_table.clone();
    dv_table["id"] = json!(variant);
    dv_table["path"] = json!(variant);
    dv_table["files"] = json!(dvs);
    dv_table["deletion_vectors"] = json!(true);
    dv_table["snapshot_version"] = json!(1);
    let manifest = json!({"status":"complete","protocol":"selective-read-production-pairs-v1","mode":"probe","sources":[source],"writer":{"status":"complete","tables":[base_table,dv_table]},
        "production_dv":{"layout":"localized","base_case_ids":[base],"extra_logical_keys":extras}});
    save(&root.join("manifest.json"), &manifest)?;
    let projection = rules::wide69();
    let output = Layout::new(&projection)?;
    let context = json!({"identity":{"comparison_revision":6},"case":{"shape":"q2","projection":projection,"canonical_sql":rules::sql(&projection,"production",None,&[1])?}});
    for (case, reused) in [(base, false), (variant.as_str(), true)] {
        let spec = Spec::load(root, case, false, &context)?;
        let expected_extras = state.extra_keys(root, &spec)?;
        assert_eq!(expected_extras, extras);
        let directory = root.join(format!("reference-{reused}"));
        fs::create_dir(&directory)?;
        let reference = directory.join("reference.parquet");
        let population = state.expected(
            root,
            &spec,
            &extras,
            &spec.saved_keys()?,
            &reference,
            &output,
            None,
        )?;
        assert_eq!(population.source_reused, reused);
        let live = population.counts[3] - population.deleted_matches;
        let physical = state.physical(
            root, &spec, &extras, &output, &reference, live, reused, None,
        )?;
        assert_eq!(
            physical.scans["reused_full_column_scans"],
            if reused { 2 } else { 0 }
        );
        assert_eq!(physical.deleted as usize, spec.saved_keys()?.len());
        assert_eq!(live, if reused { 299 } else { 300 });
        if reused {
            let mut wrong = Spec::load(root, case, false, &context)?;
            wrong.table["files"][0]["deletion_vector"]["logical_ids"][0][0] = json!(9999);
            assert!(
                state
                    .physical(root, &wrong, &extras, &output, &reference, live, true, None)
                    .is_err()
            );
            let mut wrong = manifest.clone();
            wrong["production_dv"]
                .as_object_mut()
                .unwrap()
                .remove("layout");
            save(&root.join("manifest.json"), &wrong)?;
            assert!(
                state
                    .extra_keys(root, &Spec::load(root, case, false, &context)?)
                    .is_err()
            );
            wrong = manifest.clone();
            wrong["production_dv"]["extra_logical_keys"]
                .as_array_mut()
                .unwrap()
                .pop();
            save(&root.join("manifest.json"), &wrong)?;
            assert!(
                state
                    .extra_keys(root, &Spec::load(root, case, false, &context)?)
                    .is_err()
            );
            save(&root.join("manifest.json"), &manifest)?;
            for bad in [Cell::Null, Cell::Int(999)] {
                let mut changed = records[..350].to_vec();
                changed[0].values[80] = bad;
                fixture::write(&root.join(base).join("part-0.parquet"), &stored, &changed)?;
                assert!(
                    state
                        .physical(root, &spec, &extras, &output, &reference, live, true, None)
                        .is_err()
                );
            }
        }
    }
    Ok(())
}
