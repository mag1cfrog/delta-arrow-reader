use std::collections::BTreeMap;

use arrow::array::{Array, Int32Array, Int64Array};
use datafusion::common::ScalarValue;
use sha2::{Digest, Sha256};

use super::*;

#[test]
fn dv_payload_preserves_page_group_and_batch_boundaries() -> Result<()> {
    let ordinals = [
        0,
        127,
        128,
        4095,
        4096,
        8191,
        8192,
        131071,
        131072,
        1_u64 << 32,
    ];
    let (bytes, descriptor) = dv::payload(&ordinals)?;
    assert_eq!(dv::payload(&ordinals)?, (bytes.clone(), descriptor.clone()));
    assert_eq!(descriptor["offset"], 1);
    assert_eq!(descriptor["cardinality"], ordinals.len());
    assert_eq!(bytes[0], 1);
    assert_eq!(
        u32::from_be_bytes(bytes[1..5].try_into()?) as usize,
        bytes.len() - 9
    );
    assert_eq!(u32::from_le_bytes(bytes[5..9].try_into()?), 1681511377);
    let decoded = roaring::RoaringTreemap::deserialize_from(&bytes[9..bytes.len() - 4])?;
    assert_eq!(decoded.iter().collect::<Vec<_>>(), ordinals);
    for invalid in [&[][..], &[1, 1], &[2, 1]] {
        assert!(dv::payload(invalid).is_err());
    }
    assert!(dv::deleted_key(2581, 1));
    assert!(!dv::deleted_key(2580, 1));
    Ok(())
}

#[test]
fn within_file_controls_preserve_skip_levels_and_bytes() -> Result<()> {
    let root = tempfile::tempdir()?;
    let budget = Arc::new(Budget::new(1024 * MIB));
    for case in controls::CASES {
        // One full file exercises all group/page boundaries without writing 16 copies.
        let one = controls::table(&root.path().join("one"), case, 1, budget.clone())?;
        let two = controls::table(&root.path().join("two"), case, 1, budget.clone())?;
        assert_eq!(one, two);
        assert_eq!(one["file_count"], 1);
        let file = &one["files"][0];
        assert_eq!(file["delta_stats"]["minValues"]["event_id"], "match");
        assert_eq!(file["delta_stats"]["maxValues"]["event_id"], "other");
        let groups = file["row_groups"].as_array().ok_or("groups")?;
        assert_eq!(
            groups.len(),
            if case == controls::CASES[0] { 16 } else { 2 }
        );
        for (g, group) in groups.iter().enumerate() {
            assert_eq!(group["rows"], 4096);
            let columns = group["columns"].as_array().ok_or("columns")?;
            assert_eq!(columns.len(), 18);
            let stats = &columns[1]["statistics"];
            let low = if case != controls::CASES[0] || g == 7 {
                "match"
            } else {
                "other"
            };
            assert_eq!(stats["min_hex"], fixtures::hex(low.as_bytes()));
            assert_eq!(
                stats["max_hex"],
                fixtures::hex(if case == controls::CASES[0] {
                    low.as_bytes()
                } else {
                    b"other"
                })
            );
            if case != controls::CASES[0] {
                for column in columns {
                    let pages = column["pages"].as_array().ok_or("pages")?;
                    assert_eq!(pages.len(), 32);
                    for (p, page) in pages.iter().enumerate() {
                        assert_eq!(page["first_row"], p * 128);
                        assert_eq!(page["rows"], 128);
                    }
                }
                let pages = columns[1]["pages"].as_array().ok_or("pages")?;
                assert_eq!(
                    pages
                        .iter()
                        .filter(|p| p["bounds"]["min_hex"] == "6d61746368")
                        .count(),
                    if case == "pages.localized" { 1 } else { 32 }
                );
            }
        }
        let path = root
            .path()
            .join("one")
            .join(one["path"].as_str().ok_or("path")?)
            .join(file["path"].as_str().ok_or("path")?);
        let mut matched = 0;
        let mut ordinal = 0;
        for batch in fixtures::read_batches(&path)? {
            let batch = batch?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("ids")?;
            let events = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("events")?;
            for row in 0..batch.num_rows() {
                assert_eq!(ids.value(row), ordinal);
                let expected = match case {
                    "row-groups.select" => (28672..32768).contains(&ordinal),
                    "pages.localized" => {
                        (0..32).contains(&ordinal) || (4096..4128).contains(&ordinal)
                    }
                    _ => ordinal % 128 == 0,
                };
                assert_eq!(events.value(row) == "match", expected);
                matched += usize::from(expected);
                ordinal += 1;
            }
        }
        assert_eq!(matched, if case == controls::CASES[0] { 4096 } else { 64 });
        if case != "pages.scattered" {
            let first = dv::variant(
                &root.path().join("one"),
                &one,
                Profile::Smoke,
                false,
                budget.clone(),
            )?;
            let second = dv::variant(
                &root.path().join("two"),
                &two,
                Profile::Smoke,
                false,
                budget.clone(),
            )?;
            assert_eq!(first, second);
            let file = &first["files"][0];
            assert_eq!(file["sha256"], one["files"][0]["sha256"]);
            assert_eq!(file["delta_stats"]["numRecords"], one["rows"]);
            assert_eq!(file["delta_stats"]["tightBounds"], false);
            for id in file["deletion_vector"]["logical_ids"]
                .as_array()
                .ok_or("deleted IDs")?
            {
                let row = id[0].as_u64().ok_or("row ID")?;
                assert_eq!(row % 1000, 0);
                assert!(if case == controls::CASES[0] {
                    row / 4096 != 7
                } else {
                    row % 4096 >= 32
                });
            }
            let feature = dv::variant(
                &root.path().join("one"),
                &one,
                Profile::Smoke,
                true,
                budget.clone(),
            )?;
            assert_eq!(feature["deletion_summary"]["deleted_rows"], 0);
            assert_eq!(feature["files"], one["files"]);
        }
    }
    assert_eq!(control_rows::payload_value(0, 0), None);
    assert_eq!(control_rows::payload_value(16, 1), None);
    assert_eq!(
        control_rows::payload_value(1, 0),
        Some(format!(
            "payload-000-00000001-{}",
            "abcdefghijklmnopqrstuvwxyz0123456789".repeat(12)
        ))
    );
    Ok(())
}

#[test]
fn smoke_reproduces_files_and_preserves_rows() -> Result<()> {
    let root = tempfile::tempdir()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let first = Config {
        profile: Profile::Smoke,
        output: root.path().join("first"),
        sort_memory: 512 * MIB,
        disk_limit: Some(4 * 1024 * MIB),
        repack_from: None,
        controls: false,
        dv_from: Vec::new(),
    };
    let second = Config {
        output: root.path().join("second"),
        sort_memory: 16 * MIB,
        ..first.clone()
    };
    let one = runtime.block_on(generate(&first))?;
    let two = runtime.block_on(generate(&second))?;
    // Different directories and sort-memory limits must produce byte-identical inputs.
    assert_eq!(one["sources"], two["sources"]);
    assert_eq!(one["tables"], two["tables"]);
    let spilled_sorts = two["preparation"]["sort_operators"]
        .as_array()
        .ok_or("sort metrics")?;
    assert_eq!(spilled_sorts.len(), 2);
    assert!(
        spilled_sorts
            .iter()
            .all(|sort| sort["spill_count"].as_u64().is_some_and(|n| n > 0))
    );
    assert_eq!(one["tables"].as_array().ok_or("tables")?.len(), 4);
    assert_eq!(
        one["sources"][0]["queries"]
            .as_object()
            .ok_or("queries")?
            .len(),
        9
    );
    assert_eq!(
        one["sources"][0]["wide_queries"]
            .as_object()
            .ok_or("queries")?
            .len(),
        6
    );

    let mut expected = BTreeMap::new();
    let mut expected_literals = BTreeSet::new();
    for file in fixtures::parquet_files(&first.output.join("sf0.01/source"))? {
        for batch in fixtures::read_batches(&file)? {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let values = (0..16)
                    .map(|c| ScalarValue::try_from_array(batch.column(c), row))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let key = key(&batch, row)?;
                assert!(expected.insert(key, values.clone()).is_none());
                if values[10] == ScalarValue::Date32(Some(9204))
                    && values[14] == ScalarValue::Utf8(Some("AIR".into()))
                {
                    let ScalarValue::Int64(Some(part)) = values[1] else {
                        return Err("partkey".into());
                    };
                    expected_literals.insert(part);
                }
            }
        }
    }
    assert_eq!(expected.len(), 60175);
    assert_eq!(
        expected[&(1, 1)][4],
        ScalarValue::Decimal128(Some(1700), 15, 2)
    );
    assert_eq!(
        one["sources"][0]["in_literals"],
        json!(expected_literals.iter().take(20).collect::<Vec<_>>())
    );
    assert_eq!(fixtures::payload(1, 1, 0), Some(2558623671389681668));
    assert_eq!(fixtures::payload(1, 1, 2), Some(-11468591226400559));

    for table in one["tables"].as_array().ok_or("tables")? {
        let id = table["id"].as_str().ok_or("table id")?;
        let wide = id.starts_with("wide.");
        let clustered = id.ends_with(".clustered");
        let mut seen = BTreeSet::new();
        let mut clustered_order = None;
        let mut shuffled_order = None;
        let mut nulls = 0;
        for file in fixtures::parquet_files(&first.output.join(id))? {
            for batch in fixtures::read_batches(&file)? {
                let batch = batch?;
                assert_eq!(batch.num_columns(), if wide { 80 } else { 16 });
                for row in 0..batch.num_rows() {
                    let key = key(&batch, row)?;
                    assert!(seen.insert(key), "duplicate output key");
                    for (c, expected_value) in expected[&key].iter().enumerate() {
                        assert_eq!(
                            &ScalarValue::try_from_array(batch.column(c), row)?,
                            expected_value
                        );
                    }
                    if clustered {
                        let values = &expected[&key];
                        let next = (
                            values[10].clone(),
                            values[14].clone(),
                            values[1].clone(),
                            key,
                        );
                        if let Some(previous) = &clustered_order {
                            assert!(previous < &next);
                        }
                        clustered_order = Some(next);
                    } else {
                        let hash = Sha256::digest(
                            format!("dar-shuffle-v1/20260927/{}/{}", key.0, key.1).as_bytes(),
                        )
                        .to_vec();
                        let next = (hash, key);
                        if let Some(previous) = &shuffled_order {
                            assert!(previous < &next);
                        }
                        shuffled_order = Some(next);
                    }
                    if wide {
                        for j in 0..64 {
                            let array = batch
                                .column(j + 16)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or("payload type")?;
                            let digest = Sha256::digest(
                                format!("dar-wide-v1/{}/{}/{j:02}", key.0, key.1).as_bytes(),
                            );
                            let u = u64::from_le_bytes(digest[..8].try_into()?);
                            let want = if u % 17 == 0 {
                                None
                            } else {
                                Some((u & 0x7fffffffffffffff) as i64 - 0x4000000000000000)
                            };
                            let actual = (!array.is_null(row)).then(|| array.value(row));
                            assert_eq!(actual, want);
                            nulls += u64::from(actual.is_none());
                        }
                    }
                }
            }
        }
        assert_eq!(seen, expected.keys().copied().collect());
        if wide {
            assert!(nulls > 0);
        }
        let log = fs::read_to_string(
            first
                .output
                .join(id)
                .join("_delta_log/00000000000000000000.json"),
        )?;
        let actions = log
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(
            actions[0],
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
        );
        assert_eq!(
            actions.len() - 2,
            table["file_count"].as_u64().ok_or("file count")? as usize
        );
        for (action, file) in actions[2..]
            .iter()
            .zip(table["files"].as_array().ok_or("files")?)
        {
            let stats: Value =
                serde_json::from_str(action["add"]["stats"].as_str().ok_or("add stats")?)?;
            assert_eq!(stats, file["delta_stats"]);
            assert_eq!(
                stats["minValues"].as_object().ok_or("stats columns")?.len(),
                if wide { 80 } else { 16 }
            );
            assert!(action["add"].get("deletionVector").is_none());
        }
    }
    // Corrupt metadata with the same row count must fail the full-read validation.
    let table = &one["tables"][0];
    let file = &table["files"][0];
    let mut bad_stats = file["delta_stats"].clone();
    bad_stats["minValues"]["l_orderkey"] = json!(-1);
    assert!(
        fixtures::inspect_file(
            &first
                .output
                .join(table["path"].as_str().ok_or("path")?)
                .join(file["path"].as_str().ok_or("path")?),
            original_schema(),
            &bad_stats,
            fixtures::GROUP_ROWS,
            8
        )
        .is_err()
    );
    assert!(
        Config::parse(
            ["--output", first.output.to_str().ok_or("path")?]
                .map(str::to_owned)
                .into_iter()
        )
        .is_err()
    );
    assert!(
        Config::parse(
            ["--profile", "unknown", "--output", "unused"]
                .map(str::to_owned)
                .into_iter()
        )
        .is_err()
    );
    let quota = Arc::new(Budget::new(3));
    assert!(quota.write(&root.path().join("quota"), b"four").is_err());
    assert_eq!(fs::metadata(root.path().join("quota"))?.len(), 0);
    Ok(())
}

fn key(batch: &arrow::record_batch::RecordBatch, row: usize) -> Result<(i64, i32)> {
    Ok((
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("orderkey")?
            .value(row),
        batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("linenumber")?
            .value(row),
    ))
}

#[test]
fn repack_preserves_values_and_fractional_boundaries() -> Result<()> {
    let root = tempfile::tempdir()?;
    let budget = Arc::new(Budget::new(256 * MIB));
    let rows: Vec<_> = LineItemGenerator::new(0.01, 1, 1)
        .into_iter()
        .take(20003)
        .collect();
    let expected = fixtures::source_batch(&rows)?;
    let input = root.path().join("source");
    let mut writer = TableWriter::new(&input, original_schema(), budget.clone())?;
    writer.push(expected.clone())?;
    writer.finish(None)?;
    let paths = fixtures::parquet_files(&input)?;
    for files in [2, 64] {
        let mut previous = None;
        for copy in 0..2 {
            let output = root.path().join(format!("{files}-{copy}"));
            let writer = repack::table(
                &paths,
                &output,
                original_schema(),
                rows.len() as u64,
                files,
                budget.clone(),
            )?;
            let manifest = writer.finish(Some(("smoke", "repack-check")))?;
            assert_eq!(manifest["file_count"], files);
            for (i, file) in manifest["files"]
                .as_array()
                .ok_or("files")?
                .iter()
                .enumerate()
            {
                assert_eq!(
                    file["rows"],
                    (i + 1) * rows.len() / files as usize - i * rows.len() / files as usize
                );
            }
            let actual = fixtures::parquet_files(&output)?
                .iter()
                .map(|p| {
                    fixtures::read_batches(p)?
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(Into::into)
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(
                arrow::compute::concat_batches(&original_schema(), &actual)?,
                expected
            );
            if let Some(previous) = previous {
                assert_eq!(manifest, previous);
            }
            previous = Some(manifest);
        }
    }
    for (count, files) in [(20002, 64), (20004, 64), (10, 64), (20003, 0)] {
        assert!(
            repack::table(
                &paths,
                &root.path().join(format!("bad-{count}-{files}")),
                original_schema(),
                count,
                files,
                budget.clone()
            )
            .is_err()
        );
    }
    Ok(())
}
