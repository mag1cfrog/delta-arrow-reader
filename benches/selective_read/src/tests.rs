use std::collections::BTreeMap;

use arrow::array::{Array, Int32Array, Int64Array};
use datafusion::common::ScalarValue;
use sha2::{Digest, Sha256};

use super::*;

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
            &bad_stats
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
