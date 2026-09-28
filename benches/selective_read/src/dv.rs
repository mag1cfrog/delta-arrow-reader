//! Pair the fixed public cases with immutable version-1 DV snapshots.

use super::*;
use std::collections::BTreeMap;
use std::fs::File;

use arrow::array::Int32Array;
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const TABLES: [&str; 5] = [
    "li.clustered",
    "li.shuffled",
    "wide.clustered",
    "row-groups",
    "pages.localized",
];

pub fn deleted_key(order: i64, line: i32) -> bool {
    let hash = Sha256::digest(format!("{order}/{line}").as_bytes());
    let mut first = [0; 8];
    first.copy_from_slice(&hash[..8]);
    u64::from_le_bytes(first).is_multiple_of(1000)
}

fn deletions(path: &Path, control: bool) -> Result<(Vec<u64>, Vec<Value>)> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let projection = ProjectionMask::roots(
        reader.parquet_schema(),
        if control { vec![0, 1] } else { vec![0, 3] },
    );
    let mut ordinals = Vec::new();
    let mut keys = Vec::new();
    let mut first = 0_u64;
    for batch in reader
        .with_projection(projection)
        .with_batch_size(BATCH_ROWS)
        .build()?
    {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            let key = if control {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("control row_id type")?;
                let events = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("control event_id type")?;
                (events.value(row) == "other" && ids.value(row) % 1000 == 0)
                    .then(|| json!([ids.value(row)]))
            } else {
                let orders = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("orderkey type")?;
                let lines = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("linenumber type")?;
                deleted_key(orders.value(row), lines.value(row))
                    .then(|| json!([orders.value(row), lines.value(row)]))
            };
            if let Some(key) = key {
                ordinals.push(first + row as u64);
                keys.push(key);
            }
        }
        first += batch.num_rows() as u64;
    }
    Ok((ordinals, keys))
}

pub fn payload(ordinals: &[u64]) -> Result<(Vec<u8>, Value)> {
    if ordinals.is_empty() || ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("DV ordinals must be nonempty, unique, and sorted".into());
    }
    let mut bytes = Vec::new();
    let mut writer = StreamingDeletionVectorWriter::new(&mut bytes);
    let mut bitmap = KernelDeletionVector::new();
    bitmap.add_deleted_row_indexes(ordinals.iter().copied());
    let result = writer.write_deletion_vector(bitmap)?;
    writer.finalize()?;
    Ok((
        bytes,
        json!({"storageType": "u", "offset": result.offset, "sizeInBytes": result.size_in_bytes,
                     "cardinality": result.cardinality}),
    ))
}

fn save_log(root: &Path, version: u64, actions: &[Value], budget: &Arc<Budget>) -> Result<Value> {
    let path = format!("_delta_log/{version:020}.json");
    let mut bytes = actions
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .join("\n");
    bytes.push('\n');
    budget.write(&root.join(&path), bytes.as_bytes())?;
    Ok(
        json!({"path": path, "bytes": bytes.len(), "sha256": fixtures::hash_bytes(bytes.as_bytes())}),
    )
}

pub fn variant(
    output: &Path,
    base: &Value,
    profile: Profile,
    feature_only: bool,
    budget: Arc<Budget>,
) -> Result<Value> {
    let base_id = base["id"].as_str().ok_or("base id")?;
    let suffix = if feature_only { "feature-only" } else { "dv" };
    let id = format!("{base_id}.{suffix}");
    let root = output.join(&id);
    fs::create_dir(&root)?;
    fs::create_dir(root.join("_delta_log"))?;
    let mut table = base.clone();
    table["id"] = json!(id);
    table["path"] = json!(id);
    table["base_fixture_id"] = json!(base_id);
    table["variant"] = json!(suffix);
    table["snapshot_version"] = json!(1);
    table["deletion_vectors"] = json!(!feature_only);
    table["dv_features"] = json!(true);
    let queries = base["queries"].as_object().ok_or("base queries")?;
    table["queries"] = Value::Object(
        queries
            .iter()
            .map(|(case, sql)| (format!("{case}.{suffix}"), sql.clone()))
            .collect(),
    );
    let mut initial = fs::read_to_string(
        output
            .join(base_id)
            .join("_delta_log/00000000000000000000.json"),
    )?
    .lines()
    .map(serde_json::from_str::<Value>)
    .collect::<std::result::Result<Vec<_>, _>>()?;
    let table_id = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!(
            "https://github.com/mag1cfrog/delta-arrow-reader/selective-read-v1/{}/{id}",
            profile.name()
        )
        .as_bytes(),
    );
    let metadata = initial
        .iter_mut()
        .find_map(|a| a.get_mut("metaData"))
        .ok_or("metadata missing")?;
    metadata["id"] = json!(table_id.to_string());
    let mut enabled = metadata.clone();
    enabled["configuration"]["delta.enableDeletionVectors"] = json!("true");
    let first_log = save_log(&root, 0, &initial, &budget)?;
    let mut actions = vec![
        json!({"protocol": {"minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]}}),
        json!({"metaData": enabled}),
    ];
    let mut deleted = 0_u64;
    let mut affected = 0;
    let control = base.get("scale_factor").is_none();
    for file in table["files"].as_array_mut().ok_or("files")? {
        let name = file["path"].as_str().ok_or("Parquet path")?.to_owned();
        budget.copy(&output.join(base_id).join(&name), &root.join(&name))?;
        if feature_only {
            continue;
        }
        let (ordinals, keys) = deletions(&root.join(&name), control)?;
        if ordinals.is_empty() {
            continue;
        }
        let uuid = Uuid::new_v5(&table_id, name.as_bytes());
        let dv_path = format!("deletion_vector_{uuid}.bin");
        let (bytes, mut descriptor) = payload(&ordinals)?;
        descriptor["pathOrInlineDv"] = json!(z85::encode(uuid.as_bytes()));
        budget.write(&root.join(&dv_path), &bytes)?;
        deleted += ordinals.len() as u64;
        affected += 1;
        file["delta_stats"]["tightBounds"] = json!(false);
        actions.push(
            json!({"remove": {"path": name, "deletionTimestamp": 0, "dataChange": true,
            "extendedFileMetadata": true, "partitionValues": {}, "size": file["bytes"]}}),
        );
        actions.push(json!({"add": {"path": name, "partitionValues": {}, "size": file["bytes"],
            "modificationTime": 0, "dataChange": true, "stats": serde_json::to_string(&file["delta_stats"])?,
            "deletionVector": descriptor}}));
        file["deletion_vector"] = json!({"path": dv_path, "bytes": bytes.len(), "sha256": fixtures::hash_bytes(&bytes),
            "descriptor": descriptor, "physical_ordinals": ordinals, "logical_ids": keys});
    }
    repack::verified_files(&root, &table)?;
    let last_log = save_log(&root, 1, &actions, &budget)?;
    table["delta_log"] = last_log.clone();
    table["delta_logs"] = json!([first_log, last_log]);
    let rows = table["rows"].as_u64().ok_or("rows")?;
    table["deletion_summary"] = json!({"physical_rows": rows, "deleted_rows": deleted, "live_rows": rows - deleted,
        "density": deleted as f64 / rows as f64, "dv_files": affected,
        "file_coverage": affected as f64 / table["file_count"].as_u64().ok_or("file count")? as f64,
        "rule": if feature_only { "none" } else if control { "event_id = 'other' AND row_id % 1000 = 0" }
            else { "SHA256(UTF8(l_orderkey/l_linenumber))[0:8] as unsigned LE u64 % 1000 = 0" }});
    Ok(table)
}

pub fn generate(config: &Config) -> Result<Value> {
    let started = Instant::now();
    let mut parents = Vec::new();
    let mut tables = BTreeMap::new();
    let mut sources = BTreeMap::new();
    for input in &config.dv_from {
        let bytes = fs::read(input.join("manifest.json"))?;
        let parent: Value = serde_json::from_slice(&bytes)?;
        if parent["status"] != "complete"
            || parent["protocol"] != "selective-read-v1"
            || parent["generator"]["lockfile_sha256"]
                != fixtures::hash_file(&input.join("generator-Cargo.lock"))?
            || (!parent["sources"].as_array().ok_or("sources")?.is_empty()
                && parent["profile"] != config.profile.name())
        {
            return Err("incompatible DV parent/profile".into());
        }
        for source in parent["sources"].as_array().ok_or("sources")? {
            let path = source["path"].as_str().ok_or("source path")?;
            if path
                != format!(
                    "sf{}/source",
                    source["scale_factor"].as_f64().ok_or("source scale")?
                )
                || sources
                    .insert(path.to_owned(), (input, source.clone()))
                    .is_some()
            {
                return Err("invalid or duplicate source path".into());
            }
        }
        for table in parent["tables"].as_array().ok_or("tables")? {
            let id = table["id"].as_str().ok_or("id")?;
            if TABLES.contains(&id)
                && tables
                    .insert(id.to_owned(), (input, table.clone()))
                    .is_some()
            {
                return Err("duplicate base table".into());
            }
        }
        parents.push(
            json!({"manifest_sha256": fixtures::hash_bytes(&bytes), "profile": parent["profile"],
                "protocol_sha256": parent["protocol_sha256"], "generator": parent["generator"]}),
        );
    }
    if tables.len() != TABLES.len() {
        return Err(
            "DV preparation needs public fixtures plus row-group/localized-page controls".into(),
        );
    }
    let parent = config
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let available = fixtures::available_disk(parent)?;
    let disk_limit = config
        .disk_limit
        .unwrap_or(config.profile.disk_bytes())
        .min(available.saturating_sub(512 * MIB));
    let budget = Arc::new(Budget::new(disk_limit / 2));
    fs::create_dir(&config.output)?;
    for (path, (input, source)) in &sources {
        fs::create_dir_all(config.output.join(path))?;
        for file in repack::verified_files(&input.join(path), source)? {
            budget.copy(
                &file,
                &config
                    .output
                    .join(path)
                    .join(file.file_name().ok_or("filename")?),
            )?;
        }
        repack::verified_files(&config.output.join(path), source)?;
    }
    let mut prepared = Vec::new();
    for id in TABLES {
        eprintln!("preparing {id} and DV pair");
        let (input, mut table) = tables.remove(id).ok_or("base table")?;
        if table["path"] != id
            || table["snapshot_version"] != 0
            || table["deletion_vectors"] != false
            || table["delta_log"]["path"] != "_delta_log/00000000000000000000.json"
        {
            return Err("expected immutable no-DV snapshot 0".into());
        }
        let files = table["files"].as_array().ok_or("files")?;
        let rows = files
            .iter()
            .map(|file| file["rows"].as_u64().ok_or("file rows"))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();
        if table["file_count"] != files.len() || table["rows"] != rows {
            return Err("base row or file inventory differs".into());
        }
        fs::create_dir_all(config.output.join(id).join("_delta_log"))?;
        for file in repack::verified_files(&input.join(id), &table)? {
            budget.copy(
                &file,
                &config
                    .output
                    .join(id)
                    .join(file.file_name().ok_or("filename")?),
            )?;
        }
        let log = table["delta_log"]["path"].as_str().ok_or("log path")?;
        if table["delta_log"]["sha256"] != fixtures::hash_file(&input.join(id).join(log))? {
            return Err("parent Delta log changed".into());
        }
        budget.copy(&input.join(id).join(log), &config.output.join(id).join(log))?;
        if table.get("scale_factor").is_some() {
            let source = sources
                .values()
                .find(|(_, s)| s["scale_factor"] == table["scale_factor"])
                .ok_or("source scale")?;
            let cases: &[&str] = match id {
                "li.clustered" => &["date7-full", "date7-limit"],
                "li.shuffled" => &["date7-full"],
                _ => &["eq2-in20"],
            };
            let queries = &source.1[if id.starts_with("wide.") {
                "wide_queries"
            } else {
                "queries"
            }];
            table["queries"] = Value::Object(
                cases
                    .iter()
                    .map(|case| (format!("{id}.{case}"), queries[case].clone()))
                    .collect(),
            );
        }
        repack::verified_files(&config.output.join(id), &table)?;
        let paired = variant(
            &config.output,
            &table,
            config.profile,
            false,
            budget.clone(),
        )?;
        if id == "row-groups" {
            prepared.push(variant(
                &config.output,
                &table,
                config.profile,
                true,
                budget.clone(),
            )?);
        }
        prepared.extend([table, paired]);
    }
    budget.write(
        &config.output.join("generator-Cargo.lock"),
        LOCKFILE.as_bytes(),
    )?;
    let manifest = json!({"protocol": "selective-read-v1", "protocol_sha256": fixtures::hash_bytes(PROTOCOL.as_bytes()),
        "profile": config.profile.name(), "status": "complete", "generator": generator_identity()?,
        "dv_writer": "delta_kernel 0.25.0 StreamingDeletionVectorWriter portable Roaring", "parents": parents,
        "sources": sources.values().map(|(_, s)| s).collect::<Vec<_>>(), "tables": prepared,
        "preparation": {"memory_limit_bytes": config.profile.memory_bytes(), "disk_limit_bytes": disk_limit,
            "output_limit_bytes": disk_limit / 2, "free_disk_before_bytes": available,
            "peak_rss_bytes": fixtures::peak_rss()?, "elapsed_ms": started.elapsed().as_millis()}});
    budget.write(
        &config.output.join("manifest.json"),
        &serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}
