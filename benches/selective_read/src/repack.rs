//! Reuse ordered public rows, changing only their file boundaries and resulting geometry.

use super::*;
use arrow::datatypes::SchemaRef;

pub fn table(
    input: &[PathBuf],
    output: &Path,
    schema: SchemaRef,
    rows: u64,
    files: u64,
    budget: Arc<Budget>,
    geometry_sidecars: bool,
) -> Result<TableWriter> {
    if files == 0 || rows < files || rows.div_ceil(files) > fixtures::FILE_ROWS as u64 {
        return Err("repacked files must contain 1..=1048576 rows".into());
    }
    rows.checked_mul(files).ok_or("file boundary overflow")?;
    let mut writer =
        TableWriter::new(output, schema, budget)?.with_geometry_sidecars(geometry_sidecars);
    let mut consumed = 0;
    let mut index = 0;
    for path in input {
        for batch in fixtures::read_batches(path)? {
            let batch = batch?;
            let mut offset = 0;
            while offset < batch.num_rows() {
                if consumed == rows {
                    return Err("repack input has extra rows".into());
                }
                let end = (index + 1) * rows / files;
                let length = (end - consumed).min((batch.num_rows() - offset) as u64) as usize;
                writer.push(batch.slice(offset, length))?;
                offset += length;
                consumed += length as u64;
                if consumed == end {
                    writer.end_file()?;
                    index += 1;
                }
            }
        }
    }
    if consumed != rows || index != files {
        return Err("repack input lost rows".into());
    }
    Ok(writer)
}

pub fn verified_files(input: &Path, group: &Value) -> Result<Vec<PathBuf>> {
    let files = group["files"].as_array().ok_or("missing source files")?;
    let mut paths = Vec::new();
    for (i, item) in files.iter().enumerate() {
        let name = format!("part-{i:05}.parquet");
        if item["path"] != name {
            return Err("unexpected source file path/order".into());
        }
        let path = input.join(name);
        if item["bytes"] != fs::metadata(&path)?.len()
            || item["sha256"] != fixtures::hash_file(&path)?
        {
            return Err(format!("source object changed: {}", path.display()).into());
        }
        paths.push(path);
    }
    if paths != fixtures::parquet_files(input)? {
        return Err("source file inventory differs from manifest".into());
    }
    Ok(paths)
}

pub fn generate(config: &Config, input: &Path) -> Result<Value> {
    let started = Instant::now();
    let parent_bytes = fs::read(input.join("manifest.json"))?;
    let parent: Value = serde_json::from_slice(&parent_bytes)?;
    if parent["status"] != "complete"
        || parent["protocol"] != "selective-read-v1"
        || parent["profile"] != config.profile.name()
        || parent["writer"] != fixtures::writer_settings()
        || parent["generator"]["lockfile_sha256"]
            != fixtures::hash_file(&input.join("generator-Cargo.lock"))?
    {
        return Err("incompatible repack source manifest/profile/writer".into());
    }
    let clustered = parent["tables"]
        .as_array()
        .and_then(|tables| tables.iter().find(|t| t["id"] == "li.clustered"))
        .ok_or("missing clustered original table")?;
    let scale = if config.profile == Profile::Smoke {
        0.01
    } else {
        1.0
    };
    if clustered["scale_factor"] != scale
        || clustered["path"] != "li.clustered"
        || clustered["layout"] != "clustered"
        || clustered["snapshot_version"] != 0
        || clustered["deletion_vectors"] != false
    {
        return Err(
            "repacking requires original clustered SF1 (SF0.01 for smoke), snapshot 0 without DVs"
                .into(),
        );
    }
    let source = parent["sources"]
        .as_array()
        .and_then(|sources| sources.iter().find(|s| s["scale_factor"] == scale))
        .ok_or("missing original source")?;
    let source_path = format!("sf{scale}/source");
    if source["path"] != source_path || source["rows"] != clustered["rows"] {
        return Err("source path or row count differs".into());
    }
    let source_files = verified_files(&input.join(&source_path), source)?;
    let input_files = verified_files(&input.join("li.clustered"), clustered)?;
    let rows = source["rows"].as_u64().ok_or("source rows")?;
    let output_parent = config
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(output_parent)?;
    let available = fixtures::available_disk(output_parent)?;
    let disk_limit = config
        .disk_limit
        .unwrap_or(config.profile.disk_bytes())
        .min(available.saturating_sub(512 * MIB));
    // Reserve half for the later MinIO copy; repacking does not sort or spill.
    let budget = Arc::new(Budget::new(disk_limit / 2));
    fs::create_dir(&config.output)?;
    fs::create_dir_all(config.output.join(&source_path))?;
    for path in source_files {
        budget.copy(
            &path,
            &config
                .output
                .join(&source_path)
                .join(path.file_name().ok_or("source filename")?),
        )?;
    }
    verified_files(&config.output.join(&source_path), source)?;
    let mut tables = Vec::new();
    for files in [64, 4096] {
        let id = format!("files{files}");
        eprintln!("repacking {id}, SF{scale}");
        let writer = table(
            &input_files,
            &config.output.join(&id),
            original_schema(),
            rows,
            files,
            budget.clone(),
            false,
        )?;
        let mut manifest = finish_table(writer, &id, config.profile, scale, "clustered", rows)?;
        if manifest["file_count"] != files {
            return Err("unexpected repacked file count".into());
        }
        for (i, file) in manifest["files"]
            .as_array_mut()
            .ok_or("files")?
            .iter_mut()
            .enumerate()
        {
            let first = i as u64 * rows / files;
            let end = (i as u64 + 1) * rows / files;
            if file["rows"] != end - first {
                return Err("repacked file boundary differs".into());
            }
            file["source_ordinal_range"] = json!([first, end]);
        }
        tables.push(manifest);
    }
    verified_files(&input.join("li.clustered"), clustered)?;
    budget.write(
        &config.output.join("generator-Cargo.lock"),
        LOCKFILE.as_bytes(),
    )?;
    let manifest = json!({
        "protocol": "selective-read-v1", "protocol_sha256": fixtures::hash_bytes(PROTOCOL.as_bytes()),
        "profile": config.profile.name(), "status": "complete", "generator": generator_identity()?,
        "recipe": parent["recipe"], "writer": fixtures::writer_settings(), "sources": [source], "tables": tables,
        "repacked_from": {"manifest_sha256": fixtures::hash_bytes(&parent_bytes), "fixture_id": "li.clustered",
            "ordinal_rule": "[floor(i*N/F), floor((i+1)*N/F))", "groups_split_at_file_boundaries": true},
        "preparation": {"memory_limit_bytes": config.profile.memory_bytes(), "disk_limit_bytes": disk_limit,
            "output_limit_bytes": disk_limit / 2, "free_disk_before_bytes": available,
            "peak_rss_bytes": fixtures::peak_rss()?, "elapsed_ms": started.elapsed().as_millis()}
    });
    budget.write(
        &config.output.join("manifest.json"),
        &serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}

/// Preserve wide rows and compare the fixed normal/4096-file organizations.
pub fn wide_pair(config: &Config, input: &Path, capacity: Option<Value>) -> Result<Value> {
    let started = Instant::now();
    let parent_bytes = fs::read(input.join("manifest.json"))?;
    let parent: Value = serde_json::from_slice(&parent_bytes)?;
    let scale = config
        .large
        .as_ref()
        .map_or_else(|| config.profile.scales().1, |o| f64::from(o.scale));
    if parent["status"] != "complete"
        || parent["profile"] != config.profile.name()
        || parent["writer"] != fixtures::writer_settings()
    {
        return Err("incompatible wide-file parent/profile/writer".into());
    }
    let mut normal = parent["tables"]
        .as_array()
        .ok_or("tables")?
        .iter()
        .find(|t| t["id"] == "wide.clustered")
        .ok_or("missing wide clustered parent")?
        .clone();
    if normal["path"] != "wide.clustered"
        || normal["scale_factor"] != scale
        || normal["snapshot_version"] != 0
        || normal["deletion_vectors"] != false
        || normal["schema"] != fixtures::delta_schema(&fixtures::wide_schema())?
    {
        return Err("wide-file parent must be the matching 80-column clustered snapshot 0".into());
    }
    let output_parent = config
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(output_parent)?;
    let available = fixtures::available_disk(output_parent)?;
    let disk_limit = config
        .disk_limit
        .unwrap_or(config.profile.disk_bytes())
        .min(available.saturating_sub(512 * MIB));
    let output_limit = capacity.as_ref().map_or(Ok(disk_limit / 2), |v| {
        v["output_limit_bytes"].as_u64().ok_or("output budget")
    })?;
    let budget = Arc::new(Budget::new(output_limit));
    fs::create_dir(&config.output)?;
    let source = large::reuse_source(input, &config.output, scale, budget.clone())?;
    let rows = source["rows"].as_u64().ok_or("source rows")?;
    if normal["rows"] != rows {
        return Err("wide/source row count differs".into());
    }
    let input_files = verified_files(&input.join("wide.clustered"), &normal)?;
    let normal_root = config.output.join("wide.clustered");
    fs::create_dir_all(normal_root.join("_delta_log"))?;
    for path in &input_files {
        budget.copy(path, &normal_root.join(path.file_name().ok_or("filename")?))?;
    }
    let log = "_delta_log/00000000000000000000.json";
    if normal["delta_log"]["path"] != log
        || normal["delta_log"]["sha256"]
            != fixtures::hash_file(&input.join("wide.clustered").join(log))?
    {
        return Err("wide parent Delta log changed".into());
    }
    budget.copy(
        &input.join("wide.clustered").join(log),
        &normal_root.join(log),
    )?;
    normal["queries"] = json!({"wide.clustered.eq2-in20": source["wide_queries"]["eq2-in20"]});
    let writer = table(
        &input_files,
        &config.output.join("wide.files4096"),
        fixtures::wide_schema(),
        rows,
        4096,
        budget.clone(),
        true,
    )?;
    let mut repacked = finish_table(
        writer,
        "wide.files4096",
        config.profile,
        scale,
        "clustered",
        rows,
    )?;
    if repacked["file_count"] != 4096 {
        return Err("wide repack must contain 4096 files".into());
    }
    for (i, file) in repacked["files"]
        .as_array_mut()
        .ok_or("files")?
        .iter_mut()
        .enumerate()
    {
        let (first, end) = (i as u64 * rows / 4096, (i as u64 + 1) * rows / 4096);
        if file["rows"] != end - first {
            return Err("wrong wide ordinal boundary".into());
        }
        file["source_ordinal_range"] = json!([first, end]);
    }
    repacked["queries"] = json!({"wide.files4096.eq2-in20": source["wide_queries"]["eq2-in20"]});
    let mut groups = vec![(config.output.join("wide.files4096"), &repacked)];
    if config.large_file_pair {
        groups.push((normal_root, &normal));
    }
    let literals = source["in_literals"]
        .as_array()
        .ok_or("literals")?
        .iter()
        .map(|v| v.as_i64().ok_or("literal"))
        .collect::<std::result::Result<BTreeSet<_>, _>>()?;
    let extra = dv::file_minima(&groups, &literals)?;
    let mut tables = Vec::new();
    if config.large_file_pair {
        tables.push(dv::variant(
            &config.output,
            &normal,
            config.profile,
            false,
            budget.clone(),
            &extra,
        )?);
    }
    tables.push(dv::variant(
        &config.output,
        &repacked,
        config.profile,
        false,
        budget.clone(),
        &extra,
    )?);
    tables.extend([normal, repacked]);
    if fixtures::hash_file(&input.join("manifest.json"))? != fixtures::hash_bytes(&parent_bytes) {
        return Err("wide-file parent changed during preparation".into());
    }
    budget.write(
        &config.output.join("generator-Cargo.lock"),
        LOCKFILE.as_bytes(),
    )?;
    let mut manifest = json!({"protocol": "selective-read-v1", "protocol_sha256": fixtures::hash_bytes(PROTOCOL.as_bytes()),
        "status": "complete", "profile": config.profile.name(), "generator": generator_identity()?,
        "recipe": parent["recipe"], "writer": fixtures::writer_settings(), "sources": [source], "tables": tables,
        "repacked_from": {"manifest_sha256": fixtures::hash_bytes(&parent_bytes), "fixture_id": "wide.clustered",
            "ordinal_rule": "[floor(i*N/F), floor((i+1)*N/F))", "groups_split_at_file_boundaries": true},
        "wide_file_pair": {"kind": if config.large_file_pair { "large" } else { "legacy" },
            "extra_logical_keys": extra, "shared_organizations": if config.large_file_pair { vec!["wide.clustered", "wide.files4096"] } else { vec!["wide.files4096"] }},
        "preparation": {"memory_limit_bytes": config.profile.memory_bytes(), "disk_limit_bytes": disk_limit,
            "output_limit_bytes": output_limit, "free_disk_before_bytes": available,
            "peak_rss_bytes": fixtures::peak_rss()?, "elapsed_ms": started.elapsed().as_millis()}});
    if let Some(plan) = capacity {
        large::finish_manifest(&mut manifest, config, plan, &budget)?;
    }
    budget.write(
        &config.output.join("manifest.json"),
        &serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}
