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
) -> Result<TableWriter> {
    if files == 0 || rows < files || rows.div_ceil(files) > fixtures::FILE_ROWS as u64 {
        return Err("repacked files must contain 1..=1048576 rows".into());
    }
    rows.checked_mul(files).ok_or("file boundary overflow")?;
    let mut writer = TableWriter::new(output, schema, budget)?;
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

fn verified_files(input: &Path, group: &Value) -> Result<Vec<PathBuf>> {
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
