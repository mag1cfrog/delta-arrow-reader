//! Fixed no-DV controls from the frozen within-file protocol.

use super::*;
use parquet::basic::Compression;

pub const CASES: [&str; 3] = ["row-groups.select", "pages.localized", "pages.scattered"];

pub fn table(output: &Path, case: &str, files: usize, budget: Arc<Budget>) -> Result<Value> {
    let row_groups = case == CASES[0];
    let id = if row_groups { "row-groups" } else { case };
    let groups = if row_groups { 16 } else { 2 };
    let mut properties = fixtures::writer_properties()?
        .into_builder()
        .set_max_row_group_row_count(Some(4096));
    let mut settings = fixtures::writer_settings();
    settings["row_group_rows"] = json!(4096);
    settings["groups_per_file"] = json!(groups);
    if !row_groups {
        properties = properties
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_write_batch_size(128)
            .set_data_page_row_count_limit(128);
        settings["compression"] = json!("UNCOMPRESSED");
        settings["dictionary"] = json!(false);
        settings["write_batch_rows"] = json!(128);
        settings["data_page_rows"] = json!(128);
    }
    let schema = control_rows::schema(16);
    let mut writer = TableWriter::with_geometry(
        &output.join(id),
        schema.clone(),
        budget,
        properties.build(),
        groups,
    )?;
    let rows = files * groups * 4096;
    for first in (0..rows).step_by(4096) {
        writer.push(control_rows::batch(
            schema.clone(),
            first,
            4096,
            |row| match case {
                "row-groups.select" => row / 4096 % 16 == 7,
                "pages.localized" => row % 4096 < 32,
                "pages.scattered" => row % 128 == 0,
                _ => unreachable!(),
            },
        )?)?;
    }
    let mut result = writer.finish(Some(("controls", id)))?;
    result["id"] = json!(id);
    result["path"] = json!(id);
    result["snapshot_version"] = json!(0);
    result["deletion_vectors"] = json!(false);
    result["writer"] = settings;
    let projection = std::iter::once("row_id".to_owned())
        .chain((0..16).map(control_rows::payload_name))
        .collect::<Vec<_>>()
        .join(", ");
    result["queries"] =
        json!({case: format!("SELECT {projection} FROM bench WHERE event_id = 'match'")});
    Ok(result)
}

pub fn generate(config: &Config) -> Result<Value> {
    let started = Instant::now();
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
    let mut tables = Vec::new();
    for case in CASES {
        eprintln!("generating {case}");
        tables.push(table(
            &config.output,
            case,
            if case == CASES[0] { 16 } else { 1 },
            budget.clone(),
        )?);
    }
    budget.write(
        &config.output.join("generator-Cargo.lock"),
        LOCKFILE.as_bytes(),
    )?;
    let manifest = json!({
        "protocol": "selective-read-v1", "protocol_sha256": fixtures::hash_bytes(PROTOCOL.as_bytes()),
        "profile": config.profile.name(), "status": "complete", "generator": generator_identity()?,
        "recipe": "fixed synthetic within-file controls; independent of TPC-H scale", "sources": [], "tables": tables,
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
