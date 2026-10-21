use std::fs;

use super::*;
use crate::selective_read::{
    report::batch,
    storage::{read_json, save},
    validation::Worker,
};

#[derive(clap::Args)]
pub(crate) struct Prepare {
    #[arg(long)]
    parent: PathBuf,
    #[arg(long)]
    fixtures: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

#[derive(clap::Args)]
pub(crate) struct Report {
    #[arg(long)]
    campaign: PathBuf,
    #[arg(long)]
    references: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

fn cases() -> Vec<String> {
    [64, 4096]
        .into_iter()
        .flat_map(|files| ["empty", "eq2-in20"].map(|query| format!("files{files}.{query}")))
        .collect()
}

pub(super) fn geometry(table: &Value, files: u64) -> Result<()> {
    let items = array(&table["files"])?;
    require(
        files > 0 && table["file_count"] == files && items.len() as u64 == files,
        "wrong repacked file count",
    )?;
    let rows = count(&table["rows"])?;
    for (i, item) in items.iter().enumerate() {
        let first = u64::try_from(i as u128 * u128::from(rows) / u128::from(files))?;
        let end = u64::try_from((i as u128 + 1) * u128::from(rows) / u128::from(files))?;
        require(
            item["source_ordinal_range"] == json!([first, end])
                && item["rows"] == end - first
                && item["delta_stats"]["numRecords"] == end - first,
            "wrong repacked file boundary",
        )?;
        require(
            fixtures::sum(array(&item["row_groups"])?, "rows")? == count(&item["rows"])?,
            "wrong repacked group rows",
        )?;
    }
    Ok(())
}

fn pruning(reference: &Value) -> Result<()> {
    let candidates = array(&reference["candidate_files"])?;
    let matching = array(&reference["matching_files"])?;
    if text(&reference["case_id"])?.ends_with(".empty") {
        return require(
            candidates.is_empty() && matching.is_empty() && reference["output_rows"] == 0,
            "empty predicate must exclude every file",
        );
    }
    require(
        count(&reference["output_rows"])? > 0 && !matching.is_empty(),
        "compound predicate must match rows",
    )?;
    require(
        matching.iter().all(|file| candidates.contains(file)),
        "statistics exclude matching files",
    )?;
    // Keep the contract's coarse pruning guard, independent of reader timings.
    require(
        !candidates.is_empty()
            && candidates.len() as u64 <= (count(&reference["active_files"])? / 100).max(1),
        "compound predicate lost its extensive file-skipping opportunity",
    )
}

fn parent_table<'a>(manifest: &Value, original: &'a Value, parent_hash: &str) -> Result<&'a Value> {
    require(
        manifest["status"] == "complete" && original["status"] == "complete",
        "incomplete fixture preparation",
    )?;
    require(
        manifest["repacked_from"]["manifest_sha256"] == parent_hash,
        "repack parent changed",
    )?;
    let clustered = array(&original["tables"])?
        .iter()
        .find(|t| t["id"] == manifest["repacked_from"]["fixture_id"])
        .ok_or("missing parent table")?;
    let source = array(&original["sources"])?
        .iter()
        .find(|s| s["scale_factor"].as_f64() == clustered["scale_factor"].as_f64())
        .ok_or("missing parent source")?;
    require(
        manifest["sources"] == json!([source]),
        "repacking changed the source rows, literals or SQL",
    )?;
    let tables = array(&manifest["tables"])?;
    require(
        tables.len() == 2
            && tables
                .iter()
                .map(|t| text(&t["id"]))
                .collect::<Result<BTreeSet<_>>>()?
                == BTreeSet::from(["files64", "files4096"]),
        "wrong file-organization table inventory",
    )?;
    Ok(clustered)
}

pub(crate) fn prepare(args: Prepare) -> Result<Value> {
    let parent = fs::canonicalize(&args.parent)?;
    let fixtures = fs::canonicalize(&args.fixtures)?;
    let manifest_hash = digest(&fixtures.join("manifest.json"))?;
    let parent_hash = digest(&parent.join("manifest.json"))?;
    let manifest = read_json(&fixtures.join("manifest.json"))?;
    let original = read_json(&parent.join("manifest.json"))?;
    let clustered = parent_table(&manifest, &original, &parent_hash)?;
    if let Some(parent) = args.output.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(&args.output)?;
    let output = fs::canonicalize(&args.output)?;
    let mut worker = Worker::start(&args.python, &fs::canonicalize(&args.harness)?, &output)?;
    let mut verified = Vec::new();
    for table in array(&manifest["tables"])? {
        let files = text(&table["id"])?
            .strip_prefix("files")
            .ok_or("unknown file organization")?
            .parse()?;
        geometry(table, files)?;
        let rows = worker.call(
            &json!({"action":"repacked-values","parent":parent,"original":clustered,
            "fixtures":fixtures,"table":table}),
        )?;
        require(
            rows == table["rows"] && rows == clustered["rows"],
            "repacked table lost rows",
        )?;
        verified.push(json!({"fixture_id":table["id"],"rows":rows,"file_count":table["file_count"],"parquet_bytes":field(table,"bytes")?}));
    }
    let mut references = Vec::new();
    for case in cases() {
        eprintln!("preparing {case}");
        let reference = worker.call(
            &json!({"action":"prepare-reference","fixtures":fixtures,"case_id":case,
            "reference":output.join(case),"reuse":false}),
        )?;
        pruning(&reference)?;
        references.push(reference);
    }
    for predicate in ["empty", "eq2-in20"] {
        let pair: Vec<_> = references
            .iter()
            .filter(|r| {
                r["case_id"]
                    .as_str()
                    .is_some_and(|id| id.ends_with(&format!(".{predicate}")))
            })
            .collect();
        require(
            pair.len() == 2 && pair[0]["reference_sha256"] == pair[1]["reference_sha256"],
            "repacked query results differ",
        )?;
    }
    let cases = references
        .iter()
        .map(|reference| {
            let fields = [
                "case_id",
                "source_rows",
                "output_rows",
                "active_files",
                "candidate_files",
                "matching_files",
                "reference_sha256",
            ];
            let mut row = fields
                .into_iter()
                .map(|k| Ok((k.to_owned(), field(reference, k)?.clone())))
                .collect::<Result<Map<_, _>>>()?;
            row.insert(
                "reference_metadata_sha256".into(),
                json!(digest(
                    &output
                        .join(text(&reference["case_id"])?)
                        .join("reference.json")
                )?),
            );
            Ok(json!(row))
        })
        .collect::<Result<Vec<_>>>()?;
    require(
        digest(&fixtures.join("manifest.json"))? == manifest_hash
            && digest(&parent.join("manifest.json"))? == parent_hash,
        "fixture manifest changed during preparation",
    )?;
    let result = json!({"status":"complete","profile":manifest["profile"],"fixture_manifest_sha256":manifest_hash,
        "parent_manifest_sha256":parent_hash,"verification":"exact ordered Arrow values in every column against parent li.clustered",
        "tables":verified,"cases":cases});
    save(&output.join("file-organizations.json"), &result)?;
    Ok(result)
}

fn checked_references(references: &Path, fixtures: &Path) -> Result<Value> {
    let checked = read_json(&references.join("file-organizations.json"))?;
    require(
        checked["status"] == "complete"
            && checked["fixture_manifest_sha256"] == digest(&fixtures.join("manifest.json"))?,
        "file-organization verification differs from campaign fixtures",
    )?;
    let rows = array(&checked["cases"])?;
    let actual = rows
        .iter()
        .map(|row| Ok(text(&row["case_id"])?.to_owned()))
        .collect::<Result<BTreeSet<_>>>()?;
    require(
        rows.len() == 4 && actual == cases().into_iter().collect(),
        "incomplete file-organization references",
    )?;
    for row in rows {
        let path = references
            .join(text(&row["case_id"])?)
            .join("reference.json");
        require(
            row["reference_metadata_sha256"] == digest(&path)?,
            "reference metadata changed",
        )?;
        let metadata = read_json(&path)?;
        require(
            object(row)?.iter().all(|(key, value)| {
                key == "reference_metadata_sha256" || metadata.get(key) == Some(value)
            }),
            "verification counts differ from reference",
        )?;
        pruning(&metadata)?;
    }
    Ok(checked)
}

pub(crate) fn report(args: Report) -> Result<Value> {
    let config = read_json(&args.campaign.join("campaign.json"))?;
    let checked = checked_references(&args.references, Path::new(text(&config["fixtures"])?))?;
    batch::run(serde_json::from_value(
        json!({"campaign":args.campaign,"output":args.output,
        "format":{"kind":"files","references":args.references,"checked":checked}}),
    )?)
}

#[cfg(test)]
mod tests;
