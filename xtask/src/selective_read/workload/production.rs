use std::{collections::BTreeMap, fs, process::Command};

use super::*;
use crate::{
    command::run_command,
    repository::files,
    selective_read::{
        campaign,
        storage::{read_json, save},
    },
};

#[derive(clap::Args, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Define {
    #[arg(long, required = true)]
    fixtures: Vec<PathBuf>,
    #[arg(long = "binary", required = true)]
    binaries: Vec<PathBuf>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "pilot", value_parser = ["pilot", "formal"])]
    stage: String,
    #[arg(long, default_value_t = 6)]
    comparison_revision: u64,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

fn protocols(harness: &Path) -> Result<PathBuf> {
    Ok(harness
        .parent()
        .and_then(Path::parent)
        .ok_or("invalid harness directory")?
        .join("docs/content/benchmarks"))
}

fn sources(harness: &Path) -> Result<Value> {
    let root = harness
        .parent()
        .and_then(Path::parent)
        .ok_or("invalid harness directory")?;
    let mut paths: Vec<_> = [
        "oracle_io.py",
        "oracle-requirements.txt",
        "production_shapes.py",
        "matrix.py",
        "runners/run.py",
        "runners/native.py",
    ]
    .into_iter()
    .map(|name| (name.to_owned(), harness.join(name)))
    .collect();
    paths.push(("../../Cargo.lock".into(), root.join("Cargo.lock")));
    for path in files(&root.join("xtask/src"))? {
        if path.extension().is_some_and(|extension| extension == "rs") {
            paths.push((
                format!("../../{}", path.strip_prefix(root)?.display()),
                path,
            ));
        }
    }
    Ok(json!(
        paths
            .into_iter()
            .map(|(name, path)| Ok((name, json!(digest(&path)?))))
            .collect::<Result<Map<_, _>>>()?
    ))
}

fn limits() -> Value {
    json!({"memory_bytes":16 * 1024u64.pow(3),"disk_bytes":1024u64.pow(3),"elapsed_seconds":1800})
}

fn inventory(selected: &BTreeSet<String>, readers: &[&str; 5]) -> Value {
    let mut value = cases();
    for (case, row) in value.as_object_mut().unwrap() {
        row["status"] = json!(if selected.contains(case) {
            "prepared"
        } else {
            "not_prepared"
        });
        row["readers"] = json!(readers);
    }
    value
}

fn sessions(selected: &BTreeSet<String>) -> Value {
    json!(
        selected
            .iter()
            .map(|case| (format!("reuse.{case}"), json!(case)))
            .collect::<Map<_, _>>()
    )
}

fn translate(rows: &[Value], binaries: &[PathBuf], output: &Path, harness: &Path) -> Result<Value> {
    require(
        binaries.len() == 1,
        "provide the pinned Polars translation binary",
    )?;
    let binary = fs::canonicalize(&binaries[0])?;
    let build = read_json(&binary.with_file_name("build.json"))?;
    require(
        build["reader_id"] == "polars",
        "provide the pinned Polars translation binary",
    )?;
    let sql = rows
        .iter()
        .map(|row| {
            Ok((
                text(&row["case_id"])?.to_owned(),
                row["canonical_sql"].clone(),
            ))
        })
        .collect::<Result<Map<_, _>>>()?;
    save(&output.join("sql.json"), &json!(sql))?;
    let path = output.join("polars-translations.json");
    run_command(
        Command::new(binary.with_file_name("venv").join("bin/python"))
            .args(["-I", "-B"])
            .arg(harness.join("matrix.py"))
            .arg("translations")
            .arg("--binary")
            .arg(&binary)
            .arg("--request")
            .arg(output.join("sql.json"))
            .arg("--output")
            .arg(&path),
    )?;
    let translation = read_json(&path)?;
    Ok(json!({"polars":translation}))
}

pub(in crate::selective_read) fn define(mut args: Define) -> Result<Value> {
    require(
        args.comparison_revision == 6,
        "new workloads use revision 6; use the archived harness for Daft revisions",
    )?;
    args.harness = fs::canonicalize(args.harness)?;
    let protocols = protocols(&args.harness)?;
    let readers = campaign::roster(&json!({"comparison_revision":args.comparison_revision}))?;
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for root in &args.fixtures {
        let root = fs::canonicalize(root)?;
        let manifest = read_json(&root.join("manifest.json"))?;
        for table in fixture_tables(&manifest)? {
            let case = text(&table["id"])?;
            require(seen.insert(case.to_owned()), "duplicate production case")?;
            require(
                !table["delta_log"].is_null(),
                "raw probes need native Delta logs before reader validation",
            )?;
            let mut row = query(case, &manifest, &protocols)?;
            row["fixtures"] = json!(root);
            row["fixture_manifest_sha256"] = json!(digest(&root.join("manifest.json"))?);
            rows.push(row);
        }
    }
    let modes = rows
        .iter()
        .map(|r| Ok(text(&r["fixture_mode"])?.to_owned()))
        .collect::<Result<BTreeSet<_>>>()?;
    let source_ids = rows
        .iter()
        .map(|r| text(&r["source_parent_manifest_sha256"]))
        .collect::<Result<BTreeSet<_>>>()?;
    require(
        !rows.is_empty() && modes.len() == 1 && source_ids.len() == 1,
        "mixed probe/full or source identities",
    )?;
    sampling(&args.stage, &seen, &modes)?;
    fs::create_dir(&args.output)?;
    args.output = fs::canonicalize(args.output)?;
    let translations = translate(&rows, &args.binaries, &args.output, &args.harness)?;
    let result = json!({"format":"selective-read-production-workload-v1","family":"production","comparison_revision":args.comparison_revision,
        "protocol_sha256":digest(&protocols.join("selective-read-spark-matrix.md"))?,
        "base_protocol_sha256":digest(&protocols.join("selective-read-protocol.md"))?,
        "sampling_sha256":digest(&protocols.join("selective-read-sampling.md"))?,
        "sampling_stage":args.stage,"definition_sha256":digest(&protocols.join("selective-read-production-shapes.md"))?,
        "scope":if modes.contains("probe") { "probe" } else { &args.stage }, "publication_ready":false,
        "source_sha256":sources(&args.harness)?, "definition_controller_sha256":digest(&std::env::current_exe()?)?,
        "readers":readers, "cases":rows, "sessions":sessions(&seen), "inventory":inventory(&seen,&readers),
        "oracle_limits":limits(),"translations":translations});
    let path = args.output.join("workload.json");
    save(&path, &result)?;
    identity(&result, &digest(&path)?)?;
    load(result, &args.harness)
}

fn check_binding(row: &Value, manifest: &Value, protocols: &Path) -> Result<()> {
    let expected = query(text(&row["case_id"])?, manifest, protocols)?;
    require(
        object(&expected)?
            .iter()
            .all(|(key, value)| row.get(key) == Some(value)),
        "production SQL, literals, source or geometry changed",
    )
}

pub(super) fn binding(value: &Value, fixtures: &Path, case: &str, harness: &Path) -> Result<Value> {
    let row = array(&value["cases"])?
        .iter()
        .find(|row| row["case_id"] == case)
        .ok_or("unknown production case")?;
    let path = fixtures.join("manifest.json");
    require(
        row["fixture_manifest_sha256"] == digest(&path)?,
        "production fixture identity changed",
    )?;
    check_binding(row, &read_json(&path)?, &protocols(harness)?)?;
    Ok(row.clone())
}

pub(super) fn load(value: Value, harness: &Path) -> Result<Value> {
    let harness = fs::canonicalize(harness)?;
    let protocols = protocols(&harness)?;
    let readers = campaign::roster(&value)?;
    require(
        value["format"] == "selective-read-production-workload-v1"
            && matches!(value["comparison_revision"].as_u64(), Some(5 | 6))
            && value["family"] == "production",
        "unknown production workload",
    )?;
    let rows = array(&value["cases"])?;
    let selected = rows
        .iter()
        .map(|r| Ok(text(&r["case_id"])?.to_owned()))
        .collect::<Result<BTreeSet<_>>>()?;
    let all = cases();
    require(
        !selected.is_empty()
            && selected.len() == rows.len()
            && selected.iter().all(|c| all.get(c).is_some()),
        "unknown/duplicate production cases",
    )?;
    require(
        value["readers"] == json!(readers)
            && value["sessions"] == sessions(&selected)
            && value["inventory"] == inventory(&selected, &readers),
        "incomplete production inventory",
    )?;
    let modes = rows
        .iter()
        .map(|r| Ok(text(&r["fixture_mode"])?.to_owned()))
        .collect::<Result<BTreeSet<_>>>()?;
    require(
        (modes == BTreeSet::from(["probe".into()]) || modes == BTreeSet::from(["generate".into()]))
            && value["scope"]
                == if modes.contains("probe") {
                    json!("probe")
                } else {
                    value["sampling_stage"].clone()
                }
            && value["publication_ready"] == false,
        "invalid production scope",
    )?;
    sampling(text(&value["sampling_stage"])?, &selected, &modes)?;
    require(
        value["oracle_limits"] == limits(),
        "production oracle limits changed",
    )?;
    let translations = object(&value["translations"])?;
    let expected: BTreeSet<_> = if value["comparison_revision"] == 6 {
        vec!["polars"]
    } else {
        vec!["daft", "polars"]
    }
    .into_iter()
    .collect();
    require(
        translations
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            == expected,
        "missing native translations",
    )?;
    for (reader, translation) in translations {
        require(
            translation["lock_sha256"]
                == digest(&harness.join("runners").join(reader).join("lock.json"))?
                && object(&translation["expressions"])?
                    .keys()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    == selected,
            "native translation lock/cases changed",
        )?;
        for (case, expression) in object(&translation["expressions"])? {
            let row = rows
                .iter()
                .find(|r| r["case_id"] == *case)
                .ok_or("missing case binding")?;
            require(
                expression["native_expression"]
                    .as_object()
                    .is_some_and(|v| !v.is_empty())
                    && expression["canonical_sql"] == row["canonical_sql"],
                "native expression or SQL binding invalid",
            )?;
        }
    }
    let mut manifests = BTreeMap::new();
    let mut unions = BTreeMap::new();
    for row in rows {
        let path = Path::new(text(&row["fixtures"])?).join("manifest.json");
        if !manifests.contains_key(&path) {
            manifests.insert(path.clone(), (digest(&path)?, read_json(&path)?));
        }
        let (hash, manifest) = &manifests[&path];
        require(
            row["fixture_manifest_sha256"] == *hash,
            "production fixture identity changed",
        )?;
        check_binding(row, manifest, &protocols)?;
        if row["deletion_vectors"] == true {
            let keys = field(&manifest["production_dv"], "extra_logical_keys")?;
            let previous = unions
                .entry(text(&row["shape"])?)
                .or_insert_with(|| keys.clone());
            require(
                *previous == *keys,
                "paired DV layouts delete different logical rows",
            )?;
        }
    }
    Ok(value)
}

#[cfg(test)]
mod tests;
