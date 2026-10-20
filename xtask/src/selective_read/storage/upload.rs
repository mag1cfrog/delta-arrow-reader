use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
    time::Instant,
};

use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    BUCKET, array, exclusive, put_verified, read_json, require, save, text, timestamp_ns,
    verify_server,
};
use crate::{Result, artifacts::reader_sha256, selective_read_build::digest};

#[derive(clap::Args, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Args {
    #[arg(long = "state")]
    directory: PathBuf,
    #[arg(long)]
    fixtures: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long = "table")]
    table_ids: Option<Vec<String>>,
    #[arg(long = "verify-checksum")]
    checksum: bool,
}

pub(in crate::selective_read) fn hashes(paths: &[PathBuf]) -> Result<Vec<String>> {
    paths.iter().map(|path| digest(path)).collect()
}

pub(in crate::selective_read) fn inventory(
    fixtures: &Path,
    table_ids: Option<&[String]>,
    mut hash: impl FnMut(&[PathBuf]) -> Result<Vec<String>>,
) -> Result<Value> {
    let fixtures = fs::canonicalize(fixtures)?;
    let manifest = read_json(&fixtures.join("manifest.json"))?;
    let tables = crate::selective_read::fixture_tables(&manifest)?;
    if let Some(ids) = table_ids {
        require(
            !ids.is_empty()
                && ids.iter().collect::<BTreeSet<_>>().len() == ids.len()
                && ids
                    .iter()
                    .all(|id| tables.iter().any(|table| table["id"] == *id)),
            "invalid native upload table selection",
        )?;
    }
    let mut objects = Vec::new();
    let mut paths = Vec::new();
    let mut names = BTreeSet::new();
    for table in tables {
        if table_ids.is_some_and(|ids| !ids.iter().any(|id| table["id"] == *id)) {
            continue;
        }
        let files = array(&table["files"])?;
        let logs = if let Some(logs) = table.get("delta_logs") {
            array(logs)?.iter().collect::<Vec<_>>()
        } else {
            vec![&table["delta_log"]]
        };
        for item in logs
            .into_iter()
            .chain(files)
            .chain(files.iter().filter_map(|file| file.get("deletion_vector")))
        {
            let name = Path::new(text(&table["path"])?).join(text(&item["path"])?);
            require(
                !name.as_os_str().is_empty()
                    && name
                        .components()
                        .all(|part| matches!(part, Component::Normal(_))),
                "expected a relative fixture object path",
            )?;
            let path = fs::canonicalize(fixtures.join(&name))?;
            require(
                path.starts_with(&fixtures) && path.is_file(),
                "fixture object escapes its root",
            )?;
            require(
                item["bytes"] == fs::metadata(&path)?.len(),
                "fixture object size changed",
            )?;
            require(names.insert(name.clone()), "duplicate fixture object")?;
            text(&item["sha256"])?;
            objects.push(json!({"path":name, "bytes":item["bytes"], "sha256":item["sha256"]}));
            paths.push(path);
        }
    }
    let actual = hash(&paths)?;
    require(
        actual.len() == objects.len(),
        "incomplete fixture checksums",
    )?;
    for (object, actual) in objects.iter().zip(actual) {
        require(
            object["sha256"] == actual,
            &format!(
                "fixture object checksum changed: {}",
                text(&object["path"])?
            ),
        )?;
    }
    Ok(json!(objects))
}

pub(in crate::selective_read) fn run(
    args: &Args,
    hash: impl FnMut(&[PathBuf]) -> Result<Vec<String>>,
) -> Result<Value> {
    require(!args.output.exists(), "upload requires a new receipt path")?;
    let started = Instant::now();
    let objects = inventory(&args.fixtures, args.table_ids.as_deref(), hash)?;
    let prefix = digest(&args.fixtures.join("manifest.json"))?;
    let mut methods = BTreeMap::<&str, usize>::new();
    {
        let _lock = exclusive(&args.directory)?;
        verify_server(&args.directory)?;
        for item in array(&objects)? {
            let name = text(&item["path"])?;
            let method = put_verified(
                &args.directory,
                &format!("{prefix}/{name}"),
                &args.fixtures.join(name),
                text(&item["sha256"])?,
                args.checksum,
            )?;
            *methods.entry(method).or_default() += 1;
        }
    }
    let receipt = json!({"status":"verified", "server_sha256":digest(&args.directory.join("server.json"))?,
        "fixture_manifest_sha256":prefix, "prefix":prefix, "objects":objects,
        "table_root":format!("s3://{BUCKET}/{prefix}"), "table_ids":args.table_ids,
        "verified_ns":timestamp_ns()?, "verification":{"requested":if args.checksum { "server-sha256" } else { "full-get" }, "methods":methods},
        "source_sha256":reader_sha256(include_bytes!("upload.rs").as_slice())?,
        "controller_sha256":digest(&std::env::current_exe()?)?, "elapsed_seconds":started.elapsed().as_secs_f64()});
    save(&args.output, &receipt)?;
    Ok(receipt)
}

pub(in crate::selective_read) fn prepare(
    directory: &Path,
    fixtures: &Path,
    receipt_path: &Path,
    upload_tables: &[String],
    mut hash: impl FnMut(&[PathBuf]) -> Result<Vec<String>>,
) -> Result<Value> {
    if !upload_tables.is_empty() {
        require(
            !receipt_path.exists(),
            "inline upload requires a new receipt path",
        )?;
        run(
            &Args {
                directory: directory.to_owned(),
                fixtures: fixtures.to_owned(),
                output: receipt_path.to_owned(),
                table_ids: Some(upload_tables.to_vec()),
                checksum: true,
            },
            &mut hash,
        )?;
    }
    let receipt = read_json(receipt_path)?;
    let table_ids: Option<Vec<String>> = serde_json::from_value(receipt["table_ids"].clone())?;
    let objects = inventory(fixtures, table_ids.as_deref(), hash)?;
    Ok(json!({"receipt":receipt, "objects":objects}))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixtures(root: &Path) -> Result<Value> {
        let mut tables = Vec::new();
        for name in ["base", "base.dv"] {
            fs::create_dir(root.join(name))?;
            let mut items = Vec::new();
            for file in ["log.json", "part.parquet", "sidecar.bin"] {
                let path = root.join(name).join(file);
                fs::write(&path, format!("{name}{file}"))?;
                items.push(json!({"path":file, "bytes":fs::metadata(&path)?.len(), "sha256":digest(&path)?}));
            }
            if name.ends_with(".dv") {
                items[1]["deletion_vector"] = items[2].clone();
            }
            tables.push(json!({"id":name, "path":name, "delta_log":items[0], "files":[items[1]]}));
        }
        let manifest =
            json!({"status":"complete", "protocol":"selective-read-v1", "tables":tables});
        save(&root.join("manifest.json"), &manifest)?;
        Ok(manifest)
    }

    #[test]
    fn inventory_selects_complete_snapshots_and_checks_dv_logs_and_paths() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path();
        let mut manifest = fixtures(root)?;
        let selected = inventory(root, Some(&["base".into()]), hashes)?;
        assert_eq!(
            array(&selected)?
                .iter()
                .map(|o| text(&o["path"]).unwrap())
                .collect::<Vec<_>>(),
            ["base/log.json", "base/part.parquet"]
        );
        assert_eq!(array(&inventory(root, None, hashes)?)?.len(), 5);
        assert_eq!(
            array(&inventory(root, Some(&["base.dv".into()]), hashes)?)?.len(),
            3
        );
        for invalid in [
            vec![],
            vec!["base".into(), "base".into()],
            vec!["unknown".into()],
        ] {
            assert!(inventory(root, Some(&invalid), hashes).is_err());
        }
        fs::remove_file(root.join("base.dv/part.parquet"))?;
        assert_eq!(inventory(root, Some(&["base".into()]), hashes)?, selected);
        assert!(inventory(root, None, hashes).is_err());
        fs::write(root.join("base/part.parquet"), b"bad!part.parquet")?;
        assert!(
            inventory(root, Some(&["base".into()]), hashes)
                .unwrap_err()
                .to_string()
                .contains("checksum changed")
        );
        manifest["tables"][0]["delta_logs"] = json!([
            manifest["tables"][0]["delta_log"],
            manifest["tables"][0]["delta_log"]
        ]);
        fs::write(root.join("manifest.json"), serde_json::to_vec(&manifest)?)?;
        assert!(
            inventory(root, Some(&["base".into()]), hashes)
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
        manifest["tables"][0]["delta_logs"] =
            json!([{"path":"../manifest.json","bytes":0,"sha256":"hash"}]);
        fs::write(root.join("manifest.json"), serde_json::to_vec(&manifest)?)?;
        assert!(
            inventory(root, Some(&["base".into()]), hashes)
                .unwrap_err()
                .to_string()
                .contains("relative")
        );
        Ok(())
    }

    #[test]
    fn uploads_reject_existing_receipts_before_touching_storage() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let receipt = temporary.path().join("receipt.json");
        fs::write(&receipt, b"original")?;
        let args = Args {
            directory: temporary.path().join("missing"),
            fixtures: temporary.path().join("missing"),
            output: receipt.clone(),
            table_ids: None,
            checksum: true,
        };
        assert!(run(&args, |_| panic!("must not inspect fixture data")).is_err());
        assert!(
            prepare(
                &args.directory,
                &args.fixtures,
                &receipt,
                &["base".into()],
                |_| panic!("must not inspect fixture data")
            )
            .is_err()
        );
        assert_eq!(fs::read(receipt)?, b"original");
        Ok(())
    }
}
