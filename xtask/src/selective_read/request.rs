use super::{array, fixture_tables, require, storage::read_json, text, workload};
use crate::{Result, selective_read_build::digest};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    pub(super) fixtures: PathBuf,
    pub(super) case_id: String,
    pub(super) execution_mode: String,
    pub(super) workload: Option<PathBuf>,
    pub(super) harness: PathBuf,
}

// Resolve existing symlinks even when a metadata-only fixture has reclaimed data.
fn resolve(path: &Path) -> Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(resolve(path.parent().ok_or("missing path parent")?)?
                .join(path.file_name().ok_or("invalid path")?))
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) fn run(args: Request) -> Result<Value> {
    require(
        ["open", "reuse"].contains(&args.execution_mode.as_str()),
        "unknown execution mode",
    )?;
    let fixtures = fs::canonicalize(&args.fixtures)?;
    let manifest_path = fixtures.join("manifest.json");
    let manifest = read_json(&manifest_path)?;
    let tables = fixture_tables(&manifest)?;
    let (table, sql, comparison) = if let Some(path) = &args.workload {
        let (frozen, identity) = workload::load(path, &args.harness)?;
        let row = workload::binding(&frozen, &fixtures, &args.case_id)?;
        let table = tables
            .iter()
            .find(|t| t["id"] == row["fixture_id"])
            .ok_or("unknown workload fixture")?;
        (table, row["canonical_sql"].clone(), identity)
    } else {
        require(
            manifest["protocol"] == "selective-read-v1" && manifest["profile"] != "large",
            "large fixtures require an explicit workload manifest",
        )?;
        let (table, sql) = if let Some(table) = tables
            .iter()
            .find(|t| t["queries"].get(&args.case_id).is_some())
        {
            (table, table["queries"][&args.case_id].clone())
        } else {
            let (fixture, query) = args.case_id.rsplit_once('.').ok_or("invalid case ID")?;
            let table = tables
                .iter()
                .find(|t| t["id"] == fixture)
                .ok_or("unknown fixture")?;
            let source = array(&manifest["sources"])?
                .iter()
                .find(|s| s["scale_factor"] == table["scale_factor"])
                .ok_or("unknown source scale")?;
            (
                table,
                source[if fixture.starts_with("wide.") {
                    "wide_queries"
                } else {
                    "queries"
                }][query]
                    .clone(),
            )
        };
        (table, sql, json!({"comparison_revision":2}))
    };
    text(&sql)?;
    let location = resolve(&fixtures.join(text(&table["path"])?))?;
    require(
        location.starts_with(&fixtures),
        "table path escapes fixture root",
    )?;
    let mut payload = json!({"table_uri":url::Url::from_file_path(location).map_err(|_| "invalid table URI")?.as_str(),
        "snapshot_version":table["snapshot_version"],"case_id":args.case_id,"canonical_sql":sql,
        "fixture_manifest_sha256":digest(&manifest_path)?,
        "profile":manifest.get("profile").cloned().unwrap_or_else(|| json!(format!("production-{}", manifest["mode"].as_str().unwrap_or("unknown")))),
        "execution_mode":args.execution_mode,"purpose":"validation",
        "resource_budget":{"worker_threads":8,"max_blocking_threads":64,"target_partitions":8,"batch_rows":8192,
            "datafusion_pool_bytes":4u64*1024u64.pow(3),"process_memory_bytes":8u64*1024u64.pow(3),"logical_cpus":8},
        "correctness_file":null,"campaign_id":null,"run_id":"pending","repetition":null,"order":null});
    payload
        .as_object_mut()
        .unwrap()
        .extend(comparison.as_object().ok_or("invalid comparison")?.clone());
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selective_read::storage::save;

    #[test]
    fn public_requests_bind_queries_and_paths_without_protocol_documents() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path();
        let fixtures = root.join("fixtures");
        let harness = root.join("repo/benches/selective_read");
        fs::create_dir(&fixtures)?;
        fs::create_dir_all(&harness)?;
        let mut manifest = json!({"status":"complete","protocol":"selective-read-v1","profile":"smoke",
            "tables":[{"id":"wide.clustered","path":"data space","snapshot_version":0,"scale_factor":0.01,
                       "queries":{"row-groups.select":"SELECT control"}}],
            "sources":[{"scale_factor":0.01,"wide_queries":{"eq1":"SELECT wide"}}]});
        let input = |case: &str| Request {
            fixtures: fixtures.clone(),
            case_id: case.into(),
            execution_mode: "reuse".into(),
            workload: None,
            harness: harness.clone(),
        };
        let write = |value: &Value| {
            fs::write(
                fixtures.join("manifest.json"),
                serde_json::to_vec(value).unwrap(),
            )
        };
        save(&fixtures.join("manifest.json"), &manifest)?;
        let request = run(input("wide.clustered.eq1"))?;
        assert_eq!(request["canonical_sql"], "SELECT wide");
        assert!(text(&request["table_uri"])?.ends_with("/data%20space"));
        assert!(request.get("protocol_sha256").is_none());
        assert_eq!(request["comparison_revision"], 2);
        assert_eq!(
            run(input("row-groups.select"))?["canonical_sql"],
            "SELECT control"
        );
        assert!(run(input("missing.case")).is_err());
        assert!(run(input("wide.clustered.unknown")).is_err());
        manifest["tables"][0]["path"] = json!("../outside");
        write(&manifest)?;
        assert!(run(input("row-groups.select")).is_err());
        fs::create_dir(root.join("outside"))?;
        std::os::unix::fs::symlink(root.join("outside"), fixtures.join("link"))?;
        manifest["tables"][0]["path"] = json!("link/reclaimed");
        write(&manifest)?;
        assert!(run(input("row-groups.select")).is_err());
        manifest["profile"] = json!("large");
        write(&manifest)?;
        assert!(
            run(input("row-groups.select"))
                .unwrap_err()
                .to_string()
                .contains("explicit workload")
        );
        manifest["status"] = json!("preparing");
        write(&manifest)?;
        assert!(run(input("row-groups.select")).is_err());
        Ok(())
    }
}
