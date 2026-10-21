use std::fs;

use super::*;
use crate::selective_read::{
    report::batch,
    storage::{read_json, save},
    validation::Worker,
};

const ORACLE_FIELDS: [&str; 8] = [
    "source_rows",
    "qualifying_rows",
    "output_rows",
    "qualifying_selectivity",
    "predicate_steps",
    "active_files",
    "candidate_files",
    "matching_files",
];

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub(in crate::selective_read) enum Request {
    Catalog {
        harness: PathBuf,
    },
    Load {
        path: PathBuf,
        fixtures: PathBuf,
        harness: PathBuf,
    },
}

pub(in crate::selective_read) fn run(request: Request) -> Result<Value> {
    match request {
        Request::Catalog { harness } => catalog(&harness),
        Request::Load {
            path,
            fixtures,
            harness,
        } => load(&path, &fixtures, &harness),
    }
}

#[derive(clap::Args, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Prepare {
    #[arg(long)]
    fixtures: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    references: Option<PathBuf>,
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

#[derive(clap::Args, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Report {
    #[arg(long)]
    campaign: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

fn catalog(harness: &Path) -> Result<Value> {
    let value = read_json(&harness.join("query-matrix.json"))?;
    require(
        value["format"] == "selective-read-query-matrix-v1" && value["comparison_revision"] == 2,
        "unsupported query catalog",
    )?;
    for source in object(&value["scales"])?.values() {
        for queries in object(&source["queries"])?.values() {
            for query in object(queries)?.values() {
                let expressions = object(&query["native_expressions"])?;
                require(
                    !text(&query["sql"])?.trim().is_empty()
                        && expressions
                            .keys()
                            .map(String::as_str)
                            .collect::<BTreeSet<_>>()
                            == BTreeSet::from(["daft", "polars"])
                        && expressions
                            .values()
                            .all(|v| v.as_object().is_some_and(|o| !o.is_empty())),
                    "query needs SQL and native expression descriptions",
                )?;
            }
        }
    }
    Ok(value)
}

fn case_parts(case: &str) -> Result<(&str, &str, &str)> {
    let parts: Vec<_> = case.split('.').collect();
    require(
        parts.len() == 3
            && ["li", "wide"].contains(&parts[0])
            && ["clustered", "shuffled"].contains(&parts[1]),
        "unknown public query case",
    )?;
    Ok((parts[0], parts[1], parts[2]))
}

fn shape(case: &str, frozen: &Value) -> Result<Value> {
    let (family, layout, name) = case_parts(case)?;
    let mut row = object(field(&frozen["shapes"][family], name)?)?.clone();
    let projection = array(&row["projection"])?;
    let predicate_only: Vec<_> = array(&row["predicate_columns"])?
        .iter()
        .filter(|column| !projection.contains(column))
        .cloned()
        .collect();
    let width = projection.len();
    row.extend(
        object(
            &json!({"case_id":case,"family":family,"layout":layout,"projection_columns":width,
        "predicate_only_columns":predicate_only}),
        )?
        .clone(),
    );
    Ok(json!(row))
}

fn query(case: &str, frozen: &Value, manifest: &Value) -> Result<Value> {
    let (family, layout, name) = case_parts(case)?;
    let table = array(&manifest["tables"])?
        .iter()
        .find(|t| t["id"] == format!("{family}.{layout}"))
        .ok_or("missing matrix table")?;
    let scale_value = table["scale_factor"]
        .as_f64()
        .ok_or("unknown public scale")?;
    let scale = if scale_value == 0.01 {
        "0.01"
    } else if scale_value == 1.0 {
        "1"
    } else if scale_value == 10.0 {
        "10"
    } else {
        return Err("unknown public scale".into());
    };
    let source = array(&manifest["sources"])?
        .iter()
        .find(|s| s["scale_factor"].as_f64() == Some(scale_value))
        .ok_or("missing matrix source")?;
    let expected = if manifest["profile"] == "smoke" {
        "0.01"
    } else if manifest["profile"] == "report" && family == "li" {
        "10"
    } else {
        "1"
    };
    require(
        scale == expected && table["snapshot_version"] == 0,
        "fixture scale/snapshot differs from the public matrix",
    )?;
    let frozen_source = &frozen["scales"][scale];
    let query = field(&frozen_source["queries"][family], name)?;
    let sql = field(
        &source[if family == "wide" {
            "wide_queries"
        } else {
            "queries"
        }],
        name,
    )?;
    require(
        source["in_literals"] == frozen_source["in_literals"]
            && source["rows"] == frozen_source["source_rows"]
            && *sql == query["sql"],
        "source literals/rows/SQL differ from checked-in catalog",
    )?;
    let literals = array(&source["in_literals"])?;
    let literals = match text(&frozen["shapes"][family][name]["predicate"])? {
        "eq2-in20" => literals.as_slice(),
        "eq2-in1" => &literals[..literals.len().min(1)],
        _ => &[],
    };
    Ok(
        json!({"scale":scale,"fixture_id":table["id"],"fixture_path":table["path"],"snapshot_version":table["snapshot_version"],
        "in_literals":literals,"canonical_sql":sql,"native_expressions":query["native_expressions"]}),
    )
}

fn cases(frozen: &Value) -> Result<Vec<String>> {
    let mut cases = Vec::new();
    for family in ["li", "wide"] {
        for layout in ["clustered", "shuffled"] {
            for name in object(&frozen["shapes"][family])?.keys() {
                cases.push(format!("{family}.{layout}.{name}"));
            }
        }
    }
    Ok(cases)
}

fn selected(value: &Value, fields: &[&str]) -> Result<Value> {
    Ok(json!(
        fields
            .iter()
            .map(|name| Ok((name.to_string(), field(value, name)?.clone())))
            .collect::<Result<Map<_, _>>>()?
    ))
}

fn reference_build(metadata: &Value) -> Result<()> {
    crate::selective_read::oracle::check::build(metadata)?;
    require(
        metadata["comparison_revision"] == 2,
        "unsupported matrix comparison revision",
    )
}

fn check_loaded(
    value: &Value,
    frozen: &Value,
    manifest: &Value,
    manifest_hash: &str,
) -> Result<()> {
    require(
        value["format"] == "selective-read-prepared-matrix-v1"
            && value["fixture_manifest_sha256"] == manifest_hash
            && value["profile"] == manifest["profile"]
            && value["translation_locks"] == frozen["translation_locks"],
        "prepared matrix identity changed",
    )?;
    let expected: BTreeSet<_> = cases(frozen)?.into_iter().collect();
    let rows = array(&value["cases"])?;
    let actual = rows
        .iter()
        .map(|row| Ok(text(&row["case_id"])?.to_owned()))
        .collect::<Result<BTreeSet<_>>>()?;
    require(
        rows.len() == 30 && expected.len() == 30 && actual == expected,
        "matrix must contain exactly 30 public cases",
    )?;
    for row in rows {
        let case = text(&row["case_id"])?;
        require(
            object(&shape(case, frozen)?)?
                .iter()
                .all(|(k, v)| row.get(k) == Some(v)),
            "prepared shape changed",
        )?;
        match text(&row["status"])? {
            "prepared" => {
                require(
                    object(&query(case, frozen, manifest)?)?
                        .iter()
                        .all(|(k, v)| row.get(k) == Some(v)),
                    "prepared query changed",
                )?;
                let path = Path::new(text(&row["reference"])?).join("reference.json");
                require(
                    digest(&path)? == row["reference_metadata_sha256"],
                    "reference metadata changed",
                )?;
                let metadata = read_json(&path)?;
                reference_build(&metadata)?;
                require(
                    metadata["fixture_manifest_sha256"] == value["fixture_manifest_sha256"]
                        && [
                            "case_id",
                            "snapshot_version",
                            "canonical_sql",
                            "projection",
                            "limit",
                        ]
                        .iter()
                        .all(|k| metadata[k] == row[k])
                        && row["oracle"] == selected(&metadata, &ORACLE_FIELDS)?,
                    "reference identity/counts changed",
                )?;
            }
            "preparation_failed" => require(
                !text(&row["failure_reason"])?.is_empty(),
                "missing preparation failure reason",
            )?,
            _ => return Err("unknown preparation status".into()),
        }
    }
    Ok(())
}

pub(in crate::selective_read) fn load(
    path: &Path,
    fixtures: &Path,
    harness: &Path,
) -> Result<Value> {
    let frozen = catalog(harness)?;
    let manifest_path = fixtures.join("manifest.json");
    let value = read_json(path)?;
    check_loaded(
        &value,
        &frozen,
        &read_json(&manifest_path)?,
        &digest(&manifest_path)?,
    )?;
    Ok(value)
}

pub(crate) fn prepare(args: Prepare) -> Result<Value> {
    let harness = fs::canonicalize(&args.harness)?;
    let fixtures = fs::canonicalize(&args.fixtures)?;
    let frozen = catalog(&harness)?;
    let manifest_path = fixtures.join("manifest.json");
    let manifest_hash = digest(&manifest_path)?;
    let manifest = read_json(&manifest_path)?;
    fs::create_dir(&args.output)?;
    let output = fs::canonicalize(&args.output)?;
    let references = if let Some(path) = &args.references {
        fs::canonicalize(path)?
    } else {
        output.join("references")
    };
    let mut worker = Worker::start(&args.python, &harness, &output)?;
    let mut rows = Vec::new();
    for case in cases(&frozen)? {
        let mut row = shape(&case, &frozen)?;
        let result = (|| -> Result<()> {
            row.as_object_mut()
                .unwrap()
                .extend(object(&query(&case, &frozen, &manifest)?)?.clone());
            let reference = references.join(&case);
            let metadata = worker.call(&json!({"action":"prepare-reference","fixtures":fixtures,"case_id":case,
                "reference":reference,"reuse":args.references.is_some(),"sql":row["canonical_sql"]}))?;
            reference_build(&metadata)?;
            require(
                metadata["case_id"] == case
                    && metadata["canonical_sql"] == row["canonical_sql"]
                    && metadata["fixture_manifest_sha256"] == manifest_hash
                    && metadata["reference_sha256"]
                        == digest(&reference.join("reference.parquet"))?,
                "stale independent reference",
            )?;
            row.as_object_mut().unwrap().extend(object(&json!({"status":"prepared","reference":reference,
                "reference_metadata_sha256":digest(&reference.join("reference.json"))?,"oracle":selected(&metadata, &ORACLE_FIELDS)?}))?.clone());
            Ok(())
        })();
        if let Err(error) = result {
            row["status"] = json!("preparation_failed");
            row["failure_reason"] = json!(error.to_string());
        }
        eprintln!("{}", json!({"case_id":case,"status":row["status"]}));
        rows.push(row);
    }
    rows.sort_by(|a, b| a["case_id"].as_str().cmp(&b["case_id"].as_str()));
    let result = json!({"format":"selective-read-prepared-matrix-v1",
        "status":if rows.iter().all(|row| row["status"] == "prepared") {"complete"} else {"incomplete"},
        "catalog_sha256":digest(&harness.join("query-matrix.json"))?,"protocol_sha256":frozen["protocol_sha256"],
        "profile":manifest["profile"],"translation_locks":frozen["translation_locks"],
        "fixture_manifest_sha256":manifest_hash,"cases":rows});
    require(
        digest(&manifest_path)? == manifest_hash,
        "fixture manifest changed during preparation",
    )?;
    save(&output.join("matrix.json"), &result)?;
    Ok(result)
}

pub(crate) fn report(args: Report) -> Result<Value> {
    let config = read_json(&args.campaign.join("campaign.json"))?;
    require(
        config["matrix"].is_object(),
        "campaign did not use a prepared query matrix",
    )?;
    let prepared = load(
        Path::new(text(&config["matrix"]["path"])?),
        Path::new(text(&config["fixtures"])?),
        &args.harness,
    )?;
    batch::run(serde_json::from_value(
        json!({"campaign":args.campaign,"output":args.output,
        "format":{"kind":"matrix","prepared":prepared}}),
    )?)
}

#[cfg(test)]
mod tests;
