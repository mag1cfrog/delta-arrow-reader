use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::{array, field, fixture_tables, object, require, text};
use crate::{Result, artifacts::reader_sha256, selective_read_build::digest};

pub(super) mod files;
pub(super) mod fixtures;
mod large;
pub(super) mod matrix;
pub(super) mod planner;
pub(super) mod production;
pub(super) mod wide;

const ORIGINAL: [&str; 16] = [
    "l_orderkey",
    "l_partkey",
    "l_suppkey",
    "l_linenumber",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
    "l_commitdate",
    "l_receiptdate",
    "l_shipinstruct",
    "l_shipmode",
    "l_comment",
];
const PREDICATE: &str =
    "l_shipdate = DATE '1995-03-15' AND l_shipmode = 'AIR' AND l_linenumber IN (1)";

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Request {
    Define(Box<production::Define>),
    Resolve {
        path: PathBuf,
        fixtures: PathBuf,
        case: String,
        harness: PathBuf,
    },
    Shapes {
        file_target_mib: u64,
        page_rows: u64,
        page_bytes: u64,
        write_batch_rows: u64,
    },
    Cases,
    LayoutQuery {
        shape: Value,
    },
    LayoutGeometry {
        shape: Value,
        statistics: Value,
    },
    LargeShapes,
    LargeDefinitions {
        family: String,
        pilot_case: Option<String>,
    },
    LargeQuery {
        case: String,
        manifest: Value,
    },
    FileGeometry {
        table: Value,
        files: u64,
    },
    WideGeometry {
        manifest: Value,
        large: bool,
        smoke: bool,
    },
    Query {
        case: String,
        manifest: Value,
    },
    Sampling {
        stage: String,
        cases: BTreeSet<String>,
        modes: BTreeSet<String>,
    },
}

pub(super) fn run(request: Request) -> Result<Value> {
    match request {
        Request::Define(args) => production::define(*args),
        Request::Resolve {
            path,
            fixtures,
            case,
            harness,
        } => resolve(&path, &fixtures, &case, &harness),
        Request::Shapes {
            file_target_mib,
            page_rows,
            page_bytes,
            write_batch_rows,
        } => shapes(file_target_mib, page_rows, page_bytes, write_batch_rows),
        Request::Cases => Ok(cases()),
        Request::LayoutQuery { shape } => Ok(json!(planner::layout_query(&shape)?)),
        Request::LayoutGeometry { shape, statistics } => {
            planner::layout_geometry(&shape, &statistics)
        }
        Request::LargeShapes => Ok(large::shapes()),
        Request::LargeDefinitions { family, pilot_case } => {
            large::definitions(&family, pilot_case.as_deref())
        }
        Request::LargeQuery { case, manifest } => large::query(&case, &manifest),
        Request::FileGeometry { table, files } => {
            files::geometry(&table, files)?;
            Ok(Value::Null)
        }
        Request::WideGeometry {
            manifest,
            large,
            smoke,
        } => wide::geometry(&manifest, large, smoke),
        Request::Query { case, manifest } => query(&case, &manifest),
        Request::Sampling {
            stage,
            cases,
            modes,
        } => {
            sampling(&stage, &cases, &modes)?;
            Ok(Value::Null)
        }
    }
}

fn identity(value: &Value, sha256: &str) -> Result<Value> {
    let mut identity = value.clone();
    identity["workload_manifest_sha256"] = json!(sha256);
    identity["workload_contract"] = super::contract::workload(value)?;
    super::contract::comparison(&identity)
}

pub(super) fn load(path: &Path, harness: &Path) -> Result<(Value, Value)> {
    let bytes = std::fs::read(path)?;
    let value: Value = serde_json::from_slice(&bytes)?;
    let identity = identity(&value, &reader_sha256(bytes.as_slice())?)?;
    let value = if value["format"] == "selective-read-production-workload-v1" {
        production::load(value, harness)?
    } else {
        large::load(value, harness)?
    };
    Ok((value, identity))
}

pub(super) fn binding(value: &Value, fixtures: &Path, case: &str) -> Result<Value> {
    if matches!(value["comparison_revision"].as_u64(), Some(5 | 6)) {
        production::binding(value, fixtures, case)
    } else {
        large::binding(value, fixtures, case)
    }
}

pub(super) fn resolve(path: &Path, fixtures: &Path, case: &str, harness: &Path) -> Result<Value> {
    let path = path.canonicalize()?;
    let (workload, identity) = load(&path, harness)?;
    let row = binding(&workload, fixtures, case)?;
    Ok(json!({"path":path,"workload":workload,"identity":identity,"case":row}))
}

fn payloads() -> Vec<String> {
    (0..64).map(|n| format!("payload_{n:02}")).collect()
}

fn shapes(file_target: u64, page_rows: u64, page_bytes: u64, batch_rows: u64) -> Result<Value> {
    require(
        [256, 512].contains(&file_target)
            && [2048, 20000].contains(&page_rows)
            && [8192, 65536, 1048576].contains(&page_bytes)
            && [128, 1024].contains(&batch_rows),
        "unsupported production layout",
    )?;
    let mut shapes = Map::new();
    for (name, files, stripes, extra) in [("q2", 130, 5, 336), ("q4", 60, 6, 10)] {
        let mut projection: Vec<String> = [
            "l_orderkey",
            "l_linenumber",
            "l_shipdate",
            "l_shipmode",
            "l_partkey",
        ]
        .into_iter()
        .map(str::to_owned)
        .chain(payloads())
        .collect();
        if name == "q4" {
            projection.extend(["l_suppkey".into(), "l_quantity".into()]);
        }
        shapes.insert(name.into(), json!({"files":files * (512 / file_target), "stripes":stripes, "extra_numeric_columns":extra,
            "file_target_mib":file_target, "row_group_rows":131072, "data_page_rows":page_rows, "data_page_bytes":page_bytes,
            "write_batch_rows":batch_rows, "dictionary":false, "projection":projection, "stored_columns":80 + extra,
            "canonical_sql":format!("SELECT {} FROM bench WHERE {PREDICATE}", projection.join(", "))}));
    }
    Ok(json!(shapes))
}

pub(super) fn cases() -> Value {
    let mut cases = Map::new();
    for shape in ["q2", "q4"] {
        for layout in ["localized", "scattered"] {
            for dv in [false, true] {
                cases.insert(
                    format!("production.{shape}.{layout}{}", if dv { ".dv" } else { "" }),
                    json!({"shape":shape,"layout":layout,"deletion_vectors":dv}),
                );
            }
        }
    }
    json!(cases)
}

fn count(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| "expected a nonnegative integer".into())
}

fn table<'a>(manifest: &'a Value, case: &str) -> Result<&'a Value> {
    fixture_tables(manifest)?
        .iter()
        .find(|t| t["id"] == case)
        .ok_or_else(|| format!("missing production table: {case}").into())
}

pub(super) fn query(case: &str, manifest: &Value) -> Result<Value> {
    let all = cases();
    let definition = field(&all, case)?;
    let shape_name = text(&definition["shape"])?;
    let table = table(manifest, case)?;
    let mut declared = if let Some(shapes) = manifest.get("shape_definitions") {
        field(shapes, shape_name)?
    } else {
        field(manifest, "shape_definition")?
    }
    .clone();
    if declared.get("data_page_bytes").is_none() {
        require(
            table["writer"]["data_page_bytes"] == 1048576,
            "legacy shape requires its recorded 1 MiB page target",
        )?;
        declared["data_page_bytes"] = table["writer"]["data_page_bytes"].clone();
    }
    let definitions = shapes(
        count(&declared["file_target_mib"])?,
        count(&declared["data_page_rows"])?,
        count(&declared["data_page_bytes"])?,
        count(&declared["write_batch_rows"])?,
    )?;
    let shape = &definitions[shape_name];
    require(
        object(shape)?
            .iter()
            .all(|(k, v)| declared.get(k) == Some(v)),
        "production shape definition changed",
    )?;
    require(
        matches!(
            manifest["protocol"].as_str(),
            Some("selective-read-production-fixtures-v1" | "selective-read-production-pairs-v1")
        ) && matches!(manifest["mode"].as_str(), Some("probe" | "generate")),
        "unknown production fixture/definition",
    )?;
    let source = array(&manifest["sources"])?
        .iter()
        .find(|s| s["scale_factor"].as_f64() == Some(10.0))
        .ok_or("missing SF10 source")?;
    let dv = definition["deletion_vectors"]
        .as_bool()
        .ok_or("missing deletion-vector flag")?;
    require(
        table["deletion_vectors"] == dv
            && table["snapshot_version"].as_u64() == Some(u64::from(dv))
            && table["layout"] == definition["layout"]
            && table["scale_factor"].as_f64() == Some(10.0),
        "wrong production snapshot",
    )?;
    let columns = array(&table["schema"]["fields"])?
        .iter()
        .map(|f| Ok(text(&f["name"])?.to_owned()))
        .collect::<Result<Vec<_>>>()?;
    let stored: Vec<_> = ORIGINAL
        .into_iter()
        .map(str::to_owned)
        .chain(payloads())
        .chain((0..count(&shape["extra_numeric_columns"])?).map(|n| format!("metric_{n:03}")))
        .collect();
    require(
        columns == stored && json!(columns.len()) == shape["stored_columns"],
        "wrong stored production columns",
    )?;
    let files = array(&table["files"])?;
    let sum = |key: &str| -> Result<u64> {
        files.iter().try_fold(0u64, |total, f| {
            total
                .checked_add(count(&f[key])?)
                .ok_or_else(|| "fixture size overflow".into())
        })
    };
    require(
        table["file_count"] == files.len()
            && files.len() == array(&table["file_evidence"])?.len()
            && table["rows"] == sum("rows")?
            && table["bytes"] == sum("bytes")?,
        "wrong physical inventory",
    )?;
    require(
        table["file_target_mib"] == shape["file_target_mib"]
            && [
                "row_group_rows",
                "data_page_rows",
                "data_page_bytes",
                "write_batch_rows",
                "dictionary",
            ]
            .iter()
            .all(|key| table["writer"][*key] == shape[*key]),
        "production page/group settings changed",
    )?;
    if manifest["mode"] == "generate" {
        require(
            table["file_count"] == shape["files"] && table["rows"] == source["rows"],
            "incomplete full production fixture",
        )?;
    } else {
        require(
            !files.is_empty()
                && (files.len() as u64) < count(&shape["files"])?
                && count(&table["rows"])? > 0
                && count(&table["rows"])? < count(&source["rows"])?,
            "probe must retain its reduced geometry",
        )?;
    }
    if dv {
        let base = self::table(manifest, case.strip_suffix(".dv").ok_or("invalid DV case")?)?;
        let mut files = files.clone();
        for file in &mut files {
            if file
                .as_object_mut()
                .ok_or("invalid fixture file")?
                .remove("deletion_vector")
                .is_some_and(|dv| !dv.is_null())
            {
                file["delta_stats"]
                    .as_object_mut()
                    .ok_or("invalid Delta statistics")?
                    .remove("tightBounds");
            }
        }
        require(
            json!(files) == base["files"]
                && table["schema"] == base["schema"]
                && table["file_evidence"] == base["file_evidence"],
            "DV pair changed Parquet data or geometry",
        )?;
    }
    let sql = text(&shape["canonical_sql"])?;
    let mut result = definition.clone();
    result.as_object_mut().ok_or("invalid workload definition")?.extend(object(&json!({"case_id":case, "role":"production", "query_case_id":case, "fixture_id":case,
        "fixture_path":table["path"], "snapshot_version":table["snapshot_version"], "scale_factor":10, "source_rows":source["rows"],
        "physical_rows":table["rows"], "fixture_mode":manifest["mode"], "in_literals":[1], "projection":shape["projection"],
        "projection_columns":array(&shape["projection"])?.len(), "predicate":"production-eq2-in1",
        "predicate_columns":["l_shipdate","l_shipmode","l_linenumber"], "predicate_only_columns":[], "limit":null,
        "canonical_sql":sql, "writer":table["writer"],
        "source_parent_manifest_sha256":field(manifest, "source_parent_manifest_sha256")?,
        "geometry":{"files":table["file_count"], "stored_columns":columns.len(), "physical_bytes":table["bytes"],
            "file_bytes":files.iter().map(|f| f["bytes"].clone()).collect::<Vec<_>>(),
            "row_groups":files.iter().map(|f| Ok(array(&f["row_groups"])?.len())).collect::<Result<Vec<_>>>()?}
    }))?.clone());
    Ok(result)
}

fn sampling(stage: &str, selected: &BTreeSet<String>, modes: &BTreeSet<String>) -> Result<()> {
    require(
        matches!(stage, "pilot" | "formal"),
        "unknown production sampling stage",
    )?;
    if stage == "formal" {
        let all = cases();
        require(
            modes == &BTreeSet::from(["generate".into()])
                && !selected.is_empty()
                && selected.iter().all(|case| {
                    all.get(case).is_some()
                        && selected.contains(case.strip_suffix(".dv").unwrap_or(case))
                        && selected
                            .contains(&format!("{}.dv", case.strip_suffix(".dv").unwrap_or(case)))
                }),
            "formal sampling requires complete full no-DV/DV pairs",
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
