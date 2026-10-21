//! Untimed artifact checks and observation bookkeeping for Python SDK readers.

use super::{
    array, contract,
    file_digests::Digests,
    object, require,
    storage::{read_json, save},
    supervise::progress,
    text,
};
use crate::Result;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt::Write,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Request {
    Digest {
        path: PathBuf,
    },
    Runtime {
        directory: PathBuf,
        engine: Value,
        packages: BTreeMap<String, Value>,
        metadata: Value,
        executable: PathBuf,
    },
    CheckedBuild {
        executable: PathBuf,
        reader: String,
    },
    Identity {
        request: Value,
        reader: String,
        build_path: PathBuf,
        settings: Value,
        native_expression: Value,
    },
    Correctness {
        request: Value,
        identity: Value,
    },
    Observation {
        request: Value,
    },
    Finish {
        output: PathBuf,
        record: Value,
    },
    Export {
        output: PathBuf,
        index: u64,
        identity: Value,
    },
}

pub(super) fn run(request: Request) -> Result<Value> {
    let mut hashes = Digests::default();
    match request {
        Request::Digest { path } => Ok(json!(hashes.digest(&path)?)),
        Request::Runtime {
            directory,
            engine,
            packages,
            metadata,
            executable,
        } => runtime(
            &directory,
            engine,
            packages,
            metadata,
            &executable,
            &mut hashes,
        ),
        Request::CheckedBuild { executable, reader } => {
            let executable = executable.canonicalize()?;
            let directory = executable.parent().ok_or("reader has no directory")?;
            let path = directory.join("build.json");
            let build = read_json(&path)?;
            require(
                build["reader_id"] == reader
                    && build["executable_sha256"] == hashes.digest(&executable)?
                    && build["lockfile_sha256"] == hashes.digest(&directory.join("lock.json"))?,
                "stale runner build",
            )?;
            for (name, expected) in object(&build["bundled_sha256"])? {
                require(
                    hashes.digest(&directory.join(name))? == *expected,
                    "stale runner build",
                )?;
            }
            Ok(json!([path, build]))
        }
        Request::Identity {
            request,
            reader,
            build_path,
            settings,
            native_expression,
        } => contract::identity(
            &request,
            &reader,
            &read_json(&build_path)?,
            settings,
            native_expression,
            &hashes.digest(&build_path)?,
        ),
        Request::Correctness { request, identity } => {
            if request["purpose"] != "timing" {
                return Ok(json!({"status":"not_checked","reason":"untimed invocation"}));
            }
            let path = Path::new(
                request["correctness_file"]
                    .as_str()
                    .filter(|p| !p.is_empty())
                    .ok_or("timing requires a correctness certificate")?,
            );
            let rows =
                contract::correctness(&read_json(path)?, &identity, progress::count(&request)?)?;
            Ok(
                json!({"status":"passed","path":path,"artifact_sha256":hashes.digest(path)?,"expected_output_rows":rows}),
            )
        }
        Request::Observation { request } => observation(&request),
        Request::Finish { output, mut record } => {
            finish(&mut record)?;
            save(&output.join("record.json"), &record)?;
            Ok(record)
        }
        Request::Export {
            output,
            index,
            mut identity,
        } => {
            let result = format!("query-{index}.arrow");
            let name = format!("query-{index}.identity.json");
            identity["result_sha256"] = json!(hashes.digest(&output.join(&result))?);
            save(&output.join(&name), &identity)?;
            Ok(json!({"result":result,"identity":name}))
        }
    }
}

fn files_hash(files: &BTreeMap<String, String>) -> Result<String> {
    let mut encoded = String::new();
    // Preserve the existing Python ensure_ascii encoding of artifact path maps.
    for character in serde_json::to_string(files)?.chars() {
        if character >= '\u{7f}' {
            for unit in character.encode_utf16(&mut [0; 2]) {
                write!(encoded, "\\u{unit:04x}")?;
            }
        } else {
            encoded.push(character);
        }
    }
    Ok(format!("{:x}", Sha256::digest(encoded.as_bytes())))
}

fn runtime(
    directory: &Path,
    mut engine: Value,
    packages: BTreeMap<String, Value>,
    metadata: Value,
    executable: &Path,
    hashes: &mut Digests,
) -> Result<Value> {
    let lock = read_json(&directory.join("lock.json"))?;
    if !lock["spark_version"].is_null() {
        let mut jars = Map::new();
        for jar in array(&lock["jars"])? {
            let name = text(&jar["filename"])?;
            require(
                hashes.digest(&directory.join("jars").join(name))? == jar["sha256"],
                "JAR checksum mismatch",
            )?;
            jars.insert(name.to_owned(), jar["sha256"].clone());
        }
        engine["jar_sha256"] = json!(jars);
        for (name, field) in [
            ("spark", "spark_distribution_sha256"),
            ("java", "java_sha256"),
        ] {
            let root = directory.join(name);
            let mut files = BTreeMap::new();
            for path in crate::repository::files(&root)? {
                if path.extension().is_none_or(|e| e != "pyc") {
                    files.insert(
                        path.strip_prefix(&root)?
                            .to_str()
                            .ok_or("non-UTF-8 runtime path")?
                            .to_owned(),
                        hashes.digest(&path)?,
                    );
                }
            }
            engine[field] = json!(files_hash(&files)?);
        }
    }
    let mut installed = Map::new();
    let mut versions = BTreeMap::new();
    for (name, mut package) in packages {
        let mut files = BTreeMap::new();
        for (relative, path) in object(&package["files"])? {
            let path = Path::new(text(path)?);
            if path.is_file() {
                files.insert(relative.clone(), hashes.digest(path)?);
            }
        }
        package
            .as_object_mut()
            .ok_or("invalid Python distribution")?
            .remove("files");
        package["installed_files_sha256"] = json!(files_hash(&files)?);
        versions.insert(name.clone(), text(&package["version"])?.to_owned());
        installed.insert(name, package);
    }
    let pinned = array(&lock["wheels"])?
        .iter()
        .map(|wheel| {
            Ok((
                text(&wheel["name"])?.to_owned(),
                text(&wheel["version"])?.to_owned(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    require(
        versions == pinned,
        "installed packages differ from the lock",
    )?;
    engine
        .as_object_mut()
        .ok_or("invalid engine runtime")?
        .extend(object(&metadata)?.clone());
    engine["packages"] = json!(installed);
    engine["python_executable_sha256"] = json!(hashes.digest(executable)?);
    Ok(engine)
}

fn observation(request: &Value) -> Result<Value> {
    let mut record = json!({"format":"selective-read-observation-v1", "status":"success", "failure_reason":null,
        "native_phases":{"planning_ns":null,"scan_ns":null,"unavailable_reason":"native APIs do not expose comparable separate planning/scan clocks"},
        "diagnostic_events":[],"diagnostic_session_ns":null,"phase":"setup","queries":[],"partial_query":null,"provider_evidence":null,
        "capability":{"status":"not_checked","scope":"requested query and snapshot"},"correctness":null,
        "external_metrics":{"requests":null,"response_bytes":null,"touched_parquet_objects":null,"process_cpu_ns":null,"peak_rss_bytes":null,
            "reason":"storage observer and process scheduler are separate roadmap slices"},
        "external_resource_limits":{"cpu_affinity":null,"process_memory_bytes":null,"reason":"launcher must enforce and record the CPU affinity and process memory limit"}});
    for name in [
        "open_query_ns",
        "initialization_ns",
        "session_elapsed_ns",
        "cleanup_ns",
        "initialization_plus_query1_ns",
        "initialization_plus_all_queries_ns",
    ] {
        record[name] = Value::Null;
    }
    for name in [
        "campaign_id",
        "run_id",
        "repetition",
        "order",
        "profile",
        "purpose",
        "execution_mode",
        "table_uri",
        "canonical_sql",
    ] {
        record[name] = request
            .get(name)
            .ok_or_else(|| format!("missing request field: {name}"))?
            .clone();
    }
    if request["validation_diagnostics"] == true {
        record["validation_diagnostics"] = json!(true);
    }
    Ok(record)
}

fn finish(record: &mut Value) -> Result<()> {
    if record["purpose"] == "timing" && record["capability"]["status"] == "supported" {
        let queries = array(&record["queries"])?;
        let durations = queries
            .iter()
            .map(|q| q["completion_ns"].as_u64().ok_or("missing query clock"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let first = *durations.first().ok_or("missing completed query")?;
        if record["status"] == "success"
            && queries
                .iter()
                .map(|q| &q["output_rows"])
                .ne(array(&record["correctness"]["expected_output_rows"])?.iter())
        {
            record["status"] = json!("validation_failed");
            record["failure_reason"] =
                json!("timed output row count differs from the validated result");
        }
        if record["execution_mode"] == "reuse" {
            let initialization = record["initialization_ns"]
                .as_u64()
                .ok_or("missing initialization clock")?;
            record["initialization_plus_query1_ns"] = json!(
                initialization
                    .checked_add(first)
                    .ok_or("query clock overflow")?
            );
            record["initialization_plus_all_queries_ns"] = json!(
                durations
                    .into_iter()
                    .try_fold(initialization, u64::checked_add)
                    .ok_or("query clock overflow")?
            );
        } else {
            record["open_query_ns"] = json!(first);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
