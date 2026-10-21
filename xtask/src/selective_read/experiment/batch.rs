use super::*;
use crate::{
    selective_read::validation::{Affinity, Worker},
    selective_read_build::digest,
};

#[derive(clap::Args)]
struct Common {
    #[arg(long)]
    state: PathBuf,
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    table: String,
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

#[derive(clap::Args)]
pub(crate) struct Numeric {
    #[command(flatten)]
    common: Common,
    #[arg(long)]
    baseline: PathBuf,
    #[arg(long)]
    candidate: PathBuf,
    #[arg(long = "type", value_parser = ["int32", "int64", "float", "double", "decimal20", "decimal38"])]
    column_type: String,
    #[arg(long, default_value = "sparse", value_parser = ["sparse", "dense"])]
    selection: String,
}

#[derive(clap::Args)]
pub(crate) struct Length {
    #[command(flatten)]
    common: Common,
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    cost_profile: Option<PathBuf>,
    #[arg(long, value_parser = ["grouped", "scattered", "dense"])]
    selection: String,
}

fn prepare(args: &mut Common) -> Result<(Worker, Affinity)> {
    for path in [&mut args.state, &mut args.fixture, &mut args.harness] {
        *path = fs::canonicalize(&*path)?;
    }
    fs::create_dir(&args.output)?;
    args.output = fs::canonicalize(&args.output)?;
    let worker = Worker::start(&args.python, &args.harness, &args.output)?;
    let affinity = Affinity::pin(&storage::state(&args.state)?, &worker)?;
    Ok((worker, affinity))
}

fn verify(root: &Path, item: &Value) -> Result<()> {
    let path = fs::canonicalize(root.join(text(&item["path"])?))?;
    require(path.starts_with(root), "fixture object escapes its root")?;
    require(item["sha256"] == digest(&path)?, "fixture object changed")
}

fn length_objects(root: &Path, manifest: &Value) -> Result<Vec<PathBuf>> {
    require(
        manifest["format"] == "delta-length-network-v1",
        "unknown Delta-length fixture",
    )?;
    let mut seen = std::collections::BTreeSet::new();
    array(&manifest["objects"])?
        .iter()
        .map(|item| {
            let relative = Path::new(text(&item["path"])?);
            require(
                !relative.as_os_str().is_empty()
                    && relative
                        .components()
                        .all(|c| matches!(c, std::path::Component::Normal(_)))
                    && seen.insert(relative),
                "invalid or duplicate fixture object path",
            )?;
            verify(root, item)?;
            let path = root.join(relative);
            require(
                item["bytes"] == fs::metadata(&path)?.len(),
                "fixture object size changed",
            )?;
            Ok(path)
        })
        .collect()
}

pub(crate) fn upload_length(state: &Path, fixture: &Path) -> Result<Value> {
    let fixture = fs::canonicalize(fixture)?;
    let path = fixture.join("manifest.json");
    let manifest = storage::read_json(&path)?;
    let prefix = digest(&path)?;
    let objects = length_objects(&fixture, &manifest)?;
    let _lock = storage::exclusive(state)?;
    storage::verify_server(state)?;
    for (item, source) in array(&manifest["objects"])?.iter().zip(&objects) {
        storage::put_verified(
            state,
            &format!("{prefix}/{}", text(&item["path"])?),
            source,
            text(&item["sha256"])?,
            true,
        )?;
    }
    require(
        digest(&path)? == prefix,
        "fixture manifest changed during upload",
    )?;
    Ok(json!({"table_root":format!("s3://{}/{prefix}/",storage::BUCKET),"objects":objects.len()}))
}

fn read_expected(
    worker: &mut Worker,
    args: &Common,
    manifest: &Value,
    driver: &str,
    selection: &str,
    column_type: Option<&str>,
) -> Result<u64> {
    worker
        .call(
            &json!({"action":"experiment-reference", "fixture":args.fixture, "manifest":manifest,
        "driver":driver, "selection":selection, "column_type":column_type}),
        )?
        .as_u64()
        .ok_or_else(|| "invalid reference row count".into())
}

fn numeric_record(record: &Value, rows: u64) -> Result<()> {
    require(
        record["settings"]["warmup"] == "Automatic"
            && text(&record["settings"]["scan_options"])?
                .contains("experimental_intra_page_reads: true"),
        "numeric reader defaults changed",
    )?;
    let queries = array(&record["queries"])?;
    require(
        queries.len() == 2 && queries.iter().all(|q| q["rows"] == rows),
        "numeric output row count or query sequence differs",
    )
}

fn numeric_summary(samples: &[Value]) -> Result<Value> {
    let mut medians = serde_json::Map::new();
    for name in ["baseline", "candidate"] {
        let values = samples
            .iter()
            .filter(|s| s["build"] == name && s["phase"] == "timing")
            .map(|s| {
                s["record"]["queries"][0]["seconds"]
                    .as_f64()
                    .ok_or_else(|| "missing query time".into())
            })
            .collect::<Result<_>>()?;
        medians.insert(name.into(), json!(median(values)?));
    }
    Ok(json!({"samples":samples, "first_query_medians_seconds":medians}))
}

pub(in crate::selective_read) fn numeric(mut args: Numeric) -> Result<()> {
    args.baseline = fs::canonicalize(args.baseline)?;
    args.candidate = fs::canonicalize(args.candidate)?;
    let (mut worker, _affinity) = prepare(&mut args.common)?;
    let common = &args.common;
    let manifest = storage::read_json(&common.fixture.join("manifest.json"))?;
    require(
        manifest["format"] == "numeric-page-fixture-v1",
        "unknown numeric fixture",
    )?;
    require(
        manifest["log_sha256"]
            == digest(&common.fixture.join("_delta_log/00000000000000000000.json"))?,
        "fixture log changed",
    )?;
    for file in array(&manifest["files"])? {
        verify(&common.fixture, file)?;
    }
    verify(&common.fixture, &manifest["references"][&args.column_type])?;
    let rows = read_expected(
        &mut worker,
        common,
        &manifest,
        "numeric",
        &args.selection,
        Some(&args.column_type),
    )?;
    storage::save(
        &common.output.join("experiment.json"),
        &json!({"fixture_sha256":digest(&common.fixture.join("manifest.json"))?,
        "binaries":{"baseline":digest(&args.baseline)?, "candidate":digest(&args.candidate)?},
        "type":args.column_type, "selection":args.selection, "table_uri":common.table,
        "network":if common.table.starts_with("file:") { None } else { network::config(&common.state)? },
        "generator":manifest["generator"], "value_checker_sha256":digest(&std::env::current_exe()?)?,
        "validation_sha256":digest(&std::env::current_exe()?)?, "controller_sha256":digest(&std::env::current_exe()?)?,
        "primary_metric":"first fully consumed query after initialization", "pairs":3}),
    )?;
    let _lock = storage::exclusive(&common.state)?;
    storage::verify_server(&common.state)?;
    let mut samples = Vec::new();
    for (phase, repetition) in [
        ("validation", 0),
        ("timing", 0),
        ("timing", 1),
        ("timing", 2),
    ] {
        for name in if repetition % 2 == 0 {
            ["baseline", "candidate"]
        } else {
            ["candidate", "baseline"]
        } {
            let slot = common.output.join(format!("{name}-{phase}-{repetition}"));
            let result = super::run(Request {
                directory: common.state.clone(),
                output: slot.clone(),
                binary: if name == "baseline" {
                    args.baseline.clone()
                } else {
                    args.candidate.clone()
                },
                table: common.table.clone(),
                selection: args.selection.clone(),
                reader: Reader::NumericPages {
                    column_type: args.column_type.clone(),
                    validation: phase == "validation",
                },
            })?;
            let record = &result["record"];
            numeric_record(record, rows)?;
            if phase == "validation" {
                for index in 0..2 {
                    worker.call(&json!({"action":"experiment-check", "driver":"numeric", "path":slot.join(format!("reader/{index}.arrow"))}))?;
                }
                storage::save(
                    &slot.join("correctness.json"),
                    &json!({"status":"passed","comparison":"independent Arrow reference, exact bits"}),
                )?;
            }
            let sample = json!({"build":name, "phase":phase, "repetition":repetition, "record":record, "network":result["network"]});
            storage::save(&slot.join("sample.json"), &sample)?;
            println!(
                "{}",
                json!({"build":name,"phase":phase,"repetition":repetition,"status":"success"})
            );
            samples.push(sample);
        }
    }
    storage::save(
        &common.output.join("summary.json"),
        &numeric_summary(&samples)?,
    )
}

fn length_sample(
    args: &Length,
    worker: &mut Worker,
    expected: u64,
    profile: &Value,
    mode: &str,
    phase: (&str, u8),
    arrays: &str,
) -> Result<Value> {
    let common = &args.common;
    let (phase, repetition) = phase;
    let slot = common
        .output
        .join(format!("{mode}-{phase}-{repetition}-{arrays}"));
    let result = super::run(Request {
        directory: common.state.clone(),
        output: slot.clone(),
        binary: args.binary.clone(),
        table: common.table.clone(),
        selection: args.selection.clone(),
        reader: Reader::DeltaLength {
            mode: mode.into(),
            arrays: arrays.into(),
            phase: phase.into(),
            cost_profile: args.cost_profile.clone(),
        },
    })?;
    let record = &result["record"];
    if args.cost_profile.is_some() {
        require(
            record["cost_profile"] == *profile,
            "fixed cost profile changed",
        )?;
    }
    if phase == "validate" {
        worker.call(&json!({"action":"experiment-check","driver":"length","path":slot.join("reader/result.arrow")}))?;
    }
    require(record["output_rows"] == expected, "query row count differs")?;
    let sample = json!({"mode":mode, "phase":phase, "repetition":repetition, "arrays":arrays,
        "record":record, "query_network":result["network"], "process_resources":result["process_resources"]});
    storage::save(&slot.join("sample.json"), &sample)?;
    println!(
        "{}",
        json!({"mode":mode,"phase":phase,"repetition":repetition,"arrays":arrays,"status":"success"})
    );
    Ok(sample)
}

fn length_summary(samples: &[Value]) -> Result<Value> {
    let mut medians = serde_json::Map::new();
    for mode in ["ordinary", "experimental"] {
        let values = samples
            .iter()
            .filter(|s| s["mode"] == mode && s["phase"] == "timing")
            .map(|s| {
                s["record"]["query_seconds"]
                    .as_f64()
                    .ok_or_else(|| "missing query time".into())
            })
            .collect::<Result<_>>()?;
        medians.insert(mode.into(), json!(median(values)?));
    }
    Ok(json!({"samples":samples, "medians_seconds":medians}))
}

pub(in crate::selective_read) fn length(mut args: Length) -> Result<()> {
    args.binary = fs::canonicalize(args.binary)?;
    let (mut worker, _affinity) = prepare(&mut args.common)?;
    let common = &args.common;
    let profile = if let Some(path) = &args.cost_profile {
        storage::read_json(path)?
    } else {
        Value::Null
    };
    if args.cost_profile.is_some() {
        let path = common.output.join("cost-profile.json");
        storage::save(&path, &profile)?;
        args.cost_profile = Some(path);
    }
    let manifest = storage::read_json(&common.fixture.join("manifest.json"))?;
    require(
        manifest["format"] == "delta-length-network-v1",
        "unknown Delta-length fixture",
    )?;
    let prefix = digest(&common.fixture.join("manifest.json"))?;
    require(
        common.table.trim_end_matches('/') == format!("s3://{}/{prefix}", storage::BUCKET),
        "table must match the verified fixture",
    )?;
    let network =
        network::config(&common.state)?.ok_or("a recorded network profile is required")?;
    for item in array(&manifest["objects"])? {
        verify(&common.fixture, item)?;
    }
    verify(&common.fixture, &manifest["references"][&args.selection])?;
    let expected = read_expected(
        &mut worker,
        common,
        &manifest,
        "length",
        &args.selection,
        None,
    )?;
    let sources = ["src/bin/delta-length-query.rs", "src/delta_length_reader.rs", "src/delta_length_profile.rs",
        "src/delta_length_cost.rs", "src/delta_length_simulation.rs", "src/delta_length.rs", "Cargo.lock",
        "../../crates/delta-arrow-reader-core/src/reader/backend/direct_parquet/range_planning.rs",
        "../../crates/delta-arrow-reader-core/src/reader/backend/direct_parquet/cost_model/range_cost.rs"]
        .into_iter().map(|name| Ok((name.to_owned(), json!(digest(&common.harness.join(name))?)))).collect::<Result<serde_json::Map<_,_>>>()?;
    storage::save(
        &common.output.join("experiment.json"),
        &json!({"fixture_sha256":prefix, "binary_sha256":digest(&args.binary)?,
        "generator":manifest["generator"],
        "validation_sha256":digest(&std::env::current_exe()?)?, "controller_sha256":digest(&std::env::current_exe()?)?,
        "network":network, "source_sha256":sources,
        "cost_profile":if args.cost_profile.is_some() { json!({"basis":"fixed diagnostic control", "values":profile}) }
            else { json!("measured during each process initialization on the same client, including the request-duration distribution; incomplete measurements keep ordinary reads") },
        "selection":args.selection, "pairs":3, "primary_metric":"first fully consumed query including planning, predicate and DV",
        "acceptance":"all three pairs improve beyond the larger within-mode range; investigate >5% median or consistent smaller regressions",
        "policy":"uncompressed flat V1; >=1 MiB pages; <=1/8 selected rows; >=64 bytes/row; complete failed-discovery fallback <=105% modeled ordinary batch cost; >=25% byte saving and >10% modeled remaining-read saving after discovery"}),
    )?;
    let run = (|| -> Result<Vec<Value>> {
        let _lock = storage::exclusive(&common.state)?;
        storage::verify_server(&common.state)?;
        let mut samples = Vec::new();
        for arrays in ["arrays", "views"] {
            for mode in ["ordinary", "experimental"] {
                samples.push(length_sample(
                    &args,
                    &mut worker,
                    expected,
                    &profile,
                    mode,
                    ("validate", 0),
                    arrays,
                )?);
            }
        }
        for repetition in 0..3 {
            for mode in if repetition % 2 == 0 {
                ["ordinary", "experimental"]
            } else {
                ["experimental", "ordinary"]
            } {
                samples.push(length_sample(
                    &args,
                    &mut worker,
                    expected,
                    &profile,
                    mode,
                    ("timing", repetition),
                    "arrays",
                )?);
            }
        }
        Ok(samples)
    })();
    match run {
        Ok(samples) => storage::save(
            &common.output.join("summary.json"),
            &length_summary(&samples)?,
        ),
        Err(error) => {
            storage::save(
                &common.output.join("failure.json"),
                &json!({"error":error.to_string()}),
            )?;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_length_uploads_verify_all_objects_before_storage_access() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path();
        fs::write(root.join("part"), "fixture")?;
        let item = json!({"path":"part","bytes":7,"sha256":digest(&root.join("part"))?});
        let mut manifest = json!({"format":"delta-length-network-v1","objects":[item]});
        assert_eq!(length_objects(root, &manifest)?, [root.join("part")]);
        manifest["objects"] = json!([item, item]);
        assert!(length_objects(root, &manifest).is_err());
        for change in [
            json!({"path":"../outside"}),
            json!({"bytes":8}),
            json!({"sha256":"0".repeat(64)}),
        ] {
            let mut bad = item.clone();
            bad.as_object_mut()
                .unwrap()
                .extend(change.as_object().unwrap().clone());
            manifest["objects"] = json!([bad]);
            assert!(length_objects(root, &manifest).is_err());
        }
        manifest["format"] = json!("unknown");
        assert!(length_objects(root, &manifest).is_err());
        Ok(())
    }

    #[test]
    fn typed_experiments_require_both_queries_and_exclude_validation_from_medians() -> Result<()> {
        let mut record = json!({"settings":{"warmup":"Automatic","scan_options":"experimental_intra_page_reads: true"},
            "queries":[{"rows":43,"seconds":2.0},{"rows":43,"seconds":1.0}]});
        numeric_record(&record, 43)?;
        record["queries"][1]["rows"] = json!(42);
        assert!(numeric_record(&record, 43).is_err());
        record["queries"].as_array_mut().unwrap().pop();
        assert!(numeric_record(&record, 43).is_err());
        let samples: Vec<_> = ["baseline","candidate"].into_iter().flat_map(|build| [("validation",99.0),("timing",1.0),("timing",3.0),("timing",2.0)]
            .map(|(phase, seconds)| json!({"build":build,"phase":phase,"record":{"queries":[{"seconds":seconds},{"seconds":100.0}]}}))).collect();
        assert_eq!(
            numeric_summary(&samples)?["first_query_medians_seconds"],
            json!({"baseline":2.0,"candidate":2.0})
        );
        let length: Vec<_> = samples.iter().map(|sample| json!({"mode":if sample["build"] == "baseline" { "ordinary" } else { "experimental" },
            "phase":sample["phase"],"record":{"query_seconds":sample["record"]["queries"][0]["seconds"]}})).collect();
        assert_eq!(
            length_summary(&length)?["medians_seconds"],
            json!({"ordinary":2.0,"experimental":2.0})
        );
        Ok(())
    }
}
