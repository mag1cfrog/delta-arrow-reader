use super::*;
use crate::selective_read::campaign::{evidence, schedule};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::selective_read) struct Request {
    campaign: PathBuf,
    output: PathBuf,
    format: Format,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum Format {
    Matrix {
        prepared: Value,
    },
    Files {
        references: PathBuf,
        checked: Value,
    },
    Workload {
        frozen: Value,
        identity: Value,
        bindings: Map<String, Value>,
        protocols: PathBuf,
    },
}

fn select(value: &Value, fields: &[&str]) -> Result<Value> {
    Ok(Value::Object(
        fields
            .iter()
            .map(|key| Ok(((*key).into(), field(value, key)?.clone())))
            .collect::<Result<_>>()?,
    ))
}

fn csv(path: &Path, header: &[&str], rows: Vec<Vec<Value>>) -> Result<()> {
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::CRLF)
        .from_writer(
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?,
        );
    writer.write_record(header)?;
    for row in rows {
        writer.write_record(row.iter().map(|cell| match cell {
            Value::Null => String::new(),
            Value::String(text) => text.clone(),
            Value::Bool(value) => if *value { "True" } else { "False" }.into(),
            value => value.to_string(),
        }))?;
    }
    writer.flush()?;
    Ok(())
}

fn io_records(measurements: &Value) -> Result<Vec<&Value>> {
    array(&measurements["diagnostics"]["diagnostic"])?
        .iter()
        .filter(|row| row["status"] == "success" && !row["io"].is_null())
        .map(|row| {
            object(&row["io"])?;
            Ok(&row["io"])
        })
        .collect()
}

pub(in crate::selective_read) fn run(request: Request) -> Result<Value> {
    let Request {
        campaign,
        output,
        format,
    } = request;
    require(!output.exists(), "report output already exists")?;
    let config = read_json(&campaign.join("campaign.json"))?;
    let data = evidence::read_results(&campaign, "frozen campaign inputs changed")?;
    let (inventory, summary, observations, slots) =
        (&data[0], &data[1], array(&data[2])?, &data[3]);
    let observations_sha256 = digest(&campaign.join("observations.jsonl"))?;
    let summary_sha256 = digest(&campaign.join("summary.json"))?;
    match format {
        Format::Matrix { prepared } => {
            require(
                config["matrix"].is_object(),
                "campaign did not use a prepared query matrix",
            )?;
            require(
                digest(Path::new(text(&config["matrix"]["path"])?))? == config["matrix"]["sha256"],
                "campaign matrix changed",
            )?;
            let mut rows = Vec::new();
            let mut cells = Vec::new();
            for case in array(&prepared["cases"])? {
                let id = text(&case["case_id"])?;
                for reader in roster(&json!({}))? {
                    let gate = field(&inventory[id], reader)?;
                    let result = field(&summary["jobs"][id], reader)?;
                    let plans: Vec<_> = observations
                        .iter()
                        .filter(|row| {
                            row["job_id"] == id
                                && row["reader_id"] == reader
                                && row["stage"] == "plan"
                        })
                        .map(|row| select(row, &["run_id", "status", "artifacts"]))
                        .collect::<Result<_>>()?;
                    rows.push(json!({"case":case, "reader_id":reader, "gate":gate,
                        "timing":select(result, &["eligible", "scheduled_samples", "sample_statuses", "metrics",
                            "speedup_vs_dar", "first_batch_unavailable"])?,
                        "planning":{"candidate_files":null,
                            "unavailable_reason":"adapters retain native plans without a normalized file-selection counter", "plans":plans},
                        "diagnostics":field(result,"diagnostics")?, "observer_overhead":result["observer_overhead"]}));
                    let expected = &case["oracle"];
                    let clocks = &result["metrics"]["open_query_ns"];
                    let io = io_records(result)?;
                    cells.push(vec![
                        case["case_id"].clone(),
                        json!(reader),
                        prepared["profile"].clone(),
                        case["scale"].clone(),
                        case["layout"].clone(),
                        case["predicate_columns"].clone(),
                        case["projection_columns"].clone(),
                        case["predicate_only_columns"].clone(),
                        expected["qualifying_rows"].clone(),
                        expected["output_rows"].clone(),
                        expected["qualifying_selectivity"].clone(),
                        expected["active_files"].clone(),
                        expected["candidate_files"]
                            .as_array()
                            .map_or(Value::Null, |v| json!(v.len())),
                        expected["matching_files"]
                            .as_array()
                            .map_or(Value::Null, |v| json!(v.len())),
                        gate["status"].clone(),
                        result["eligible"].clone(),
                        result["scheduled_samples"].clone(),
                        result["sample_statuses"].clone(),
                        clocks["median"].clone(),
                        clocks["q1"].clone(),
                        clocks["q3"].clone(),
                        clocks["iqr"].clone(),
                        result["speedup_vs_dar"]["open_query_ns"]["value"].clone(),
                        json!(
                            io.iter()
                                .map(|r| array(&r["touched_parquet_objects"]).map(|a| a.len()))
                                .collect::<Result<Vec<_>>>()?
                        ),
                        json!(
                            io.iter()
                                .map(|r| field(r, "response_bytes"))
                                .collect::<Result<Vec<_>>>()?
                        ),
                        json!(
                            io.iter()
                                .map(|r| field(r, "requests"))
                                .collect::<Result<Vec<_>>>()?
                        ),
                    ]);
                }
            }
            fs::create_dir(&output)?;
            save(
                &output.join("matrix-report.json"),
                &json!({"status":summary["status"], "profile":prepared["profile"],
                "campaign_id":summary["campaign_id"], "campaign_summary_sha256":summary_sha256,
                "observations_sha256":observations_sha256,
                "scope":"30 TPC-H-derived scan cases, not an official TPC-H result or the full 46-case report", "rows":rows}),
            )?;
            csv(
                &output.join("matrix-report.csv"),
                &[
                    "case_id",
                    "reader",
                    "profile",
                    "scale",
                    "layout",
                    "predicate_columns",
                    "projected_columns",
                    "hidden_predicate_columns",
                    "qualifying_rows",
                    "output_rows",
                    "selectivity",
                    "active_files",
                    "oracle_candidate_files",
                    "oracle_matching_files",
                    "gate_status",
                    "eligible",
                    "scheduled_samples",
                    "sample_statuses",
                    "median_ns",
                    "q1_ns",
                    "q3_ns",
                    "iqr_ns",
                    "comparator_over_dar",
                    "diagnostic_touched_files",
                    "diagnostic_response_bytes",
                    "diagnostic_requests",
                ],
                cells,
            )?;
            Ok(json!({"status":summary["status"], "entries":rows.len()}))
        }
        Format::Files {
            references,
            checked,
        } => {
            let cases: Map<_, _> = array(&checked["cases"])?
                .iter()
                .map(|case| Ok((text(&case["case_id"])?.to_owned(), case.clone())))
                .collect::<Result<_>>()?;
            let expected: BTreeSet<_> = cases
                .keys()
                .map(|id| (id.as_str(), id.as_str(), "open"))
                .chain([("reuse.files4096", "files4096.eq2-in20", "reuse")])
                .collect();
            let jobs = array(&config["jobs"])?;
            let actual = jobs
                .iter()
                .map(|job| {
                    Ok((
                        text(&job["id"])?,
                        text(&job["case_id"])?,
                        text(&job["execution_mode"])?,
                    ))
                })
                .collect::<Result<BTreeSet<_>>>()?;
            require(actual == expected, "wrong file-organization campaign jobs")?;
            let mut rows = Vec::new();
            let mut cells = Vec::new();
            for job in jobs {
                let id = text(&job["id"])?;
                let oracle = cases
                    .get(text(&job["case_id"])?)
                    .ok_or("unknown file-organization case")?;
                for reader in roster(&json!({}))? {
                    let gate = field(&inventory[id], reader)?;
                    require(
                        gate["runnable"] != true
                            || gate["reference_sha256"] == oracle["reference_metadata_sha256"],
                        "gate used a different reference",
                    )?;
                    let mut plans = Vec::new();
                    for row in observations.iter().filter(|row| {
                        row["job_id"] == id && row["reader_id"] == reader && row["stage"] == "plan"
                    }) {
                        let events = row["observation"]
                            .get("diagnostic_events")
                            .map(array)
                            .transpose()?
                            .map(Vec::as_slice)
                            .unwrap_or_default();
                        let time = |name| -> Result<Option<i64>> {
                            events
                                .iter()
                                .find(|event| event["event"] == name)
                                .map(|event| {
                                    event["time_ns"]
                                        .as_i64()
                                        .ok_or_else(|| "invalid diagnostic timestamp".into())
                                })
                                .transpose()
                        };
                        let elapsed = time("query_start")?
                            .zip(time("snapshot_open")?)
                            .map(|(query, opened)| {
                                query
                                    .checked_sub(opened)
                                    .ok_or("diagnostic timestamp overflow")
                            })
                            .transpose()?;
                        plans.push(json!({"status":row["status"], "artifacts":row["artifacts"], "diagnostic_snapshot_open_ns":elapsed}));
                    }
                    let result = field(&summary["jobs"][id], reader)?;
                    rows.push(json!({"job":job, "reader_id":reader, "oracle":oracle, "gate":gate, "result":result,
                        "planning":{"plans":plans, "candidate_files":null, "planning_ns":null,
                            "unavailable_reason":"native plans retained; adapters do not expose comparable file-selection or isolated planning counters"}}));
                    let io = io_records(result)?;
                    let mut row = vec![
                        job["id"].clone(),
                        json!(reader),
                        gate["status"].clone(),
                        result["eligible"].clone(),
                        result["scheduled_samples"].clone(),
                        oracle["active_files"].clone(),
                        json!(array(&oracle["candidate_files"])?.len()),
                        json!(array(&oracle["matching_files"])?.len()),
                    ];
                    for clock in [
                        "open_query_ns",
                        "initialization_ns",
                        "initialization_plus_query1_ns",
                        "initialization_plus_all_queries_ns",
                    ] {
                        row.push(result["metrics"][clock]["median"].clone());
                    }
                    row.push(json!(
                        io.iter()
                            .map(|r| array(&r["touched_parquet_objects"]).map(|a| a.len()))
                            .collect::<Result<Vec<_>>>()?
                    ));
                    row.push(json!(
                        io.iter()
                            .map(|r| field(r, "by_class"))
                            .collect::<Result<Vec<_>>>()?
                    ));
                    cells.push(row);
                }
            }
            fs::create_dir_all(output.parent().ok_or("missing report parent")?)?;
            fs::create_dir(&output)?;
            save(
                &output.join("file-organizations-report.json"),
                &json!({"status":summary["status"], "profile":checked["profile"],
                "campaign_summary_sha256":summary_sha256, "observations_sha256":observations_sha256,
                "verification_sha256":digest(&references.join("file-organizations.json"))?, "tables":checked["tables"], "rows":rows}),
            )?;
            csv(
                &output.join("file-organizations-report.csv"),
                &[
                    "job",
                    "reader",
                    "gate_status",
                    "eligible",
                    "samples",
                    "active_files",
                    "oracle_candidates",
                    "matching_files",
                    "open_query_ns",
                    "initialization_ns",
                    "initialization_plus_query1_ns",
                    "initialization_plus_all_queries_ns",
                    "touched_parquet_objects",
                    "traffic_by_class",
                ],
                cells,
            )?;
            Ok(json!({"status":summary["status"], "entries":rows.len()}))
        }
        Format::Workload {
            frozen,
            identity,
            bindings,
            protocols,
        } => {
            let identity = comparison(&identity, &protocols)?;
            require(
                comparison(&config, &protocols)? == identity,
                "campaign workload changed",
            )?;
            require(
                comparison(summary, &protocols)? == identity
                    && array(slots)?
                        .iter()
                        .map(|slot| comparison(slot, &protocols))
                        .collect::<Result<Vec<_>>>()?
                        .iter()
                        .all(|value| *value == identity),
                "schedule/summary workload changed",
            )?;
            let ordered_inventory: Vec<_> = object(inventory)?
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            require(
                *slots
                    == schedule(
                        &ordered_inventory,
                        text(&config["campaign_id"])?,
                        &identity,
                        None,
                        None,
                        None,
                    )?,
                "schedule differs from the frozen ordering rules",
            )?;
            evidence::diagnostics(
                &config,
                &ordered_inventory,
                array(slots)?,
                observations,
                &protocols,
            )?;
            let readers = roster(&identity)?;
            let jobs: BTreeMap<_, _> = array(&config["jobs"])?
                .iter()
                .map(|job| Ok((text(&job["id"])?, job)))
                .collect::<Result<_>>()?;
            require(
                jobs.keys()
                    .copied()
                    .eq(object(inventory)?.keys().map(String::as_str))
                    && jobs
                        .keys()
                        .copied()
                        .eq(object(&summary["jobs"])?.keys().map(String::as_str)),
                "campaign inventory differs from jobs",
            )?;
            for (id, entries) in object(inventory)? {
                require(
                    object(entries)?
                        .keys()
                        .map(String::as_str)
                        .collect::<BTreeSet<_>>()
                        == readers.into_iter().collect(),
                    "all five reader statuses are required",
                )?;
                let job = jobs[id.as_str()];
                let case = text(&job["case_id"])?;
                require(
                    array(&frozen["cases"])?
                        .iter()
                        .any(|row| row["case_id"] == case)
                        && (job["execution_mode"] == "open" && id == case
                            || job["execution_mode"] == "reuse" && frozen["sessions"][id] == case),
                    "unknown workload job",
                )?;
            }
            let mut seen = BTreeSet::new();
            for row in observations {
                require(seen.insert(text(&row["run_id"])?), "duplicate observation")?;
                require(
                    comparison(row, &protocols)? == identity
                        && comparison(&row["request"], &protocols)? == identity,
                    "observation workload changed",
                )?;
                let job = jobs
                    .get(text(&row["job_id"])?)
                    .ok_or("unknown workload job")?;
                let expected = bindings
                    .get(text(&job["case_id"])?)
                    .ok_or("unknown workload case")?;
                require(
                    row["request"]["canonical_sql"] == expected["canonical_sql"]
                        && row["request"]["execution_mode"] == job["execution_mode"],
                    "observation query changed",
                )?;
                let gate = field(&inventory[text(&row["job_id"])?], text(&row["reader_id"])?)?;
                evidence::validate(
                    &row["observation"],
                    &row["request"],
                    text(&row["reader_id"])?,
                    (row["stage"] != "gate").then_some(gate),
                    &protocols,
                )?;
            }
            let mut rows = Vec::new();
            for job in array(&config["jobs"])? {
                let case = array(&frozen["cases"])?
                    .iter()
                    .find(|case| case["case_id"] == job["case_id"])
                    .ok_or("unknown workload case")?;
                for reader in readers {
                    let gate = field(&inventory[text(&job["id"])?], reader)?;
                    let mut oracle = Value::Null;
                    if let Some(reference) =
                        gate["reference"].as_str().filter(|value| !value.is_empty())
                    {
                        let path = Path::new(reference).join("reference.json");
                        require(
                            digest(&path)? == gate["reference_sha256"],
                            "reference metadata changed",
                        )?;
                        let metadata = read_json(&path)?;
                        require(
                            comparison(&metadata, &protocols)? == identity,
                            "reference workload changed",
                        )?;
                        oracle = select(
                            &metadata,
                            &[
                                "source_rows",
                                "physical_qualifying_rows",
                                "qualifying_rows",
                                "deleted_qualifying_rows",
                                "output_rows",
                                "projected_logical_bytes",
                                "candidate_files",
                                "matching_files",
                                "wide_file_geometry",
                                "within_file_geometry",
                                "physical_rows",
                                "derived_value_checks",
                            ]
                            .into_iter()
                            .filter(|key| metadata.get(key).is_some())
                            .collect::<Vec<_>>(),
                        )?;
                    }
                    rows.push(json!({"job":job, "case":case, "reader_id":reader, "gate":gate,
                        "measurements":field(&summary["jobs"][text(&job["id"])?],reader)?, "oracle":oracle,
                        "native_phases":{"planning_ns":null,"scan_ns":null,
                            "unavailable_reason":"adapters do not expose comparable separate phase clocks"}}));
                }
            }
            let mut result = identity.clone();
            object(&identity)?;
            result.as_object_mut().unwrap().extend(
                object(
                    &json!({"status":summary["status"],"campaign_id":config["campaign_id"],
                "scope":frozen["scope"], "publication_ready":false, "rows":rows,
                "observations_sha256":observations_sha256, "summary_sha256":summary_sha256}),
                )?
                .clone(),
            );
            if matches!(frozen["comparison_revision"].as_u64(), Some(5 | 6)) {
                result["core_inventory"] = field(&frozen, "inventory")?.clone();
            }
            fs::create_dir(&output)?;
            save(&output.join("large-workload-report.json"), &result)?;
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests;
