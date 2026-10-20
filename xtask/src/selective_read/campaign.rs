use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::{array, field, object, require, text};
use crate::{Result, scan_scheduling::balanced_orders};

pub(super) mod controller;
pub(super) mod evidence;
mod worker;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Request {
    Schedule {
        inventory: Vec<(String, Value)>,
        campaign_id: String,
        comparison: Value,
        combined: Option<bool>,
        gate_warmup: Option<bool>,
        gate_diagnostics: Option<bool>,
    },
    Summarize {
        inventory: Value,
        slots: Vec<Value>,
        rows: Vec<Value>,
        resolution: f64,
        integrity: bool,
    },
    Mode {
        config: Value,
        mode: evidence::Mode,
        protocols: PathBuf,
    },
    PlanEvidence {
        record: Value,
        artifacts: PathBuf,
    },
    Validate {
        record: Value,
        payload: Value,
        reader: String,
        gate: Option<Value>,
        protocols: PathBuf,
    },
    TimerResolution,
}

pub(super) fn run(request: Request) -> Result<Value> {
    match request {
        Request::Schedule {
            inventory,
            campaign_id,
            comparison,
            combined,
            gate_warmup,
            gate_diagnostics,
        } => schedule(
            &inventory,
            &campaign_id,
            &comparison,
            combined,
            gate_warmup,
            gate_diagnostics,
        ),
        Request::Summarize {
            inventory,
            slots,
            rows,
            resolution,
            integrity,
        } => summarize(&inventory, &slots, &rows, resolution, integrity),
        Request::Mode {
            config,
            mode,
            protocols,
        } => Ok(json!(mode.enabled(&config, &protocols)?)),
        Request::PlanEvidence { record, artifacts } => evidence::plans(&record, &artifacts),
        Request::Validate {
            record,
            payload,
            reader,
            gate,
            protocols,
        } => {
            evidence::validate(&record, &payload, &reader, gate.as_ref(), &protocols)?;
            Ok(Value::Null)
        }
        Request::TimerResolution => evidence::timer_resolution(),
    }
}

fn mode(
    inventory: &[(String, Value)],
    key: &str,
    override_value: Option<bool>,
    label: &str,
) -> Result<bool> {
    let mut values = BTreeSet::new();
    for (_, entries) in inventory {
        for entry in object(entries)?.values() {
            let value = entry
                .get(key)
                .unwrap_or(&Value::Bool(false))
                .as_bool()
                .ok_or_else(|| format!("reader inventory has inconsistent {label} modes"))?;
            values.insert(value);
        }
    }
    require(
        values.len() <= 1,
        &format!("reader inventory has inconsistent {label} modes"),
    )?;
    Ok(override_value.unwrap_or_else(|| values.first().copied().unwrap_or(false)))
}

pub(super) fn roster(comparison: &Value) -> Result<[&'static str; 5]> {
    let revision = comparison
        .get("comparison_revision")
        .unwrap_or(&Value::from(2))
        .as_u64()
        .ok_or("unknown comparison reader roster")?;
    require(
        (2..=6).contains(&revision),
        "unknown comparison reader roster",
    )?;
    Ok([
        "delta-arrow-reader",
        "delta-rs",
        "duckdb",
        "polars",
        if revision == 6 { "spark" } else { "daft" },
    ])
}

pub(super) fn schedule(
    inventory: &[(String, Value)],
    campaign: &str,
    comparison: &Value,
    combined: Option<bool>,
    gate_warmup: Option<bool>,
    gate_diagnostics: Option<bool>,
) -> Result<Value> {
    let combined = mode(inventory, "combined_diagnostics", combined, "diagnostic")?;
    let gate_warmup = mode(inventory, "gate_warmup", gate_warmup, "warmup")?;
    let gate_diagnostics = mode(
        inventory,
        "gate_diagnostics",
        gate_diagnostics,
        "gate-diagnostic",
    )?;
    let revision6 = comparison["comparison_revision"] == 6;
    require(
        !gate_diagnostics || combined && gate_warmup && revision6,
        "gate diagnostics require revision 6, combined diagnostics and gate warmup",
    )?;
    require(
        !gate_warmup || revision6,
        "gate warmup requires comparison revision 6",
    )?;
    let jobs: BTreeSet<_> = inventory.iter().map(|(job, _)| job.as_str()).collect();
    require(jobs.len() == inventory.len(), "duplicate campaign job")?;
    if gate_warmup {
        let cases: BTreeSet<_> = jobs
            .iter()
            .map(|job| job.strip_prefix("reuse.").unwrap_or(job))
            .collect();
        require(
            cases.len() == 1
                && jobs.len() == 2
                && jobs.contains(*cases.first().unwrap())
                && jobs.contains(format!("reuse.{}", cases.first().unwrap()).as_str()),
            "gate warmup requires one snapshot with both open and reuse profiles",
        )?;
    }
    require(
        !combined || revision6,
        "combined diagnostics require comparison revision 6",
    )?;
    let roster = roster(comparison)?;
    let mut readers = Vec::new();
    for (job, entries) in inventory {
        let entries = object(entries)?;
        require(
            entries.len() == roster.len()
                && roster.iter().all(|reader| entries.contains_key(*reader)),
            "reader inventory differs from the comparison roster",
        )?;
        let mut runnable = Vec::new();
        for reader in roster {
            if field(&entries[reader], "runnable")?
                .as_bool()
                .ok_or("invalid runnable flag")?
            {
                runnable.push(reader);
            }
        }
        readers.push((job, runnable));
    }
    let reduced = comparison["comparison_revision"].as_u64().unwrap_or(2) >= 4;
    let samples = if comparison["sampling_stage"] == "pilot" {
        2
    } else {
        5
    };
    let mut slots = Vec::new();
    let mut add = |job: &str,
                   stage: &str,
                   readers: &[&str],
                   repetition: usize,
                   traced: bool,
                   fixed_position: Option<usize>|
     -> Result<()> {
        for (position, reader) in readers.iter().enumerate() {
            let mut slot = json!({"run_id": format!("{campaign}-{:05}", slots.len()), "job_id": job,
                "reader_id": reader, "stage": stage, "repetition": repetition,
                "order": fixed_position.unwrap_or(position), "traced": traced});
            slot.as_object_mut()
                .unwrap()
                .extend(object(comparison)?.clone());
            slots.push(slot);
        }
        Ok(())
    };
    for (job, readers) in &readers {
        if readers.is_empty() {
            continue;
        }
        if !gate_warmup {
            add(job, "warmup", readers, 0, false, None)?;
            if !reduced {
                add(
                    job,
                    "warmup",
                    &readers.iter().rev().copied().collect::<Vec<_>>(),
                    1,
                    false,
                    None,
                )?;
            }
        }
        let orders = balanced_orders(readers.len());
        let count = if reduced {
            samples
        } else {
            10usize.div_ceil(orders.len()) * orders.len()
        };
        for repetition in 0..count {
            let order: Vec<_> = orders[repetition % orders.len()]
                .iter()
                .map(|index| readers[*index])
                .collect();
            add(job, "timing", &order, repetition, false, None)?;
        }
    }
    for (job, readers) in &readers {
        let plans: Vec<_> = readers
            .iter()
            .copied()
            .filter(|reader| !combined || *reader == "duckdb")
            .collect();
        add(job, "plan", &plans, 0, false, None)?;
        if gate_diagnostics {
            continue;
        }
        if reduced {
            add(job, "diagnostic", readers, 0, true, None)?;
            continue;
        }
        let reversed: Vec<_> = readers.iter().rev().copied().collect();
        for (repetition, order) in [readers, &reversed].into_iter().enumerate() {
            add(job, "diagnostic_warmup", order, repetition, true, None)?;
        }
        for (repetition, order) in [readers, &reversed].into_iter().enumerate() {
            for (position, reader) in order.iter().enumerate() {
                for traced in if repetition == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    add(
                        job,
                        if traced {
                            "diagnostic"
                        } else {
                            "observer_baseline"
                        },
                        &[*reader],
                        repetition,
                        traced,
                        Some(position),
                    )?;
                }
            }
        }
    }
    Ok(json!(slots))
}

fn warmed_gate(gate: &Value, observations: &BTreeMap<String, Value>) -> bool {
    let row = gate["run_id"]
        .as_str()
        .and_then(|id| observations.get(id))
        .unwrap_or(&Value::Null);
    let record = &row["observation"];
    row["stage"] == "gate"
        && row["status"] == "success"
        && gate["status"] == "success"
        && gate["identity"].is_object()
        && record["status"] == "success"
        && record["identity"] == gate["identity"]
        && record["correctness"]["status"] == "passed"
        && record["cleanup"]["status"] == "passed"
        && (gate["gate_diagnostics"] != true
            || (row["traced"] == true
                && row["request"]["validation_diagnostics"] == true
                && record["validation_diagnostics"] == true
                && record["storage_capture"]["status"] == "passed"
                && record["storage_environment"]["trace_enabled"] == true
                && record["storage_io"].is_object()
                && (row["reader_id"] == "duckdb"
                    || record["plan_artifacts"]
                        .as_array()
                        .is_some_and(|plans| !plans.is_empty()))))
}

fn distribution(mut values: Vec<f64>) -> Result<Value> {
    require(
        !values.is_empty() && values.iter().all(|v| v.is_finite()),
        "empty or invalid distribution",
    )?;
    values.sort_by(f64::total_cmp);
    let quantile = |p: f64| {
        let index = (values.len() - 1) as f64 * p;
        let low = index.floor() as usize;
        let high = index.ceil() as usize;
        values[low] + (values[high] - values[low]) * (index - low as f64)
    };
    let (q1, median, q3) = (quantile(0.25), quantile(0.5), quantile(0.75));
    Ok(json!({"samples": values.len(), "median": median, "q1": q1, "q3": q3, "iqr": q3 - q1}))
}

fn measurements(record: &Value) -> Result<Map<String, Value>> {
    let mut values = Map::new();
    for key in [
        "open_query_ns",
        "initialization_ns",
        "initialization_plus_query1_ns",
        "initialization_plus_all_queries_ns",
        "session_elapsed_ns",
        "cleanup_ns",
    ] {
        let value = field(record, key)?;
        if !value.is_null() {
            values.insert(key.into(), value.clone());
        }
    }
    if let Some(value) = record.get("startup_ns").filter(|value| !value.is_null()) {
        values.insert("startup_ns".into(), value.clone());
    }
    for key in [
        "process_user_cpu_ns",
        "process_system_cpu_ns",
        "process_cpu_ns",
        "peak_rss_bytes",
        "process_elapsed_ns",
    ] {
        values.insert(key.into(), field(&record["external_metrics"], key)?.clone());
    }
    for query in array(&record["queries"])? {
        let index = query["query_index"].as_u64().ok_or("invalid query index")?;
        for key in ["completion_ns", "first_batch_ns"] {
            let value = field(query, key)?;
            if !value.is_null() {
                values.insert(format!("query_{index}.{key}"), value.clone());
            }
        }
    }
    Ok(values)
}

fn summarize(
    inventory: &Value,
    slots: &[Value],
    rows: &[Value],
    resolution: f64,
    integrity: bool,
) -> Result<Value> {
    require(
        resolution.is_finite() && resolution > 0.0,
        "invalid timer resolution",
    )?;
    let by_id: BTreeMap<_, _> = rows
        .iter()
        .map(|row| Ok((text(&row["run_id"])?.to_owned(), row.clone())))
        .collect::<Result<_>>()?;
    let mut result = Map::new();
    for (job, entries) in object(inventory)? {
        let mut readers = Map::new();
        for (reader, gate) in object(entries)? {
            let selected: Vec<_> = slots
                .iter()
                .filter(|s| s["job_id"] == *job && s["reader_id"] == *reader)
                .collect();
            let timed: Vec<_> = selected
                .iter()
                .copied()
                .filter(|s| s["stage"] == "timing")
                .collect();
            let observed = |slot: &Value| {
                by_id
                    .get(slot["run_id"].as_str().unwrap_or(""))
                    .unwrap_or(&Value::Null)
            };
            let mut statuses = BTreeMap::<String, usize>::new();
            for slot in &timed {
                *statuses
                    .entry(
                        observed(slot)["status"]
                            .as_str()
                            .unwrap_or("not_run")
                            .to_owned(),
                    )
                    .or_default() += 1;
            }
            let gate_warmup = gate["gate_warmup"] == true;
            let gate_diagnostics = gate["gate_diagnostics"] == true;
            let eligible = integrity
                && gate["runnable"] == true
                && !timed.is_empty()
                && selected
                    .iter()
                    .filter(|s| matches!(s["stage"].as_str(), Some("warmup" | "timing")))
                    .all(|s| observed(s)["status"] == "success")
                && (!gate_warmup || warmed_gate(gate, &by_id));
            let values = if eligible {
                timed
                    .iter()
                    .map(|s| measurements(&observed(s)["observation"]))
                    .collect::<Result<Vec<_>>>()?
            } else {
                Vec::new()
            };
            let mut metrics = Map::new();
            let mut durations = Map::new();
            if let Some(first) = values.first() {
                for key in first
                    .keys()
                    .filter(|key| values.iter().all(|v| v.contains_key(*key)))
                {
                    let samples = values
                        .iter()
                        .map(|v| v[key].as_f64().ok_or_else(|| "invalid measurement".into()))
                        .collect::<Result<Vec<_>>>()?;
                    metrics.insert(key.clone(), distribution(samples.clone())?);
                    if key.ends_with("_ns") {
                        durations.insert(key.clone(), json!(samples));
                    }
                }
            }
            let mut unavailable = Vec::new();
            for slot in &timed {
                let row = observed(slot);
                if row["status"] == "success" {
                    for query in array(&row["observation"]["queries"])? {
                        if field(query, "first_batch_ns")?.is_null() {
                            unavailable
                                .push(field(query, "first_batch_unavailable_reason")?.clone());
                        }
                    }
                }
            }
            let mut summary = json!({"gate_status": field(gate, "status")?, "eligible": eligible,
                "scheduled_samples": timed.len(), "sample_statuses": statuses, "metrics": metrics,
                "speedup_vs_dar": {}, "first_batch_unavailable": unavailable, "_durations": durations});
            if gate_warmup {
                summary["warmup"] = json!({"source": "exact-validation-gate", "run_id": gate["run_id"], "passed": warmed_gate(gate, &by_id)});
            }
            let mut diagnostics = Map::new();
            let mut overhead = BTreeMap::new();
            let mut successful = true;
            for stage in ["diagnostic", "observer_baseline"] {
                let observations: Vec<_> = if gate_diagnostics && stage == "diagnostic" {
                    vec![
                        by_id
                            .get(gate["run_id"].as_str().unwrap_or(""))
                            .cloned()
                            .unwrap_or_else(
                                || json!({"run_id": gate["run_id"], "status": "not_run"}),
                            ),
                    ]
                } else {
                    selected
                        .iter()
                        .filter(|s| s["stage"] == stage)
                        .map(|s| {
                            by_id
                                .get(s["run_id"].as_str().unwrap_or(""))
                                .cloned()
                                .unwrap_or_else(
                                    || json!({"run_id": s["run_id"], "status": "not_run"}),
                                )
                        })
                        .collect()
                };
                let mut records = Vec::new();
                let mut clocks = Vec::new();
                for row in observations {
                    successful &= row["status"] == "success";
                    clocks.push(row["observation"]["diagnostic_session_ns"].as_u64());
                    let mut record = json!({"run_id": row["run_id"], "status": row["status"],
                        "session_ns": row["observation"]["diagnostic_session_ns"], "io": row["observation"]["storage_io"]});
                    if gate_diagnostics && stage == "diagnostic" {
                        record["source"] = json!("exact-validation-gate");
                    }
                    records.push(record);
                }
                diagnostics.insert(stage.into(), json!(records));
                overhead.insert(stage, clocks);
            }
            summary["diagnostics"] = json!(diagnostics);
            if successful
                && overhead
                    .values()
                    .all(|v| v.len() == 2 && v.iter().all(Option::is_some))
            {
                let off: Vec<_> = overhead["observer_baseline"]
                    .iter()
                    .map(|v| v.unwrap() as f64)
                    .collect();
                let on: Vec<_> = overhead["diagnostic"]
                    .iter()
                    .map(|v| v.unwrap() as f64)
                    .collect();
                let off_median = distribution(off.clone())?["median"].as_f64().unwrap();
                let on_median = distribution(on.clone())?["median"].as_f64().unwrap();
                summary["observer_overhead"] = json!({"off_median_ns": off_median, "on_median_ns": on_median,
                    "difference_ns": on_median - off_median,
                    "ratio": if off.iter().chain(&on).all(|v| *v >= resolution) { Some(on_median / off_median) } else { None },
                    "note": "two descriptive pairs; separate observations from headline timing"});
            }
            readers.insert(reader.clone(), summary);
        }
        let dar = readers
            .get("delta-arrow-reader")
            .ok_or("missing DAR inventory")?
            .clone();
        for (name, reader) in &mut readers {
            if name != "delta-arrow-reader" {
                for key in [
                    "open_query_ns",
                    "initialization_plus_query1_ns",
                    "initialization_plus_all_queries_ns",
                ] {
                    let mut ratio = None;
                    let mut reason = Some(
                        "both readers must pass validation and every scheduled warmup/timing slot",
                    );
                    if dar["eligible"] == true
                        && reader["eligible"] == true
                        && !dar["metrics"][key].is_null()
                        && !reader["metrics"][key].is_null()
                    {
                        if array(&dar["_durations"][key])?
                            .iter()
                            .chain(array(&reader["_durations"][key])?)
                            .all(|v| v.as_f64().is_some_and(|v| v >= resolution))
                        {
                            ratio = Some(
                                reader["metrics"][key]["median"]
                                    .as_f64()
                                    .ok_or("invalid comparator median")?
                                    / dar["metrics"][key]["median"]
                                        .as_f64()
                                        .ok_or("invalid DAR median")?,
                            );
                            reason = None;
                        } else {
                            reason = Some("duration below timer resolution");
                        }
                    }
                    reader["speedup_vs_dar"][key] =
                        json!({"value": ratio, "unavailable_reason": reason});
                }
            }
            reader.as_object_mut().unwrap().remove("_durations");
        }
        result.insert(job.clone(), json!(readers));
    }
    Ok(json!(result))
}

#[cfg(test)]
mod tests;
