use std::{collections::BTreeMap, fs, path::Path};

use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::{array, field, object, require, schedule, summarize, text, warmed_gate};
use crate::{
    Result,
    selective_read::supervise::{monotonic_ns, progress},
    selective_read_build::digest,
};

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::selective_read) enum Mode {
    CombinedDiagnostics,
    GateWarmup,
    GateDiagnostics,
}

impl Mode {
    pub(in crate::selective_read) fn enabled(
        &self,
        config: &Value,
        _protocols: &Path,
    ) -> Result<bool> {
        let key = match self {
            Self::CombinedDiagnostics => "combined_diagnostics",
            Self::GateWarmup => "gate_warmup",
            Self::GateDiagnostics => "gate_diagnostics",
        };
        let enabled = config
            .get(key)
            .unwrap_or(&Value::Bool(false))
            .as_bool()
            .ok_or_else(|| format!("{key} must be a boolean"))?;
        if enabled {
            require(
                config["comparison_revision"] == 6
                    && (!matches!(self, Self::GateDiagnostics)
                        || config["combined_diagnostics"] == true && config["gate_warmup"] == true),
                &format!("incompatible {key} mode"),
            )?;
        }
        Ok(enabled)
    }
}

pub(in crate::selective_read) fn comparison(value: &Value, _protocols: &Path) -> Result<Value> {
    super::super::contract::comparison(value)
}

pub(in crate::selective_read) use super::super::contract::same_comparison;

pub(super) fn plans(record: &Value, artifacts: &Path) -> Result<Value> {
    let mut evidence = Vec::new();
    for query in array(&record["queries"])? {
        let name = text(&query["physical_plan"])?;
        require(
            !name.is_empty() && !matches!(name, "." | "..") && !name.contains('/'),
            "missing or invalid native plan",
        )?;
        let path = artifacts.join("reader").join(name);
        let metadata = fs::symlink_metadata(&path)?;
        require(
            metadata.is_file() && metadata.len() > 0,
            "native plan export is empty or missing",
        )?;
        evidence.push(json!({"query_index": field(query, "query_index")?, "path": path, "sha256": digest(&path)?}));
    }
    Ok(json!(evidence))
}

fn purpose(slot: &Value, combined: bool) -> &'static str {
    match slot["stage"].as_str() {
        Some("gate") => "validation",
        Some("warmup" | "timing") => "timing",
        Some("plan") => "diagnostic",
        Some("diagnostic") if combined && slot["reader_id"] != "duckdb" => "diagnostic",
        _ => "io",
    }
}

pub(in crate::selective_read) fn diagnostics(
    config: &Value,
    inventory: &[(String, Value)],
    slots: &[Value],
    rows: &[Value],
    protocols: &Path,
) -> Result<()> {
    let combined = Mode::CombinedDiagnostics.enabled(config, protocols)?;
    let warmup = Mode::GateWarmup.enabled(config, protocols)?;
    let gate_diagnostics = Mode::GateDiagnostics.enabled(config, protocols)?;
    require(
        schedule(
            inventory,
            text(&config["campaign_id"])?,
            &comparison(config, protocols)?,
            Some(combined),
            Some(warmup),
            Some(gate_diagnostics),
        )? == json!(slots),
        "schedule differs from the declared diagnostic mode",
    )?;
    let by_id: BTreeMap<_, _> = rows
        .iter()
        .map(|row| Ok((text(&row["run_id"])?.to_owned(), row.clone())))
        .collect::<Result<_>>()?;
    for (_, entries) in inventory {
        for entry in object(entries)?.values() {
            for (enabled, key) in [
                (combined, "combined_diagnostics"),
                (warmup, "gate_warmup"),
                (gate_diagnostics, "gate_diagnostics"),
            ] {
                require(
                    !enabled || entry[key] == true,
                    &format!("{key} was not declared in the frozen inventory"),
                )?;
            }
            if warmup && entry["runnable"] == true {
                require(
                    warmed_gate(entry, &by_id),
                    "storage-warmup gate is missing or incomplete",
                )?;
            }
        }
    }
    if !combined {
        return Ok(());
    }
    let by_slot: BTreeMap<_, _> = slots
        .iter()
        .map(|slot| Ok((text(&slot["run_id"])?, slot)))
        .collect::<Result<_>>()?;
    let observed: std::collections::BTreeSet<_> = rows
        .iter()
        .filter(|r| r["stage"] != "gate")
        .map(|row| text(&row["run_id"]))
        .collect::<Result<_>>()?;
    require(
        observed == by_slot.keys().copied().collect(),
        "combined diagnostic schedule has missing or extra observations",
    )?;
    for row in rows {
        if row["stage"] != "gate" {
            let slot = by_slot[text(&row["run_id"])?];
            require(
                object(slot)?.iter().all(|(k, v)| row.get(k) == Some(v)),
                "observation differs from the declared diagnostic schedule",
            )?;
        }
        require(
            row["request"]["purpose"] == purpose(row, combined),
            "diagnostic purpose changed",
        )?;
        if gate_diagnostics {
            let is_gate = row["stage"] == "gate";
            require(
                row["request"]
                    .get("validation_diagnostics")
                    .unwrap_or(&Value::Bool(false))
                    == &json!(is_gate)
                    && (!is_gate || row["traced"] == true),
                "gate diagnostic request/tracing changed",
            )?;
        }
        let record = &row["observation"];
        if record["status"] != "success" {
            continue;
        }
        if row["request"]["purpose"] == "diagnostic"
            || gate_diagnostics && row["stage"] == "gate" && row["reader_id"] != "duckdb"
        {
            require(
                record["plan_artifacts"] == plans(record, Path::new(text(&row["artifacts"])?))?,
                "native plan artifacts changed",
            )?;
        }
        if row["stage"] == "diagnostic" || gate_diagnostics && row["stage"] == "gate" {
            require(
                record["storage_capture"]["status"] == "passed"
                    && record["storage_environment"]["trace_enabled"] == true
                    && !record["storage_io"].is_null(),
                "combined diagnostic lacks reconciled I/O",
            )?;
        }
    }
    Ok(())
}

fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| "expected a nonnegative integer".into())
}

pub(in crate::selective_read) fn validate(
    record: &Value,
    payload: &Value,
    reader: &str,
    gate: Option<&Value>,
    protocols: &Path,
) -> Result<()> {
    if field(record, "status")? != "success" {
        return Ok(());
    }
    let identity = field(record, "identity")?;
    let comparison = comparison(payload, protocols)?;
    require(identity["reader_id"] == reader, "reader identity changed")?;
    require(
        same_comparison(identity, &comparison),
        "observation comparison differs",
    )?;
    for key in [
        "fixture_manifest_sha256",
        "case_id",
        "snapshot_version",
        "canonical_sql",
    ] {
        require(
            field(identity, key)? == field(payload, key)?,
            &format!("observation identity differs: {key}"),
        )?;
    }
    require(
        identity["reader_config"].is_object() && identity["reader_config"] == record["settings"],
        "reader configuration differs",
    )?;
    for key in [
        "campaign_id",
        "run_id",
        "purpose",
        "execution_mode",
        "repetition",
        "order",
        "table_uri",
        "profile",
        "canonical_sql",
    ] {
        require(
            field(record, key)? == field(payload, key)?,
            &format!("observation request differs: {key}"),
        )?;
    }
    require(
        record["capability"]["status"] == "supported" && record["cleanup"]["status"] == "passed",
        "incomplete capability or cleanup",
    )?;
    require(
        record["external_resource_limits"]["process_memory_bytes"]
            == crate::selective_read::budget()["process_memory_bytes"],
        "missing enforced memory limit",
    )?;
    if integer(&payload["comparison_revision"])? >= 3 && payload["purpose"] == "validation" {
        require(
            field(&record["external_resource_limits"], "max_file_size_bytes")?
                == field(record, "validation_export_limit_bytes_per_file")?,
            "validation export limit was not inherited by the reader",
        )?;
    }
    let queries = array(&record["queries"])?;
    let count = progress::count(payload)?;
    require(queries.len() == count, "incomplete query count")?;
    for (index, query) in queries.iter().enumerate() {
        require(
            integer(&query["query_index"])? == index as u64
                && query["output_rows"].as_u64().is_some()
                && query["output_batches"].as_u64().is_some(),
            "invalid query counts",
        )?;
    }
    if let Some(gate) = gate {
        require(
            super::same_reader_identity(identity, field(gate, "identity")?),
            "validated build/configuration changed",
        )?;
    }
    if matches!(payload["purpose"].as_str(), Some("validation" | "timing")) {
        require(
            record["correctness"]["status"] == "passed",
            "missing correctness gate",
        )?;
    }
    if !payload["validation_diagnostics"].is_null() && payload["validation_diagnostics"] != false {
        require(
            payload["validation_diagnostics"] == true
                && payload["purpose"] == "validation"
                && payload["comparison_revision"] == 6
                && record["validation_diagnostics"] == true,
            "undeclared native gate diagnostics",
        )?;
        require(
            record["storage_capture"]["status"] == "passed"
                && record["storage_environment"]["trace_enabled"] == true
                && record["storage_io"].is_object()
                && record["diagnostic_session_ns"].as_u64().is_some(),
            "missing reconciled gate I/O",
        )?;
        require(
            array(&record["diagnostic_events"])?
                .iter()
                .filter(|e| e["event"] == "stream_complete")
                .count()
                == count,
            "missing gate stream boundaries",
        )?;
        require(
            queries.iter().all(|q| {
                q.get("completion_ns") == Some(&Value::Null)
                    && q.get("first_batch_ns") == Some(&Value::Null)
            }) && field(record, "open_query_ns")?.is_null()
                && field(record, "session_elapsed_ns")?.is_null(),
            "diagnostic gate contains timing samples",
        )?;
        if reader == "duckdb" {
            require(
                queries
                    .iter()
                    .all(|q| q.get("physical_plan") == Some(&Value::Null)),
                "DuckDB EXPLAIN entered gate I/O",
            )?;
        } else {
            require(
                array(&record["plan_artifacts"])?.len() == count,
                "missing gate native plans",
            )?;
        }
    }
    if payload["purpose"] != "timing" {
        return Ok(());
    }
    for query in queries {
        let completion = integer(&query["completion_ns"])?;
        let first = field(query, "first_batch_ns")?;
        require(
            if first.is_null() {
                query["first_batch_unavailable_reason"]
                    .as_str()
                    .is_some_and(|reason| !reason.is_empty())
            } else {
                integer(first)? <= completion
            },
            "invalid first-batch time",
        )?;
    }
    integer(&record["cleanup_ns"])?;
    let elapsed = integer(&record["session_elapsed_ns"])?;
    if count == 1 {
        require(
            record["open_query_ns"] == queries[0]["completion_ns"],
            "open/query clocks disagree",
        )?;
    } else {
        let init = integer(&record["initialization_ns"])? as u128;
        let mut total = init;
        for query in queries {
            total += integer(&query["completion_ns"])? as u128;
        }
        require(
            integer(&record["initialization_plus_query1_ns"])? as u128
                == init + integer(&queries[0]["completion_ns"])? as u128
                && integer(&record["initialization_plus_all_queries_ns"])? as u128 == total,
            "session totals disagree",
        )?;
        require(
            elapsed as u128 >= total,
            "enclosing session is shorter than its intervals",
        )?;
    }
    for key in [
        "process_user_cpu_ns",
        "process_system_cpu_ns",
        "process_cpu_ns",
        "peak_rss_bytes",
        "process_elapsed_ns",
    ] {
        require(
            record["external_metrics"][key].as_u64().is_some(),
            &format!("missing process resource: {key}"),
        )?;
    }
    Ok(())
}

pub(in crate::selective_read) fn read_results(
    directory: &Path,
    frozen_error: &str,
) -> Result<Value> {
    let read = |name: &str| -> Result<Value> {
        Ok(serde_json::from_reader(fs::File::open(
            directory.join(name),
        )?)?)
    };
    let mut frozen = Map::new();
    for name in ["campaign.json", "inventory.json", "schedule.json"] {
        frozen.insert(name.into(), json!(digest(&directory.join(name))?));
    }
    require(read("frozen.json")? == Value::Object(frozen), frozen_error)?;
    let (inventory, summary, slots) = (
        read("inventory.json")?,
        read("summary.json")?,
        read("schedule.json")?,
    );
    let observations: Vec<Value> = fs::read_to_string(directory.join("observations.jsonl"))?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()?;
    require(
        summary["jobs"]
            == summarize(
                &inventory,
                array(&slots)?,
                &observations,
                summary["timer_resolution"]["ratio_floor_ns"]
                    .as_f64()
                    .ok_or("invalid timer resolution")?,
                summary["integrity_passed"]
                    .as_bool()
                    .ok_or("invalid integrity flag")?,
            )?,
        "summary differs from raw observations",
    )?;
    Ok(json!([inventory, summary, observations, slots]))
}

pub(super) fn timer_resolution() -> Result<Value> {
    let mut resolution = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_getres initializes this timespec on success.
    if unsafe { libc::clock_getres(libc::CLOCK_MONOTONIC, resolution.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: clock_getres succeeded.
    let resolution = unsafe { resolution.assume_init() };
    let nominal = resolution.tv_sec as u64 * 1_000_000_000 + resolution.tv_nsec as u64;
    let mut previous = monotonic_ns()?;
    let mut minimum = u64::MAX;
    for _ in 0..10_000 {
        let current = monotonic_ns()?;
        if current > previous {
            minimum = minimum.min(current - previous);
        }
        previous = current;
    }
    require(minimum != u64::MAX, "monotonic clock did not advance")?;
    Ok(
        json!({"clock": "Linux CLOCK_MONOTONIC (Rust Instant/Python perf_counter)", "nominal_ns": nominal,
        "minimum_observed_tick_ns": minimum, "ratio_floor_ns": nominal.max(minimum)}),
    )
}

#[cfg(test)]
mod diagnostics_tests;
#[cfg(test)]
mod tests;
