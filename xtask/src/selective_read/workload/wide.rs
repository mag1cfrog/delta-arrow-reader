use super::*;
use crate::selective_read::experiment::median;
use std::collections::BTreeMap;

fn date(value: &Value) -> Result<&str> {
    let value = text(value)?;
    let bytes = value.as_bytes();
    require(
        bytes.len() == 10
            && bytes[4] == b'-'
            && bytes[7] == b'-'
            && bytes
                .iter()
                .enumerate()
                .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit()),
        "invalid fixture date bound",
    )?;
    let year: u32 = value[..4].parse()?;
    let month: usize = value[5..7].parse()?;
    let day: u32 = value[8..].parse()?;
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = [
        31,
        28 + u32::from(leap),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    require(
        year > 0 && (1..=12).contains(&month) && day > 0 && day <= days[month - 1],
        "invalid fixture date bound",
    )?;
    Ok(value)
}

fn candidate(stats: &Value, literals: &[Value]) -> Result<bool> {
    for (column, target) in [("l_shipdate", "1995-03-15"), ("l_shipmode", "AIR")] {
        if count(&stats["nullCount"][column])? == count(&stats["numRecords"])? {
            return Ok(false);
        }
        let bounds = [&stats["minValues"][column], &stats["maxValues"][column]];
        let [low, high] = if column == "l_shipdate" {
            [date(bounds[0])?, date(bounds[1])?]
        } else {
            [text(bounds[0])?, text(bounds[1])?]
        };
        if !(low <= target && target <= high) {
            return Ok(false);
        }
    }
    if count(&stats["nullCount"]["l_partkey"])? == count(&stats["numRecords"])? {
        return Ok(false);
    }
    let integer = |v: &Value| v.as_i64().ok_or("invalid part-key bound or literal");
    let low = integer(&stats["minValues"]["l_partkey"])?;
    let high = integer(&stats["maxValues"]["l_partkey"])?;
    Ok(literals
        .iter()
        .map(integer)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .any(|value| low <= value && value <= high))
}

pub(super) fn geometry(manifest: &Value, large: bool, smoke: bool) -> Result<Value> {
    require(
        manifest["wide_file_pair"]["kind"] == if large { "large" } else { "legacy" },
        "wrong wide-file deletion contract",
    )?;
    let all = array(&manifest["tables"])?;
    let tables: BTreeMap<_, _> = all
        .iter()
        .map(|t| Ok((text(&t["id"])?, t)))
        .collect::<Result<_>>()?;
    let mut expected = BTreeSet::from(["wide.clustered", "wide.files4096", "wide.files4096.dv"]);
    if large {
        expected.insert("wide.clustered.dv");
    }
    require(
        all.len() == tables.len() && tables.keys().copied().collect::<BTreeSet<_>>() == expected,
        "wrong wide-file inventory",
    )?;
    let sources = array(&manifest["sources"])?;
    require(sources.len() == 1, "one shared source required")?;
    let source = &sources[0];
    let scale = source["scale_factor"]
        .as_f64()
        .ok_or("invalid source scale")?;
    require(
        if smoke {
            scale == 0.01
        } else if large {
            [10.0, 30.0, 100.0, 300.0].contains(&scale)
        } else {
            scale == 1.0
        },
        "wrong wide-file source scale",
    )?;
    let normal = tables["wide.clustered"];
    let repacked = tables["wide.files4096"];
    let names = array(&normal["schema"]["fields"])?
        .iter()
        .map(|f| text(&f["name"]))
        .collect::<Result<Vec<_>>>()?;
    let columns: Vec<_> = ORIGINAL
        .into_iter()
        .map(str::to_owned)
        .chain(payloads())
        .collect();
    require(names == columns, "wide pair must preserve all 80 columns")?;
    for (id, table) in &tables {
        require(
            table["scale_factor"].as_f64() == Some(scale)
                && table["rows"] == source["rows"]
                && table["schema"] == normal["schema"],
            "wide organizations differ in scale, rows or schema",
        )?;
        let dv = id.ends_with(".dv");
        require(
            table["deletion_vectors"] == dv
                && table["snapshot_version"].as_u64() == Some(u64::from(dv)),
            "wrong wide-file snapshot",
        )?;
    }
    files::geometry(repacked, 4096)?;
    let files = array(&repacked["files"])?;
    let mut sizes = files
        .iter()
        .map(|f| count(&f["bytes"]))
        .collect::<Result<Vec<_>>>()?;
    sizes.sort_unstable();
    let middle = median(sizes.iter().map(|n| *n as f64).collect())?;
    let mut candidates = Vec::new();
    let mut excluded = BTreeSet::new();
    for file in files {
        let name = text(&file["path"])?;
        if candidate(&file["delta_stats"], array(&source["in_literals"])?)? {
            candidates.push(name);
        } else {
            excluded.insert(name);
        }
    }
    require(
        !candidates.is_empty() && candidates.len() <= 40,
        "wide pair needs at least 99% independently excludable files",
    )?;
    require(
        !large || smoke || middle >= 64.0 * 1024.0f64.powi(2),
        "large 4096-file median is below 64 MiB",
    )?;
    Ok(
        json!({"source_scale":scale,"physical_rows":source["rows"],"files":4096,
        "file_bytes_sorted":sizes,"median_file_bytes":middle,"candidate_files":candidates,"excluded_files":excluded,
        "scope":if smoke {"smoke geometry; not large-data acceptance"} else if large {"large geometry"} else {"SF1 mechanism control"}}),
    )
}

#[cfg(test)]
mod tests;
