//! Bounded Q2/Q4 Parquet writing from an ordered stream of original source rows.

use super::*;
use arrow::array::{ArrayRef, Int32Array, UInt32Array};
use arrow::compute::{concat_batches, take_record_batch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};
use std::io;

const CONTRACT: &str =
    include_str!("../../../docs/content/benchmarks/selective-read-production-shapes.md");
const DRIVER: &str = include_str!("../production_fixtures.py");
const GROUP_ROWS: usize = fixtures::GROUP_ROWS;
const WRITE_BATCH_ROWS: usize = 1024;

fn dimensions(name: &str, file_target_mib: u64) -> Result<(usize, i64, usize)> {
    let multiplier = match file_target_mib {
        512 => 1,
        256 => 2,
        _ => return Err("file target must be 256 or 512 MiB".into()),
    };
    match name {
        "q2" => Ok((130 * multiplier, 5, 336)),
        "q4" => Ok((60 * multiplier, 6, 10)),
        _ => Err("production shape must be q2 or q4".into()),
    }
}

fn schema(metrics: usize) -> SchemaRef {
    let mut fields = fixtures::wide_schema().fields().to_vec();
    fields.extend(
        (0..metrics).map(|j| Arc::new(Field::new(format!("metric_{j:03}"), DataType::Int64, true))),
    );
    Arc::new(Schema::new(fields))
}

fn expand(batch: &RecordBatch, metrics: usize) -> Result<RecordBatch> {
    let wide = fixtures::add_payloads(batch)?;
    let orders = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("order type")?;
    let parts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("part type")?;
    let suppliers = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("supplier type")?;
    let lines = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("line type")?;
    let mut columns = wide.columns().to_vec();
    columns.extend((0..metrics).map(|j| {
        Arc::new(Int64Array::from_iter((0..batch.num_rows()).map(|r| {
            let line = i64::from(lines.value(r));
            ((orders.value(r) + line + j as i64) % 17 != 0)
                .then_some((parts.value(r) + (j as i64 + 1) * suppliers.value(r) + line) % 1024)
        }))) as ArrayRef
    }));
    Ok(RecordBatch::try_new(schema(metrics), columns)?)
}

fn reorder(batch: &RecordBatch, scattered: bool) -> Result<RecordBatch> {
    if !scattered {
        return Ok(batch.clone());
    }
    let orders = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("order type")?;
    let lines = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("line type")?;
    let mut order: Vec<_> = (0..batch.num_rows())
        .map(|r| {
            (
                Sha256::digest(
                    format!(
                        "dar-production-scatter-v1/{}/{}",
                        orders.value(r),
                        lines.value(r)
                    )
                    .as_bytes(),
                ),
                orders.value(r),
                lines.value(r),
                r as u32,
            )
        })
        .collect();
    order.sort_unstable();
    Ok(take_record_batch(
        batch,
        &UInt32Array::from_iter_values(order.into_iter().map(|r| r.3)),
    )?)
}

fn matching(batch: &RecordBatch) -> Result<Vec<usize>> {
    let days = batch
        .column(10)
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or("date type")?;
    let modes = batch
        .column(14)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("mode type")?;
    let lines = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("line type")?;
    // 1995-03-15 in the pinned Date32 source representation.
    Ok((0..batch.num_rows())
        .filter(|&r| days.value(r) == 9204 && modes.value(r) == "AIR" && lines.value(r) == 1)
        .collect())
}

fn candidate(stats: &Value) -> Result<bool> {
    let date = "1995-03-15";
    Ok(stats["minValues"]["l_shipdate"]
        .as_str()
        .ok_or("date min")?
        <= date
        && date
            <= stats["maxValues"]["l_shipdate"]
                .as_str()
                .ok_or("date max")?
        && stats["minValues"]["l_shipmode"]
            .as_str()
            .ok_or("mode min")?
            <= "AIR"
        && "AIR"
            <= stats["maxValues"]["l_shipmode"]
                .as_str()
                .ok_or("mode max")?
        && stats["minValues"]["l_linenumber"]
            .as_i64()
            .ok_or("line min")?
            <= 1
        && 1 <= stats["maxValues"]["l_linenumber"]
            .as_i64()
            .ok_or("line max")?)
}

/// Consume exactly one planned file, retaining only its original 16 columns.
fn next_file<R: io::Read>(
    reader: &mut StreamReader<R>,
    pending: &mut Option<RecordBatch>,
    rows: usize,
) -> Result<RecordBatch> {
    let mut batches = Vec::new();
    let mut remaining = rows;
    while remaining > 0 {
        let batch = match pending.take() {
            Some(batch) => batch,
            None => reader
                .next()
                .ok_or("source stream ended before planned file")??,
        };
        if batch.num_columns() != 16
            || batch.num_rows() == 0
            || batch
                .schema()
                .fields()
                .iter()
                .zip(original_schema().fields())
                .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
            || batch.columns().iter().any(|c| c.null_count() != 0)
        {
            return Err("invalid original source stream schema/rows/nulls".into());
        }
        let batch = fixtures::normalize(&batch, original_schema())?;
        let count = remaining.min(batch.num_rows());
        batches.push(batch.slice(0, count));
        if count < batch.num_rows() {
            *pending = Some(batch.slice(count, batch.num_rows() - count));
        }
        remaining -= count;
    }
    Ok(concat_batches(&original_schema(), &batches)?)
}

fn physical_evidence(
    path: &Path,
    file: &Value,
    expected: &[RecordBatch],
    projection: &[String],
    page_ceiling: usize,
) -> Result<Value> {
    let mut positions = Vec::new();
    let (mut group, mut offset, mut row) = (0, 0, 0);
    for actual in fixtures::read_batches(&path.join(file["path"].as_str().ok_or("file path")?))? {
        let actual = actual?;
        let mut start = 0;
        while start < actual.num_rows() {
            let batch = expected.get(group).ok_or("Parquet roundtrip added rows")?;
            let count = (batch.num_rows() - offset).min(actual.num_rows() - start);
            if batch.slice(offset, count) != actual.slice(start, count) {
                return Err(
                    "Parquet roundtrip changed source/payload/metric values or nulls".into(),
                );
            }
            start += count;
            offset += count;
            if offset == batch.num_rows() {
                group += 1;
                offset = 0;
            }
        }
        positions.extend(matching(&actual)?.into_iter().map(|r| row + r));
        row += actual.num_rows();
    }
    if group != expected.len() || offset != 0 {
        return Err("Parquet roundtrip lost rows".into());
    }
    let geometry: Value = serde_json::from_slice(&fs::read(
        path.join(file["geometry"]["path"].as_str().ok_or("geometry path")?),
    )?)?;
    let mut groups = Vec::new();
    let mut maximum_page_rows = 0;
    for group in geometry["row_groups"].as_array().ok_or("row groups")? {
        let first = group["first_row"].as_u64().ok_or("group offset")? as usize;
        let rows = group["rows"].as_u64().ok_or("group rows")? as usize;
        let matches: Vec<_> = positions
            .iter()
            .copied()
            .filter(|&r| first <= r && r < first + rows)
            .map(|r| r - first)
            .collect();
        let mut output_pages = 0;
        let mut matching_output_pages = 0;
        for column in group["columns"].as_array().ok_or("columns")? {
            let projected = projection.iter().any(|c| column["column"] == c.as_str());
            for page in column["pages"].as_array().ok_or("pages")? {
                let start = page["first_row"].as_u64().ok_or("page offset")? as usize;
                let count = page["rows"].as_u64().ok_or("page rows")? as usize;
                maximum_page_rows = maximum_page_rows.max(count);
                if count > page_ceiling {
                    return Err("production page exceeds the declared writer limit".into());
                }
                if projected {
                    output_pages += 1;
                    matching_output_pages +=
                        usize::from(matches.iter().any(|&r| start <= r && r < start + count));
                }
            }
        }
        if !matches.is_empty() {
            groups.push(
                json!({"first_row": first, "rows": rows, "matching_rows": matches.len(),
                "output_pages": output_pages, "matching_output_pages": matching_output_pages}),
            );
        }
    }
    Ok(
        json!({"matching_ordinals": positions, "matching_groups": groups,
        "maximum_page_rows": maximum_page_rows, "full_value_roundtrip": "passed",
        "scope": "actual stored row positions and page boundaries; not reader decode counters"}),
    )
}

fn write_stream<R: io::Read>(
    input: R,
    request: &Value,
    output: &Path,
    budget: Arc<Budget>,
) -> Result<Vec<Value>> {
    let name = request["shape"].as_str().ok_or("shape")?;
    let (_, stripes, metrics) = dimensions(
        name,
        request["file_target_mib"].as_u64().ok_or("file target")?,
    )?;
    let page_rows = request["data_page_rows"].as_u64().ok_or("page row limit")? as usize;
    if ![2048, 20000].contains(&page_rows) {
        return Err("page row limit must be 2048 or 20000".into());
    }
    let files = request["files"].as_array().ok_or("planned files")?;
    let selected: BTreeSet<usize> = request["selected"]
        .as_array()
        .ok_or("selected files")?
        .iter()
        .map(|n| n.as_u64().map(|v| v as usize).ok_or("selected file index"))
        .collect::<std::result::Result<_, _>>()?;
    let probe = request["mode"] == "probe";
    let layouts = if probe {
        vec!["localized", "scattered"]
    } else {
        match request["layout"].as_str() {
            Some("localized") => vec!["localized"],
            Some("scattered") => vec!["scattered"],
            _ => return Err("invalid layout".into()),
        }
    };
    let max_rows = files
        .iter()
        .map(|f| f["rows"].as_u64().ok_or("file row count"))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .ok_or("empty file list")? as usize;
    if max_rows == 0
        || max_rows > fixtures::FILE_ROWS
        || files.iter().any(|f| f["rows"] == 0)
        || selected.is_empty()
        || selected.iter().any(|&n| n >= files.len())
        || selected.len()
            != request["selected"]
                .as_array()
                .ok_or("selected files")?
                .len()
        || (!probe && selected.len() != files.len())
    {
        return Err("invalid production file selection or size".into());
    }
    let max_groups = max_rows.div_ceil(GROUP_ROWS);
    // ponytail: conservative per-file admission estimate; the 8 GiB process limit remains authoritative.
    // Retain one wide expected file, original rows, group encoder/readback buffers and metadata.
    let worker_bytes = max_rows * ((80 + metrics) * 9 + 256)
        + GROUP_ROWS * (80 + metrics) * 24
        + 128 * MIB as usize;
    let write_threads = std::thread::available_parallelism()?
        .get()
        .min((7 * 1024 * MIB as usize / worker_bytes).max(1));
    let properties = fixtures::writer_properties()?
        .into_builder()
        .set_max_row_group_row_count(Some(GROUP_ROWS))
        .set_write_batch_size(WRITE_BATCH_ROWS)
        .set_data_page_row_count_limit(page_rows)
        .set_dictionary_enabled(false)
        .build();
    let projection = request["projection"]
        .as_array()
        .ok_or("projection")?
        .iter()
        .map(|v| v.as_str().map(str::to_owned).ok_or("projection column"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut writers = Vec::new();
    for layout in layouts {
        let id = format!("production.{name}.{layout}");
        writers.push((id, layout, Vec::new(), Vec::new(), [0_u128; 3]));
    }
    let mut reader = StreamReader::try_new(input, None)?;
    let mut pending = None;
    let mut file_number = 0;
    for (chunk, planned_files) in files.chunks(write_threads).enumerate() {
        let mut jobs = Vec::new();
        for (offset, planned) in planned_files.iter().enumerate() {
            let index = chunk * write_threads + offset;
            let rows = planned["rows"].as_u64().ok_or("planned rows")? as usize;
            let batch = next_file(&mut reader, &mut pending, rows)?;
            let orders = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("orders")?;
            if orders
                .values()
                .iter()
                .any(|&key| key % stripes != planned["stripe"].as_i64().unwrap_or(-1))
                || planned["matching_rows"] != matching(&batch)?.len()
            {
                return Err(format!(
                    "source stream differs from planned stripe/matches at file {index}"
                )
                .into());
            }
            if selected.contains(&index) {
                jobs.push((index, file_number, batch));
                file_number += 1;
            }
        }
        for (id, layout, completed, evidence, totals) in &mut writers {
            let path = output.join(&*id);
            let scattered = *layout == "scattered";
            let results = std::thread::scope(|scope| -> Result<Vec<_>> {
                let handles: Vec<_> = jobs
                    .iter()
                    .map(|(index, number, batch)| {
                        let path = &path;
                        let budget = budget.clone();
                        let properties = properties.clone();
                        let projection = &projection;
                        scope.spawn(move || -> Result<_> {
                            let planned = &files[*index];
                            let rows = batch.num_rows();
                            let mut writer = TableWriter::with_geometry(
                                path,
                                schema(metrics),
                                budget,
                                properties,
                                max_groups,
                            )?
                            .with_geometry_sidecars(true)
                            .with_first_file_index(*number);
                            let started = Instant::now();
                            let mut expected = Vec::new();
                            for start in (0..rows).step_by(GROUP_ROWS) {
                                let group = reorder(
                                    &batch.slice(start, (rows - start).min(GROUP_ROWS)),
                                    scattered,
                                )?;
                                let group = expand(&group, metrics)?;
                                expected.push(group);
                            }
                            let expansion_ms = started.elapsed().as_millis();
                            let started = Instant::now();
                            for group in &expected {
                                writer.push(group.clone())?;
                            }
                            let table = writer.finish(None)?;
                            let file = &table["files"][0];
                            let writing_ms = started.elapsed().as_millis();
                            if candidate(&file["delta_stats"])?
                                != planned["candidate"].as_bool().ok_or("candidate flag")?
                            {
                                return Err(
                                    "actual Delta statistics disagree with planned candidates"
                                        .into(),
                                );
                            }
                            let started = Instant::now();
                            let mut actual = physical_evidence(
                                path,
                                file,
                                &expected,
                                &projection,
                                page_rows.div_ceil(WRITE_BATCH_ROWS) * WRITE_BATCH_ROWS,
                            )?;
                            actual["source_file_ordinal"] = json!(*index);
                            actual["planned"] = planned.clone();
                            actual["path"] = file["path"].clone();
                            Ok((
                                file.clone(),
                                actual,
                                [expansion_ms, writing_ms, started.elapsed().as_millis()],
                            ))
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().map_err(|_| "production file worker panicked")?)
                    .collect()
            })?;
            for (file, actual, times) in results {
                completed.push(file);
                evidence.push(actual);
                for (total, elapsed) in totals.iter_mut().zip(times) {
                    *total += elapsed;
                }
            }
        }
        if !jobs.is_empty() && (chunk % 128_usize.div_ceil(write_threads) == 0 || probe) {
            eprintln!(
                "production {name}: wrote source file {}/{}",
                (chunk * write_threads + planned_files.len()),
                files.len()
            );
        }
    }
    if pending.is_some() || reader.next().is_some() {
        return Err("source stream has extra rows/batches".into());
    }
    let mut result = Vec::new();
    for (id, layout, completed, evidence, totals) in writers {
        let mut table = TableWriter::finish_files(
            &output.join(&id),
            schema(metrics),
            budget.clone(),
            completed,
            if probe {
                None
            } else {
                Some(("production-sf10", &id))
            },
        )?;
        table["id"] = json!(id);
        table["path"] = table["id"].clone();
        table["layout"] = json!(layout);
        table["scale_factor"] = json!(10);
        table["snapshot_version"] = json!(0);
        table["deletion_vectors"] = json!(false);
        table["file_target_mib"] = request["file_target_mib"].clone();
        table["file_evidence"] = json!(evidence);
        table["preparation_ms_summed_across_files"] = json!({
            "payloads_metrics_and_scatter": totals[0], "parquet_write_and_statistics_readback": totals[1],
            "exact_readback_and_page_evidence": totals[2], "threads": write_threads,
            "estimated_worker_bytes": worker_bytes});
        let mut settings = fixtures::writer_settings();
        settings["row_group_rows"] = json!(GROUP_ROWS);
        settings["groups_per_file"] = json!(max_groups);
        settings["write_batch_rows"] = json!(WRITE_BATCH_ROWS);
        settings["data_page_rows"] = json!(page_rows);
        settings["dictionary"] = json!(false);
        table["writer"] = settings;
        result.push(table);
    }
    Ok(result)
}

pub fn run(args: &[String]) -> Result<()> {
    if args.len() != 3 {
        return Err("production-write REQUEST_JSON OUTPUT_DIRECTORY ELAPSED_SECONDS".into());
    }
    let seconds: u32 = args[2].parse()?;
    if seconds == 0 {
        return Err("positive deadline required".into());
    }
    large::set_deadline(seconds)?;
    fixtures::limit_memory(8 * 1024 * MIB)?;
    #[cfg(target_os = "linux")]
    // SAFETY: installs a kernel-delivered termination signal if the IPC producer dies.
    unsafe {
        let parent = libc::getppid();
        if parent <= 1
            || libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0
            || libc::getppid() != parent
        {
            return Err("cannot supervise production writer parent".into());
        }
    }
    let bytes = fs::read(&args[0])?;
    let request: Value = serde_json::from_slice(&bytes)?;
    if request["format"] != "selective-read-production-write-v1"
        || request["contract_sha256"] != fixtures::hash_bytes(CONTRACT.as_bytes())
        || request["driver_sha256"] != fixtures::hash_bytes(DRIVER.as_bytes())
        || !["probe", "generate"].iter().any(|s| request["mode"] == *s)
    {
        return Err("unknown or stale production writer request".into());
    }
    let (count, _, _) = dimensions(
        request["shape"].as_str().ok_or("shape")?,
        request["file_target_mib"].as_u64().ok_or("file target")?,
    )?;
    if request["files"].as_array().ok_or("files")?.len() != count
        || (request["mode"] == "generate"
            && request["selected"].as_array().ok_or("selection")?.len() != count)
    {
        return Err("incomplete production file inventory".into());
    }
    let limit = request["output_limit_bytes"]
        .as_u64()
        .ok_or("output ceiling")?;
    if limit == 0 || limit > 192 * 1024 * MIB {
        return Err("output ceiling must be within 192 GiB".into());
    }
    let budget = Arc::new(Budget::new(limit));
    let started = Instant::now();
    let output = Path::new(&args[1]);
    let tables = write_stream(io::stdin().lock(), &request, output, budget.clone())?;
    let result = json!({"status": "complete", "request_sha256": fixtures::hash_bytes(&bytes),
        "generator": generator_identity()?, "tables": tables, "elapsed_ms": started.elapsed().as_millis(),
        "peak_rss_bytes": fixtures::peak_rss()?, "output_bytes_before_result": budget.written_bytes()});
    budget.write(
        &output.join("writer-result.json"),
        &serde_json::to_vec_pretty(&result)?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ipc::writer::StreamWriter;

    #[test]
    fn streamed_files_preserve_values_groups_and_scatter_pages() -> Result<()> {
        let root = tempfile::tempdir()?;
        // Cross IPC, row-group and file boundaries with different row counts.
        let file_rows = GROUP_ROWS + 3;
        let total_rows = file_rows * 2 + 1;
        let rows: Vec<_> = LineItemGenerator::new(0.1, 1, 1)
            .into_iter()
            .take(total_rows)
            .collect();
        let source = fixtures::source_batch(&rows)?;
        let mut columns = source.columns().to_vec();
        columns[0] = Arc::new(Int64Array::from_iter_values((0..total_rows).map(|r| {
            if r < file_rows {
                r as i64 * 30
            } else {
                (r - file_rows) as i64 * 30 + 1
            }
        })));
        columns[3] = Arc::new(Int32Array::from(vec![1; total_rows]));
        columns[10] = Arc::new(Date32Array::from_iter_values((0..total_rows).map(|r| {
            if r < 16 || (file_rows..file_rows + 16).contains(&r) {
                9204
            } else {
                9205
            }
        })));
        columns[14] = Arc::new(StringArray::from(vec!["AIR"; total_rows]));
        let source = RecordBatch::try_new(original_schema(), columns)?;
        let mut ipc = Vec::new();
        let mut stream = StreamWriter::try_new(&mut ipc, &source.schema())?;
        for r in (0..total_rows).step_by(997) {
            stream.write(&source.slice(r, (total_rows - r).min(997)))?;
        }
        stream.finish()?;
        drop(stream);
        let mut request = json!({"mode": "probe", "shape": "q4",
            "file_target_mib": 512, "data_page_rows": 2048, "selected": [0, 1],
            "projection": ["l_orderkey", "payload_00"], "files": [
                {"stripe": 0, "file_index": 0, "rows": file_rows, "candidate": true, "matching_rows": 16},
                {"stripe": 1, "file_index": 0, "rows": file_rows + 1, "candidate": true, "matching_rows": 16}]});
        for (name, page_rows) in [("q4", 2048_usize), ("q2", 2048), ("q4", 20000)] {
            request["shape"] = json!(name);
            request["data_page_rows"] = json!(page_rows);
            let output = root.path().join(format!("{name}-{page_rows}"));
            let tables = write_stream(
                ipc.as_slice(),
                &request,
                &output,
                Arc::new(Budget::new(1024 * MIB)),
            )?;
            for table in &tables {
                assert_eq!(table["rows"], total_rows);
                assert_eq!(table["file_count"], 2);
                assert_eq!(
                    table["schema"]["fields"].as_array().unwrap().len(),
                    if name == "q4" { 90 } else { 416 }
                );
                for (i, file) in table["files"].as_array().unwrap().iter().enumerate() {
                    let batches = fixtures::read_batches(
                        &output
                            .join(table["path"].as_str().unwrap())
                            .join(file["path"].as_str().unwrap()),
                    )?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                    let stored = concat_batches(&batches[0].schema(), &batches)?;
                    let original = source.slice(if i == 0 { 0 } else { file_rows }, file_rows + i);
                    let sorted_keys = |batch: &RecordBatch| {
                        let mut keys = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values()
                            .to_vec();
                        keys.sort_unstable();
                        keys
                    };
                    for start in (0..original.num_rows()).step_by(GROUP_ROWS) {
                        let len = (original.num_rows() - start).min(GROUP_ROWS);
                        assert_eq!(
                            sorted_keys(&stored.slice(start, len)),
                            sorted_keys(&original.slice(start, len))
                        );
                    }
                    // Independent arithmetic check for every synthetic metric, including nulls.
                    let orders = stored
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let parts = stored
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let suppliers = stored
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for j in 0..stored.num_columns() - 80 {
                        let values = stored
                            .column(80 + j)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        for r in 0..stored.num_rows() {
                            assert_eq!(
                                values.is_null(r),
                                (orders.value(r) + 1 + j as i64) % 17 == 0
                            );
                            if !values.is_null(r) {
                                assert_eq!(
                                    values.value(r),
                                    (parts.value(r) + (j as i64 + 1) * suppliers.value(r) + 1)
                                        % 1024
                                );
                            }
                        }
                    }
                    let evidence = &table["file_evidence"][i];
                    assert_eq!(evidence["full_value_roundtrip"], "passed");
                    assert_eq!(
                        evidence["maximum_page_rows"],
                        page_rows.div_ceil(WRITE_BATCH_ROWS) * WRITE_BATCH_ROWS
                    );
                    assert_eq!(table["writer"]["dictionary"], false);
                    assert_eq!(evidence["matching_groups"].as_array().unwrap().len(), 1);
                    let pages = evidence["matching_groups"][0]["matching_output_pages"]
                        .as_u64()
                        .unwrap();
                    if table["layout"] == "localized" {
                        assert_eq!(pages, 2);
                    } else {
                        assert!(pages > 2);
                    }
                }
            }
        }
        // Reject duplicated selections and a truncated source instead of publishing partial data.
        request["selected"] = json!([0, 0]);
        assert!(
            write_stream(
                ipc.as_slice(),
                &request,
                &root.path().join("duplicate"),
                Arc::new(Budget::new(1024 * MIB))
            )
            .is_err()
        );
        request["selected"] = json!([0, 1]);
        assert!(
            write_stream(
                &ipc[..ipc.len() / 2],
                &request,
                &root.path().join("truncated"),
                Arc::new(Budget::new(1024 * MIB))
            )
            .is_err()
        );
        Ok(())
    }
}
