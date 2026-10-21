use super::{
    Result, State,
    files::{self, Identity},
    require,
    rows::{Layout, Row},
    rules,
    spec::Spec,
};
use crate::selective_read::array;
use arrow_array::{Array, Date32Array, Int32Array, Int64Array, RecordBatch, StringArray};
use serde_json::{Value, json};
use std::{collections::HashMap, path::Path};

#[derive(Clone)]
pub(super) struct Source {
    pub counts: [u64; 4],
    pub rows: Vec<Row>,
}
#[derive(Clone)]
pub(super) struct Physical {
    pub rows: u64,
    pub ordinals: Vec<u64>,
    pub matches: Vec<Row>,
}
#[derive(Default)]
pub(super) struct Cache {
    pub sources: HashMap<Vec<(Identity, u64, String)>, Source>,
    pub files: HashMap<(Identity, usize), Physical>,
}
fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref())
        .ok_or_else(|| format!("wrong stored type: {name}").into())
}
pub(super) fn keys(batch: &RecordBatch) -> Result<impl Iterator<Item = [i64; 2]> + '_> {
    let order = column::<Int64Array>(batch, "l_orderkey")?;
    let line = column::<Int32Array>(batch, "l_linenumber")?;
    require(
        order.null_count() == 0 && line.null_count() == 0,
        "null in row key",
    )?;
    Ok((0..batch.num_rows()).map(move |i| [order.value(i), i64::from(line.value(i))]))
}
pub(super) fn stages(batch: &RecordBatch) -> Result<Vec<usize>> {
    let date = column::<Date32Array>(batch, "l_shipdate")?;
    let mode = column::<StringArray>(batch, "l_shipmode")?;
    let line = column::<Int32Array>(batch, "l_linenumber")?;
    let super::rows::Cell::Int(day) = rules::date("1995-03-15")? else {
        unreachable!()
    };
    Ok((0..batch.num_rows())
        .map(|i| {
            if date.value(i) as i64 != day {
                0
            } else if mode.value(i) != "AIR" {
                1
            } else if line.value(i) != 1 {
                2
            } else {
                3
            }
        })
        .collect())
}
pub(super) fn metrics(batch: &RecordBatch, count: usize) -> Result<()> {
    let order = column::<Int64Array>(batch, "l_orderkey")?;
    let line = column::<Int32Array>(batch, "l_linenumber")?;
    let part = column::<Int64Array>(batch, "l_partkey")?;
    let supplier = column::<Int64Array>(batch, "l_suppkey")?;
    for j in 0..count {
        let actual = column::<Int64Array>(batch, &format!("metric_{j:03}"))?;
        for i in 0..batch.num_rows() {
            let line = i64::from(line.value(i));
            let null = (order.value(i) + line + j as i64) % 17 == 0;
            require(
                actual.is_null(i) == null
                    && (null
                        || actual.value(i)
                            == (part.value(i) + supplier.value(i) * (j as i64 + 1) + line) % 1024),
                "wrong stored metric values/nulls",
            )?;
        }
    }
    Ok(())
}
impl State {
    pub(super) fn source_counts(&mut self, request: &Value) -> Result<Value> {
        let projection =
            ["l_orderkey", "l_linenumber", "l_shipdate", "l_shipmode"].map(str::to_owned);
        let layout = Layout::new(&projection)?;
        let mut counts = [0u64; 4];
        for path in array(&request["paths"])? {
            let mut parquet = self
                .decoder
                .open(Path::new(crate::selective_read::text(path)?), &projection)?;
            for batch in parquet.by_ref() {
                let batch = layout.normalize(batch?)?;
                for stage in stages(&batch)? {
                    for count in &mut counts[..=stage] {
                        *count += 1;
                    }
                }
            }
            parquet.finish()?;
        }
        Ok(json!(counts))
    }
    pub(super) fn source_scan(&mut self, fixtures: &Path, spec: &Spec) -> Result<(Source, bool)> {
        let source = spec.source.as_ref().ok_or("missing production source")?;
        let items = array(&source["files"])?;
        let identities = items
            .iter()
            .map(|f| {
                Ok((
                    files::identity(&files::path(fixtures, source, f)?)?,
                    rules::integer(&f["rows"])?,
                    self.digests.digest(&files::path(fixtures, source, f)?)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(scan) = self.cache.sources.get(&identities) {
            require(
                scan.counts[0] == rules::integer(&source["rows"])?,
                "source row count changed",
            )?;
            return Ok((scan.clone(), true));
        }
        eprintln!("production oracle: scan original source");
        let input = Layout::new(&rules::original())?;
        let output = Layout::new(&rules::wide())?;
        let mut scan = Source {
            counts: [0; 4],
            rows: Vec::new(),
        };
        let mut previous = None;
        for (item, identity) in items.iter().zip(&identities) {
            let path = files::path(fixtures, source, item)?;
            let mut parquet = self.decoder.open(&path, &[])?;
            input.check(parquet.schema().as_ref())?;
            let mut count = 0;
            for batch in parquet.by_ref() {
                let batch = input.normalize(batch?)?;
                let selected = stages(&batch)?;
                for (i, key) in keys(&batch)?.enumerate() {
                    require(
                        previous.is_none_or(|old| old < key),
                        "source keys duplicated or unordered",
                    )?;
                    previous = Some(key);
                    for n in &mut scan.counts[..=selected[i]] {
                        *n += 1;
                    }
                    if selected[i] == 3 {
                        scan.rows.push(rules::project(
                            &input.read_row(&batch, i)?,
                            &input,
                            &output,
                            true,
                        )?);
                    }
                }
                count += batch.num_rows() as u64;
            }
            parquet.finish()?;
            require(
                count == rules::integer(&item["rows"])? && files::identity(&path)? == identity.0,
                "source changed during scan",
            )?;
        }
        require(
            scan.counts[0] == rules::integer(&source["rows"])?
                && scan.counts.windows(2).all(|n| n[0] > n[1] && n[1] > 0),
            "source predicate geometry changed",
        )?;
        require(
            (300..=2000).contains(&scan.counts[3]),
            "production output exceeds the declared hundreds-of-rows shape",
        )?;
        self.cache.sources.insert(identities, scan.clone());
        Ok((scan, false))
    }
    pub(super) fn scan_metadata(source_reused: bool) -> Value {
        json!({"full_column_scans":0,"reused_full_column_scans":0,"source_scan_reused":source_reused,"scan_batch_rows":super::rows::BATCH_ROWS,
            "reuse_guard":"unchanged local device/inode/size/mtime_ns/ctime_ns"})
    }
}
