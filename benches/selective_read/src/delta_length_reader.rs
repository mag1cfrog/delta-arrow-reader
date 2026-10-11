//! Experimental transport for the generated #452 tables. All query paths share it.

use std::{
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    basic::{Compression, Type},
    errors::{ParquetError, Result},
    file::metadata::ParquetMetaData,
};
use serde_json::{Value, json};
use thrift::protocol::TType;
use tokio::sync::Semaphore;

#[allow(dead_code)]
#[path = "../../../src/reader/backend/direct_parquet/compact_thrift.rs"]
mod compact_thrift;
#[allow(dead_code)]
#[path = "../../../src/reader/backend/direct_parquet/range_planning.rs"]
mod range_planning;
use super::delta_length;
use compact_thrift::{invalid_metadata, read_field, read_i64};
use range_planning::{
    TransportEstimate, choose_bounded_range_plan, partial_plan_cost, range_bytes,
};

const CONCURRENCY: usize = 32;
const PREFIX_BYTES: u64 = 8192;

#[derive(Clone, Copy)]
pub struct CostModel {
    transport: TransportEstimate,
    request_overhead_bytes: u128,
}

impl CostModel {
    pub fn new(profile: &Value) -> Result<Self> {
        let number = |name| {
            profile[name]
                .as_u64()
                .ok_or_else(|| invalid("cost profile"))
        };
        let latency = number("latency_ms")?;
        let bandwidth = number("mbps")?;
        if latency > 60000 || !(1..=100000).contains(&bandwidth) {
            return Err(invalid("cost profile outside experiment bounds"));
        }
        Ok(Self {
            transport: TransportEstimate {
                request_latency: Duration::from_millis(latency),
                shared_throughput_bytes_per_second: bandwidth * 125000,
            },
            request_overhead_bytes: u128::from(number("request_overhead_bytes")?),
        })
    }

    fn cost(self, bytes: u128, requests: usize) -> u128 {
        partial_plan_cost(
            bytes,
            requests,
            self.transport,
            CONCURRENCY,
            self.request_overhead_bytes,
        )
    }
}

// Bound the modeled work lost if lengths reveal that no payload can be skipped.
struct ProbeBudget {
    model: CostModel,
    limit: u128,
    spent: u128,
    rounds: [(u128, usize); 9],
}

impl ProbeBudget {
    fn new(model: CostModel, ordinary: &[Range<u64>], pages: usize) -> Option<Self> {
        let mut budget = Self {
            model,
            limit: model.cost(range_bytes(ordinary), ordinary.len()) / 10,
            spent: 0,
            rounds: [(0, 0); 9],
        };
        budget
            .reserve(0, u128::from(PREFIX_BYTES) * pages as u128, pages)
            .then_some(budget)
    }

    fn reserve(&mut self, round: usize, bytes: u128, requests: usize) -> bool {
        let Some(&(previous_bytes, previous_requests)) = self.rounds.get(round) else {
            return false;
        };
        let next = (previous_bytes + bytes, previous_requests + requests);
        let spent = self.spent - self.model.cost(previous_bytes, previous_requests)
            + self.model.cost(next.0, next.1);
        if spent > self.limit {
            return false;
        }
        self.rounds[round] = next;
        self.spent = spent;
        true
    }
}

#[derive(Default)]
pub struct Metrics {
    requests: AtomicU64,
    bytes: AtomicU64,
    discovery_requests: AtomicU64,
    partial_pages: AtomicU64,
    rejected_pages: AtomicU64,
    fallback_pages: AtomicU64,
    max_discovery_rounds: AtomicU64,
    length_and_null_bytes: AtomicU64,
    selected_payload_bytes: AtomicU64,
    logical_prefix_chunks: AtomicU64,
    cost_rejected_pages: AtomicU64,
    probe_budget_exhausted: AtomicU64,
}

impl Metrics {
    pub fn snapshot(&self) -> Value {
        let get = |v: &AtomicU64| v.load(Ordering::Relaxed);
        json!({"data_requests":get(&self.requests),"data_bytes":get(&self.bytes),
            "discovery_requests":get(&self.discovery_requests),"partial_pages":get(&self.partial_pages),
            "rejected_pages":get(&self.rejected_pages),"fallback_pages":get(&self.fallback_pages),
            "max_discovery_rounds":get(&self.max_discovery_rounds),"length_and_null_bytes":get(&self.length_and_null_bytes),
            "selected_payload_bytes":get(&self.selected_payload_bytes),"logical_prefix_chunks":get(&self.logical_prefix_chunks),
            "cost_rejected_pages":get(&self.cost_rejected_pages),"probe_budget_exhausted":get(&self.probe_budget_exhausted)})
    }
}

pub struct Io {
    pub store: Arc<dyn ObjectStore>,
    pub metrics: Metrics,
    permits: Semaphore,
    cost_model: Option<CostModel>,
}

impl Io {
    pub fn new(store: Arc<dyn ObjectStore>, cost_model: Option<CostModel>) -> Self {
        Self {
            store,
            metrics: Metrics::default(),
            permits: Semaphore::new(CONCURRENCY),
            cost_model,
        }
    }

    async fn read(&self, path: &Path, range: Range<u64>, discovery: bool) -> Result<Bytes> {
        let _permit = self.permits.acquire().await.unwrap();
        let size = range.end - range.start;
        let bytes = self.store.get_range(path, range).await?;
        if bytes.len() as u64 != size {
            return Err(invalid("short object-store response"));
        }
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        self.metrics.bytes.fetch_add(size, Ordering::Relaxed);
        if discovery {
            self.metrics
                .discovery_requests
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(bytes)
    }
}

#[derive(Clone)]
struct Page {
    range: Range<u64>,
    rows: usize,
    nullable: bool,
    selected: Vec<usize>,
}

#[derive(Clone)]
pub struct Reader {
    io: Arc<Io>,
    path: Path,
    metadata: Arc<ParquetMetaData>,
    pages: Arc<Vec<Page>>,
}

impl Reader {
    pub fn new(
        io: Arc<Io>,
        path: Path,
        metadata: Arc<ParquetMetaData>,
        selected: &[bool],
        experimental: bool,
    ) -> Result<Self> {
        let mut pages = Vec::new();
        if experimental && io.cost_model.is_some() {
            let indexes = metadata
                .offset_index()
                .ok_or_else(|| invalid("missing offset index"))?;
            let mut first = 0;
            for (group, index) in indexes.iter().enumerate() {
                let row_group = metadata.row_group(group);
                for (column, index) in index.iter().enumerate().skip(3) {
                    let column = row_group.column(column);
                    for (i, location) in index.page_locations().iter().enumerate() {
                        let end_row = index
                            .page_locations()
                            .get(i + 1)
                            .map_or(row_group.num_rows(), |next| next.first_row_index);
                        let start_row = usize::try_from(location.first_row_index)
                            .map_err(|_| invalid("page row index"))?;
                        let rows = usize::try_from(end_row - location.first_row_index)
                            .map_err(|_| invalid("page row count"))?;
                        let size = location.compressed_page_size;
                        let supported = column.column_type() == Type::BYTE_ARRAY
                            && column.column_descr().max_rep_level() == 0
                            && column.column_descr().max_def_level() <= 1
                            && column.compression() == Compression::UNCOMPRESSED;
                        // Predeclared experiment scope: sparse, large pages with substantial payload per row.
                        if !supported
                            || rows > 1048576
                            || !(1048576..=8388608).contains(&size)
                            || size as usize / rows.max(1) < 64
                        {
                            io.metrics.rejected_pages.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        let selection = selected
                            .get(first + start_row..first + start_row + rows)
                            .ok_or_else(|| invalid("selection boundary"))?;
                        let keep: Vec<_> = selection
                            .iter()
                            .enumerate()
                            .filter_map(|(row, keep)| keep.then_some(row))
                            .collect();
                        if keep.is_empty() || keep.len() > rows / 8 {
                            io.metrics.rejected_pages.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        let start =
                            u64::try_from(location.offset).map_err(|_| invalid("page offset"))?;
                        pages.push(Page {
                            range: start..start + size as u64,
                            rows,
                            nullable: column.column_descr().max_def_level() == 1,
                            selected: keep,
                        });
                    }
                }
                first += row_group.num_rows() as usize;
            }
            if first != selected.len() {
                return Err(invalid("file selection count"));
            }
            pages.sort_unstable_by_key(|page| page.range.start);
        }
        Ok(Self {
            io,
            path,
            metadata,
            pages: Arc::new(pages),
        })
    }

    async fn read_ranges(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let pages = self
            .pages
            .iter()
            .filter(|page| {
                ranges
                    .iter()
                    .any(|range| range.start <= page.range.start && page.range.end <= range.end)
            })
            .count();
        let budget = self
            .io
            .cost_model
            .filter(|_| pages != 0)
            .and_then(|model| ProbeBudget::new(model, &ranges, pages))
            .map(Mutex::new);
        if pages != 0 && budget.is_none() {
            self.io
                .metrics
                .cost_rejected_pages
                .fetch_add(pages as u64, Ordering::Relaxed);
        }
        stream::iter(ranges)
            .map(|range| async {
                if let Some(budget) = &budget {
                    self.range(range, budget).await
                } else {
                    self.io.read(&self.path, range, false).await
                }
            })
            .buffered(CONCURRENCY)
            .try_collect()
            .await
    }

    async fn range(&self, range: Range<u64>, budget: &Mutex<ProbeBudget>) -> Result<Bytes> {
        let pages: Vec<_> = self
            .pages
            .iter()
            .filter(|page| page.range.start < range.end && page.range.end > range.start)
            .cloned()
            .collect();
        if pages.is_empty()
            || pages
                .iter()
                .any(|page| page.range.start < range.start || page.range.end > range.end)
        {
            return self.io.read(&self.path, range, false).await;
        }
        let mut parts = Vec::new();
        let mut cursor = range.start;
        for page in pages {
            if cursor < page.range.start {
                parts.push((cursor..page.range.start, None));
            }
            cursor = page.range.end;
            parts.push((page.range.clone(), Some(page)));
        }
        if cursor < range.end {
            parts.push((cursor..range.end, None));
        }
        let pieces: Vec<_> = stream::iter(parts)
            .map(|(range, page)| {
                let reader = self.clone();
                async move {
                    match page {
                        Some(page) => reader.page(&page, budget).await,
                        None => reader.io.read(&reader.path, range, false).await,
                    }
                }
            })
            .buffered(CONCURRENCY)
            .try_collect()
            .await?;
        let mut output = Vec::with_capacity((range.end - range.start) as usize);
        for bytes in pieces {
            output.extend_from_slice(&bytes);
        }
        Ok(output.into())
    }

    async fn page(&self, page: &Page, budget: &Mutex<ProbeBudget>) -> Result<Bytes> {
        let prefix = self
            .io
            .read(
                &self.path,
                page.range.start..page.range.start + PREFIX_BYTES,
                true,
            )
            .await?;
        let Some(header) = header_size(&prefix, page.rows, page.range.end - page.range.start)?
        else {
            self.io
                .metrics
                .fallback_pages
                .fetch_add(1, Ordering::Relaxed);
            return self.finish_page(page, prefix.to_vec()).await;
        };
        let body_start = page.range.start + header as u64;
        let body_size = (page.range.end - body_start) as usize;
        let cached = Arc::new(Mutex::new(prefix[header..].to_vec()));
        let rounds = Arc::new(AtomicU64::new(1));
        let plan = delta_length::discover(
            body_size,
            page.rows,
            page.nullable,
            &page.selected,
            |range| {
                let cached = cached.clone();
                let rounds = rounds.clone();
                let io = self.io.clone();
                let path = self.path.clone();
                async move {
                    let start = cached.lock().unwrap().len();
                    if range.end > start {
                        if !budget.lock().unwrap().reserve(
                            rounds.load(Ordering::Relaxed) as usize,
                            (range.end - start) as u128,
                            1,
                        ) {
                            io.metrics
                                .probe_budget_exhausted
                                .fetch_add(1, Ordering::Relaxed);
                            return Err(delta_length::Error::Fallback("modeled discovery budget"));
                        }
                        let bytes = io
                            .read(
                                &path,
                                body_start + start as u64..body_start + range.end as u64,
                                true,
                            )
                            .await
                            .map_err(|error| delta_length::Error::Io(error.to_string()))?;
                        cached.lock().unwrap().extend_from_slice(&bytes);
                        rounds.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(cached.lock().unwrap()[range].to_vec())
                }
            },
        )
        .await;
        let cached = std::mem::take(&mut *cached.lock().unwrap());
        self.io
            .metrics
            .max_discovery_rounds
            .fetch_max(rounds.load(Ordering::Relaxed), Ordering::Relaxed);
        let plan = match plan {
            Ok(plan) => plan,
            Err(delta_length::Error::Fallback(_)) => {
                self.io
                    .metrics
                    .fallback_pages
                    .fetch_add(1, Ordering::Relaxed);
                let mut bytes = prefix[..header].to_vec();
                bytes.extend_from_slice(&cached);
                return self.finish_page(page, bytes).await;
            }
            Err(error) => return Err(invalid(&format!("length discovery: {error:?}"))),
        };
        assert!(cached.starts_with(&plan.prefix));
        self.io
            .metrics
            .length_and_null_bytes
            .fetch_add(plan.payload_start as u64, Ordering::Relaxed);
        self.io
            .metrics
            .selected_payload_bytes
            .fetch_add(plan.selected_bytes as u64, Ordering::Relaxed);
        self.io
            .metrics
            .logical_prefix_chunks
            .fetch_add(plan.probes as u64, Ordering::Relaxed);
        let ranges: Vec<_> = plan
            .ranges
            .iter()
            .filter_map(|range| {
                let start = range.start.max(cached.len());
                (start < range.end).then_some(start..range.end)
            })
            .collect();
        let model = self.io.cost_model.expect("budget requires a cost model");
        let ranges: Vec<_> = ranges
            .iter()
            .map(|range| range.start as u64..range.end as u64)
            .collect();
        let ranges = choose_bounded_range_plan(
            &ranges,
            model.transport,
            CONCURRENCY,
            (body_size - cached.len()) as u128,
            model.request_overhead_bytes,
        )
        .ok_or_else(|| invalid("payload plan exceeded remaining bytes"))?;
        let remaining_bytes = range_bytes(&ranges);
        let ordinary_cost = model.cost((body_size - cached.len()) as u128, 1);
        let partial_cost = model.cost(remaining_bytes, ranges.len());
        // A few very long selected values can defeat sparse row selection.
        if cached.len() as u128 + remaining_bytes > (body_size * 3 / 4) as u128
            || partial_cost.saturating_mul(100) >= ordinary_cost.saturating_mul(90)
        {
            self.io
                .metrics
                .fallback_pages
                .fetch_add(1, Ordering::Relaxed);
            let mut bytes = prefix[..header].to_vec();
            bytes.extend_from_slice(&cached);
            return self.finish_page(page, bytes).await;
        }
        let mut output = vec![0; (page.range.end - page.range.start) as usize];
        output[..header].copy_from_slice(&prefix[..header]);
        output[header..header + cached.len()].copy_from_slice(&cached);
        let pieces: Vec<_> = stream::iter(ranges)
            .map(|range| async move {
                let bytes = self
                    .io
                    .read(
                        &self.path,
                        body_start + range.start..body_start + range.end,
                        false,
                    )
                    .await?;
                Ok::<_, ParquetError>((range, bytes))
            })
            .buffered(CONCURRENCY)
            .try_collect()
            .await?;
        for (range, bytes) in pieces {
            output[header + range.start as usize..header + range.end as usize]
                .copy_from_slice(&bytes);
        }
        self.io
            .metrics
            .partial_pages
            .fetch_add(1, Ordering::Relaxed);
        Ok(output.into())
    }

    async fn finish_page(&self, page: &Page, mut prefix: Vec<u8>) -> Result<Bytes> {
        let start = page.range.start + prefix.len() as u64;
        if start < page.range.end {
            prefix.extend_from_slice(
                &self
                    .io
                    .read(&self.path, start..page.range.end, false)
                    .await?,
            );
        }
        Ok(prefix.into())
    }
}

impl AsyncFileReader for Reader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        async move { Ok(self.read_ranges(vec![range]).await?.remove(0)) }.boxed()
    }
    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        self.read_ranges(ranges).boxed()
    }
    fn get_metadata<'a>(
        &'a mut self,
        _: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        futures_util::future::ready(Ok(self.metadata.clone())).boxed()
    }
}

fn invalid(message: &str) -> ParquetError {
    ParquetError::General(message.into())
}

fn header_size(bytes: &[u8], rows: usize, total_bytes: u64) -> Result<Option<usize>> {
    let field = |bytes: &[u8], id| {
        read_field(&mut &*bytes, id, TType::I32, |cursor| {
            i32::try_from(read_i64(cursor)?).map_err(|_| invalid_metadata())
        })
    };
    let parse = || -> thrift::Result<_> {
        let mut cursor = bytes;
        let data = read_field(&mut cursor, 5, TType::Struct, |cursor| {
            let start = *cursor;
            let count = read_field(cursor, 1, TType::I32, read_i64)?;
            Ok((count, field(start, 2)?, field(start, 3)?, field(start, 4)?))
        })?;
        Ok((
            bytes.len() - cursor.len(),
            field(bytes, 1)?,
            field(bytes, 2)?,
            field(bytes, 3)?,
            field(bytes, 4)?,
            data,
        ))
    };
    let Ok((
        size,
        Some(0),
        Some(raw),
        Some(compressed),
        None,
        Some((Some(count), Some(6), Some(3), Some(3))),
    )) = parse()
    else {
        return Ok(None);
    };
    if raw < 0
        || raw != compressed
        || count != rows as i64
        || size as u64 + raw as u64 != total_bytes
    {
        return Err(invalid("page header/index mismatch"));
    }
    Ok(Some(size))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slow_model() -> CostModel {
        CostModel::new(&json!({"latency_ms":220,"mbps":150,"request_overhead_bytes":0})).unwrap()
    }

    #[test]
    fn discovery_rejects_small_batches_before_fetching_lengths() {
        let model = slow_model();
        let small: Vec<_> = (0..8).map(|i| i * 1048576..(i + 1) * 1048576).collect();
        assert!(ProbeBudget::new(model, &small, 8).is_none());
        let large: Vec<_> = (0..32).map(|i| i * 4194304..(i + 1) * 4194304).collect();
        let mut budget = ProbeBudget::new(model, &large, 32).unwrap();
        assert!(budget.reserve(1, 32 * 8192, 32));
        assert!(budget.reserve(2, 32 * 8192, 32));
        let spent = budget.spent;
        assert!(!budget.reserve(3, 8192, 1));
        assert_eq!(budget.spent, spent);
        assert!(!budget.reserve(9, 1, 1));
    }

    #[test]
    fn sequential_probes_pay_separate_latency_and_request_capacity() {
        let model = slow_model();
        let ranges = vec![0..64 * 1048576, 64 * 1048576..128 * 1048576];
        let mut parallel = ProbeBudget::new(model, &ranges, 1).unwrap();
        assert!(parallel.reserve(0, 8192, 1));
        let mut sequential = ProbeBudget::new(model, &ranges, 1).unwrap();
        assert!(sequential.reserve(1, 8192, 1));
        assert!(sequential.spent > parallel.spent);
        let expensive = CostModel {
            request_overhead_bytes: 2000000,
            ..model
        };
        assert!(ProbeBudget::new(expensive, &ranges, 32).is_none());
    }

    #[test]
    fn cost_profile_requires_finite_usable_inputs() {
        for profile in [
            json!({}),
            json!({"latency_ms":220,"mbps":0,"request_overhead_bytes":0}),
            json!({"latency_ms":60001,"mbps":150,"request_overhead_bytes":0}),
        ] {
            assert!(CostModel::new(&profile).is_err());
        }
    }
}
