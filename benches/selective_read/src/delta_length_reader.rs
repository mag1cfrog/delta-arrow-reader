//! Experimental transport for the generated #452 tables. All query paths share it.

use std::{
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream};
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, path::Path};
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
#[path = "../../../crates/delta-arrow-reader-core/src/reader/backend/direct_parquet/compact_thrift.rs"]
mod compact_thrift;
#[allow(dead_code)]
#[path = "../../../crates/delta-arrow-reader-core/src/reader/backend/direct_parquet/range_planning.rs"]
mod range_planning;
use super::delta_length;
use compact_thrift::{invalid_metadata, read_field, read_i64};
use range_planning::{
    TransportEstimate, choose_bounded_range_plan, merge_ranges, partial_plan_cost, range_bytes,
};

use cost::{CONCURRENCY, PREFIX_BYTES};

#[path = "delta_length_profile.rs"]
mod profile;
pub use profile::calibrate;
#[path = "delta_length_cost.rs"]
mod cost;

pub struct CostModel {
    transport: TransportEstimate,
    request_overhead_bytes: u128,
    request_batch_costs_ns: Option<Arc<[u64]>>,
    fallback_estimator: Option<Arc<cost::Estimator>>,
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
        let request_batch_costs_ns = profile
            .get("request_batch_costs_ns")
            .map(|value| {
                let costs: Vec<u64> = serde_json::from_value(value.clone())
                    .map_err(|_| invalid("request batch costs"))?;
                if costs.len() != 65
                    || costs[0] != 0
                    || costs[1] == 0
                    || costs.windows(2).any(|pair| pair[0] > pair[1])
                {
                    return Err(invalid("request batch costs must increase from zero"));
                }
                Ok(Arc::from(costs))
            })
            .transpose()?;
        let fallback_estimator = profile
            .get("request_durations_ns")
            .map(|value| {
                let durations = serde_json::from_value(value.clone())
                    .map_err(|_| invalid("request durations"))?;
                cost::Estimator::new(bandwidth * 125000, durations)
                    .ok_or_else(|| invalid("request durations outside experiment bounds"))
            })
            .transpose()?;
        Ok(Self {
            transport: TransportEstimate {
                request_latency: Duration::from_millis(latency),
                shared_throughput_bytes_per_second: bandwidth * 125000,
            },
            request_overhead_bytes: u128::from(number("request_overhead_bytes")?),
            request_batch_costs_ns,
            fallback_estimator,
        })
    }

    fn cost(&self, bytes: u128, requests: usize) -> Option<u128> {
        let capacity = match &self.request_batch_costs_ns {
            Some(costs) => {
                u128::from(*costs.get(requests)?)
                    * u128::from(self.transport.shared_throughput_bytes_per_second)
                    / 1_000_000_000
            }
            None => self.request_overhead_bytes.saturating_mul(requests as u128),
        };
        // Capacity already includes request waiting and overlaps payload delivery.
        Some(partial_plan_cost(bytes, requests, self.transport, CONCURRENCY, 0).max(capacity))
    }

    fn payload_ranges(
        &self,
        requested: &[Range<u64>],
        cached_bytes: usize,
        body_bytes: usize,
    ) -> Result<Option<Vec<Range<u64>>>> {
        let remaining = body_bytes
            .checked_sub(cached_bytes)
            .ok_or_else(|| invalid("cached prefix exceeds page body"))?
            as u128;
        let initial = choose_bounded_range_plan(
            requested,
            self.transport,
            CONCURRENCY,
            remaining,
            self.request_overhead_bytes,
        )
        .ok_or_else(|| invalid("payload plan exceeded remaining bytes"))?;
        let Some(byte_budget) = (body_bytes as u128 * 3 / 4).checked_sub(cached_bytes as u128)
        else {
            return Ok(None);
        };
        let ordinary = self.cost(remaining, 1);
        let saves = |plan: &Vec<Range<u64>>| {
            let bytes = range_bytes(plan);
            bytes <= byte_budget
                && self
                    .cost(bytes, plan.len())
                    .zip(ordinary)
                    .is_some_and(|(partial, ordinary)| {
                        partial.saturating_mul(100) < ordinary.saturating_mul(90)
                    })
        };
        if saves(&initial) {
            return Ok(Some(initial));
        }
        // The measured request curve may favor a plan the linear score discarded.
        Ok(
            choose_bounded_range_plan_by_cost(requested, byte_budget, |bytes, count| {
                self.cost(bytes, count)
            })
            .filter(saves),
        )
    }

    async fn probe_budget(
        &self,
        ranges: &[Range<u64>],
        parts: &[Vec<(Range<u64>, Option<Page>)>],
    ) -> Option<ProbeBudget<'_>> {
        let started = Instant::now();
        let pages = parts
            .iter()
            .flatten()
            .filter(|(_, page)| page.is_some())
            .count();
        if pages == 0 {
            return None;
        }
        let Some(estimator) = &self.fallback_estimator else {
            return ProbeBudget::new(self, ranges, pages, parts.iter().map(Vec::len).sum());
        };
        let input = modeled_ranges(ranges, parts)?;
        let allowance = self.cost(range_bytes(ranges), ranges.len())? / 20;
        let nanos = allowance.checked_mul(1_000_000_000)?
            / u128::from(self.transport.shared_throughput_bytes_per_second);
        let deadline = started.checked_add(Duration::from_nanos(nanos.try_into().ok()?))?;
        let extra_ns = estimator.estimate(input, deadline.into()).await?;
        ProbeBudget::from_estimate(self, ranges, pages, extra_ns, started.elapsed())
    }
}

// Keep the experimental capacity score out of production until #453.
/// Chooses among bounded plans using the caller's cost model.
///
/// The score must not decrease as bytes increase at a fixed request count.
/// Returning `None` excludes an unsupported request count from consideration.
fn choose_bounded_range_plan_by_cost(
    requested_ranges: &[Range<u64>],
    byte_budget: u128,
    cost: impl Fn(u128, usize) -> Option<u128>,
) -> Option<Vec<Range<u64>>> {
    let exact = merge_ranges(requested_ranges, 0);
    let exact_bytes = range_bytes(&exact);
    if exact_bytes > byte_budget {
        return None;
    }
    let mut gaps: Vec<_> = exact
        .windows(2)
        .enumerate()
        .map(|(index, ranges)| (ranges[1].start - ranges[0].end, index))
        .collect();
    gaps.sort_unstable();
    let mut bytes = exact_bytes;
    let mut best_cost = cost(bytes, exact.len());
    let mut merge_count = 0;
    for (merged, (gap, _)) in gaps.iter().enumerate() {
        bytes += u128::from(*gap);
        if bytes > byte_budget {
            break;
        }
        let Some(score) = cost(bytes, exact.len() - merged - 1) else {
            continue;
        };
        if best_cost.is_none_or(|best| score < best) {
            best_cost = Some(score);
            merge_count = merged + 1;
        }
    }
    // Ties keep the earlier, smaller plan. The caller applies the savings margin
    // against ordinary reads; a slower candidate must not hide a profitable one.
    best_cost.map(|_| merge_smallest_gaps(&exact, &gaps, merge_count))
}

/// Builds a plan by merging the first `merge_count` gaps in ascending size order.
fn merge_smallest_gaps(
    exact: &[Range<u64>],
    sorted_gaps: &[(u64, usize)],
    merge_count: usize,
) -> Vec<Range<u64>> {
    let Some(first) = exact.first() else {
        return Vec::new();
    };
    let mut merge = vec![false; exact.len() - 1];
    for (_, index) in sorted_gaps.iter().take(merge_count) {
        merge[*index] = true;
    }
    let mut plan = Vec::with_capacity(exact.len() - merge_count);
    let mut current = first.clone();
    for (index, next) in exact.iter().enumerate().skip(1) {
        if merge[index - 1] {
            current.end = next.end;
        } else {
            plan.push(current);
            current = next.clone();
        }
    }
    plan.push(current);
    plan
}

fn modeled_ranges(
    ranges: &[Range<u64>],
    parts: &[Vec<(Range<u64>, Option<Page>)>],
) -> Option<Vec<cost::ReadRange>> {
    if ranges.len() != parts.len() || ranges.len() > cost::MAX_RANGES {
        return None;
    }
    ranges
        .iter()
        .zip(parts)
        .map(|(range, parts)| {
            // The checked model has one prefix/tail dependency per outer request.
            let [(part, page)] = parts.as_slice() else {
                return None;
            };
            (part == range).then_some(cost::ReadRange {
                bytes: range.end.checked_sub(range.start)?,
                split: page.is_some(),
            })
        })
        .collect()
}

// Bound the modeled work lost if lengths reveal that no payload can be skipped.
struct ProbeBudget<'a> {
    model: &'a CostModel,
    limit: u128,
    spent: u128,
    remaining_bytes: u128,
    // None means the initial numerical estimate already includes complete fallback.
    fallback_requests: Option<usize>,
    rounds: [(u128, usize); 9],
}

impl<'a> ProbeBudget<'a> {
    fn new(
        model: &'a CostModel,
        ordinary: &[Range<u64>],
        pages: usize,
        fallback_requests: usize,
    ) -> Option<Self> {
        let bytes = range_bytes(ordinary);
        let ordinary_cost = model.cost(bytes, ordinary.len())?;
        let mut budget = Self {
            model,
            limit: ordinary_cost + ordinary_cost / 20,
            spent: 0,
            remaining_bytes: bytes,
            fallback_requests: Some(fallback_requests),
            rounds: [(0, 0); 9],
        };
        budget
            .reserve(0, u128::from(PREFIX_BYTES) * pages as u128, pages)
            .then_some(budget)
    }

    fn from_estimate(
        model: &'a CostModel,
        ordinary: &[Range<u64>],
        pages: usize,
        extra_ns: f64,
        elapsed: Duration,
    ) -> Option<Self> {
        if !extra_ns.is_finite() || extra_ns < 0.0 || extra_ns >= u128::MAX as f64 {
            return None;
        }
        let bytes = range_bytes(ordinary);
        let ordinary_cost = model.cost(bytes, ordinary.len())?;
        let nanos = (extra_ns.ceil() as u128).checked_add(elapsed.as_nanos())?;
        let extra_cost = nanos
            .checked_mul(u128::from(
                model.transport.shared_throughput_bytes_per_second,
            ))?
            .div_ceil(1_000_000_000);
        if extra_cost > ordinary_cost / 20 {
            return None;
        }
        Some(Self {
            model,
            limit: ordinary_cost + ordinary_cost / 20,
            spent: ordinary_cost + extra_cost,
            remaining_bytes: bytes.checked_sub(u128::from(PREFIX_BYTES) * pages as u128)?,
            fallback_requests: None,
            rounds: [(0, 0); 9],
        })
    }

    fn reserve(&mut self, round: usize, bytes: u128, requests: usize) -> bool {
        if self.fallback_requests.is_none() && round == 0 {
            return false;
        }
        let Some(&(previous_bytes, previous_requests)) = self.rounds.get(round) else {
            return false;
        };
        let next = (previous_bytes + bytes, previous_requests + requests);
        let Some(next_cost) = self.model.cost(next.0, next.1) else {
            return false;
        };
        let previous_cost = self
            .model
            .cost(previous_bytes, previous_requests)
            .expect("previously reserved request count");
        let spent = self.spent - previous_cost + next_cost;
        let Some(remaining_bytes) = self.remaining_bytes.checked_sub(bytes) else {
            return false;
        };
        let fallback_cost = match self.fallback_requests {
            Some(requests) => match self.model.cost(remaining_bytes, requests) {
                Some(cost) => cost,
                None => return false,
            },
            // Keep the initial full-tail charge, even when extra probes reuse more bytes.
            None => 0,
        };
        // Prefix bytes are reused, but each discovery round precedes the remaining reads.
        if spent + fallback_cost > self.limit {
            return false;
        }
        self.rounds[round] = next;
        self.spent = spent;
        self.remaining_bytes = remaining_bytes;
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
    cost_decisions: AtomicU64,
    cost_decision_ns: AtomicU64,
}

impl Metrics {
    pub fn snapshot(&self) -> Value {
        let get = |v: &AtomicU64| v.load(Ordering::Relaxed);
        json!({"data_requests":get(&self.requests),"data_bytes":get(&self.bytes),
            "discovery_requests":get(&self.discovery_requests),"partial_pages":get(&self.partial_pages),
            "rejected_pages":get(&self.rejected_pages),"fallback_pages":get(&self.fallback_pages),
            "max_discovery_rounds":get(&self.max_discovery_rounds),"length_and_null_bytes":get(&self.length_and_null_bytes),
            "selected_payload_bytes":get(&self.selected_payload_bytes),"logical_prefix_chunks":get(&self.logical_prefix_chunks),
            "cost_rejected_pages":get(&self.cost_rejected_pages),"probe_budget_exhausted":get(&self.probe_budget_exhausted),
            "cost_decisions":get(&self.cost_decisions),"cost_decision_ns":get(&self.cost_decision_ns)})
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

    async fn read_into(&self, path: &Path, range: Range<u64>, output: &mut [u8]) -> Result<()> {
        if range.end.checked_sub(range.start) != Some(output.len() as u64) {
            return Err(invalid("response destination has the wrong length"));
        }
        let _permit = self.permits.acquire().await.unwrap();
        let response = self
            .store
            .get_opts(path, GetOptions::new().with_range(Some(range.clone())))
            .await?;
        if response.range != range {
            return Err(invalid("object-store response range mismatch"));
        }
        let size = output.len();
        let mut remaining = output;
        let mut chunks = response.into_stream();
        while let Some(chunk) = chunks.try_next().await? {
            let (destination, rest) = remaining
                .split_at_mut_checked(chunk.len())
                .ok_or_else(|| invalid("oversized object-store response"))?;
            destination.copy_from_slice(&chunk);
            remaining = rest;
        }
        if !remaining.is_empty() {
            return Err(invalid("short object-store response"));
        }
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        self.metrics.bytes.fetch_add(size as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn read_tail(
        &self,
        path: &Path,
        range: Range<u64>,
        mut prefix: Vec<u8>,
    ) -> Result<Bytes> {
        let _permit = self.permits.acquire().await.unwrap();
        let size = (range.end - range.start) as usize;
        let response = self
            .store
            .get_opts(path, GetOptions::new().with_range(Some(range.clone())))
            .await?;
        if response.range != range {
            return Err(invalid("object-store response range mismatch"));
        }
        // Append the response directly to avoid buffering and copying a second full tail.
        prefix.reserve_exact(size);
        let mut remaining = size;
        let mut chunks = response.into_stream();
        while let Some(chunk) = chunks.try_next().await? {
            remaining = remaining
                .checked_sub(chunk.len())
                .ok_or_else(|| invalid("oversized object-store response"))?;
            prefix.extend_from_slice(&chunk);
        }
        if remaining != 0 {
            return Err(invalid("short object-store response"));
        }
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        self.metrics.bytes.fetch_add(size as u64, Ordering::Relaxed);
        Ok(prefix.into())
    }
}

#[derive(Clone)]
struct Page {
    range: Range<u64>,
    rows: usize,
    nullable: bool,
    selected: Vec<usize>,
}

fn range_parts(range: Range<u64>, candidates: &[Page]) -> Vec<(Range<u64>, Option<Page>)> {
    let pages: Vec<_> = candidates
        .iter()
        .filter(|page| page.range.start < range.end && page.range.end > range.start)
        .cloned()
        .collect();
    if pages.is_empty()
        || pages
            .iter()
            .any(|page| page.range.start < range.start || page.range.end > range.end)
    {
        return vec![(range, None)];
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
    parts
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
        let parts: Vec<_> = ranges
            .iter()
            .map(|range| range_parts(range.clone(), &self.pages))
            .collect();
        let pages = parts
            .iter()
            .flatten()
            .filter(|(_, page)| page.is_some())
            .count();
        let budget = match self.io.cost_model.as_ref().filter(|_| pages != 0) {
            Some(model) => {
                let started = Instant::now();
                let budget = model.probe_budget(&ranges, &parts).await.map(Mutex::new);
                self.io
                    .metrics
                    .cost_decisions
                    .fetch_add(1, Ordering::Relaxed);
                self.io.metrics.cost_decision_ns.fetch_add(
                    started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                budget
            }
            None => None,
        };
        if pages != 0 && budget.is_none() {
            self.io
                .metrics
                .cost_rejected_pages
                .fetch_add(pages as u64, Ordering::Relaxed);
        }
        stream::iter(ranges.into_iter().zip(parts))
            .map(|(range, parts)| async {
                if let Some(budget) = &budget {
                    self.range(range, parts, budget).await
                } else {
                    self.io.read(&self.path, range, false).await
                }
            })
            .buffered(CONCURRENCY)
            .try_collect()
            .await
    }

    async fn range(
        &self,
        range: Range<u64>,
        parts: Vec<(Range<u64>, Option<Page>)>,
        budget: &Mutex<ProbeBudget<'_>>,
    ) -> Result<Bytes> {
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
        if let [piece] = pieces.as_slice() {
            return Ok(piece.clone());
        }
        let mut output = Vec::with_capacity((range.end - range.start) as usize);
        for bytes in pieces {
            output.extend_from_slice(&bytes);
        }
        Ok(output.into())
    }

    async fn page(&self, page: &Page, budget: &Mutex<ProbeBudget<'_>>) -> Result<Bytes> {
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
            prefix[header..].to_vec(),
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
        let model = self
            .io
            .cost_model
            .as_ref()
            .expect("budget requires a cost model");
        let ranges: Vec<_> = ranges
            .iter()
            .map(|range| range.start as u64..range.end as u64)
            .collect();
        // A few very long selected values can defeat sparse row selection.
        let Some(ranges) = model.payload_ranges(&ranges, cached.len(), body_size)? else {
            self.io
                .metrics
                .fallback_pages
                .fetch_add(1, Ordering::Relaxed);
            let mut bytes = prefix[..header].to_vec();
            bytes.extend_from_slice(&cached);
            return self.finish_page(page, bytes).await;
        };
        let mut output = vec![0; (page.range.end - page.range.start) as usize];
        output[..header].copy_from_slice(&prefix[..header]);
        output[header..header + cached.len()].copy_from_slice(&cached);
        // Fill disjoint destinations directly, without collecting another payload copy.
        let mut remaining = &mut output[header..];
        let mut cursor = 0;
        let mut reads = Vec::with_capacity(ranges.len());
        for range in ranges {
            let start =
                usize::try_from(range.start).map_err(|_| invalid("payload range outside page"))?;
            let end =
                usize::try_from(range.end).map_err(|_| invalid("payload range outside page"))?;
            let gap = start
                .checked_sub(cursor)
                .ok_or_else(|| invalid("overlapping payload ranges"))?;
            let size = end
                .checked_sub(start)
                .ok_or_else(|| invalid("reversed payload range"))?;
            let (_, after_gap) = remaining
                .split_at_mut_checked(gap)
                .ok_or_else(|| invalid("payload range outside page"))?;
            let (destination, rest) = after_gap
                .split_at_mut_checked(size)
                .ok_or_else(|| invalid("payload range outside page"))?;
            remaining = rest;
            cursor = end;
            reads.push(self.io.read_into(
                &self.path,
                body_start + range.start..body_start + range.end,
                destination,
            ));
        }
        stream::iter(reads)
            .buffered(CONCURRENCY)
            .try_collect::<()>()
            .await?;
        self.io
            .metrics
            .partial_pages
            .fetch_add(1, Ordering::Relaxed);
        Ok(output.into())
    }

    async fn finish_page(&self, page: &Page, prefix: Vec<u8>) -> Result<Bytes> {
        let start = page.range.start + prefix.len() as u64;
        if start < page.range.end {
            return self
                .io
                .read_tail(&self.path, start..page.range.end, prefix)
                .await;
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

    #[tokio::test]
    async fn payload_reads_fill_the_destination_and_release_permits() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("payload");
        store
            .put(&path, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        let io = Io::new(
            Arc::new(object_store::chunked::ChunkedStore::new(store, 3)),
            None,
        );
        let mut output = [b'_'; 12];
        io.read_into(&path, 4..12, &mut output[2..10])
            .await
            .unwrap();
        assert_eq!(&output, b"__456789ab__");
        assert_eq!(io.metrics.snapshot()["data_bytes"], 8);
        assert_eq!(io.metrics.snapshot()["data_requests"], 1);
        assert!(io.read_into(&path, 0..8, &mut output[..7]).await.is_err());
        assert!(io.read_into(&path, 12..20, &mut output[..8]).await.is_err());
        assert!(
            io.read_into(&Path::from("missing"), 0..8, &mut output[..8])
                .await
                .is_err()
        );
        assert_eq!(io.permits.available_permits(), CONCURRENCY);
        assert_eq!(io.metrics.snapshot()["data_requests"], 1);
        assert_eq!(io.metrics.snapshot()["data_bytes"], 8);
    }

    #[test]
    fn bounded_costs_skip_unsupported_counts_and_keep_byte_limits() {
        let ranges = [0..10, 20..30, 40..50];
        let score = |bytes, count| (count <= 2).then_some(bytes);
        assert_eq!(
            super::choose_bounded_range_plan_by_cost(&ranges, 50, score),
            Some(vec![0..30, 40..50])
        );
        assert!(super::choose_bounded_range_plan_by_cost(&ranges, 39, score).is_none());
        assert!(super::choose_bounded_range_plan_by_cost(&ranges, 50, |_, _| None).is_none());
        assert_eq!(
            super::choose_bounded_range_plan_by_cost(&ranges, 50, |_, count| (count <= 2)
                .then_some(1)),
            Some(vec![0..30, 40..50])
        );
        assert_eq!(
            super::choose_bounded_range_plan_by_cost(&[], 0, score),
            Some(vec![])
        );
    }

    #[test]
    fn payload_plan_recovers_a_profitable_merge_and_preserves_accepted_plans() {
        let mut curve = vec![1_500_000_000; 65];
        curve[..4].copy_from_slice(&[0, 1_000_000_000, 1_300_000_000, 1_400_000_000]);
        let mut model = CostModel {
            transport: TransportEstimate {
                request_latency: Duration::from_secs(1),
                shared_throughput_bytes_per_second: 1_000,
            },
            request_overhead_bytes: 30,
            request_batch_costs_ns: Some(curve.into()),
            fallback_estimator: None,
        };
        let ranges = [0..10, 60..70, 120..130, 180..190];
        let initial =
            choose_bounded_range_plan(&ranges, model.transport, CONCURRENCY, 400, 30).unwrap();
        assert_eq!(initial, [0..70, 120..130, 180..190]);
        assert_eq!(
            model.cost(range_bytes(&initial), initial.len()),
            model.cost(400, 1)
        );
        assert_eq!(
            model.payload_ranges(&ranges, 0, 400).unwrap(),
            Some(std::iter::once(0..190).collect())
        );
        assert!(
            model
                .payload_ranges(std::slice::from_ref(&(0..350)), 0, 400)
                .unwrap()
                .is_none()
        );
        assert!(
            model
                .payload_ranges(std::slice::from_ref(&(0..401)), 0, 400)
                .is_err()
        );
        assert!(
            model
                .payload_ranges(std::slice::from_ref(&(310..320)), 310, 400)
                .unwrap()
                .is_none()
        );
        assert!(model.payload_ranges(&[], 401, 400).is_err());

        let mut curve = vec![1_300_000_000; 65];
        curve[..4].copy_from_slice(&[0, 1_000_000_000, 1_050_000_000, 1_150_000_000]);
        model.request_batch_costs_ns = Some(curve.into());
        // An already profitable plan retains its smaller byte footprint.
        assert_eq!(
            model.payload_ranges(&ranges, 0, 400).unwrap(),
            Some(initial)
        );
    }

    #[tokio::test]
    async fn fallback_keeps_the_prefix_and_reads_only_the_tail() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("page");
        store
            .put(&path, Bytes::from_static(b"headerpayload").into())
            .await
            .unwrap();
        let io = Io::new(
            Arc::new(object_store::chunked::ChunkedStore::new(store, 2)),
            None,
        );
        let bytes = io
            .read_tail(&path, 6..13, b"header".to_vec())
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"headerpayload");
        let metrics = io.metrics.snapshot();
        assert_eq!(metrics["data_bytes"], 7);
        assert_eq!(metrics["data_requests"], 1);
        assert_eq!(metrics["discovery_requests"], 0);
        assert_eq!(io.permits.available_permits(), CONCURRENCY);
    }

    #[tokio::test]
    async fn failed_fallback_does_not_return_partial_data_or_keep_a_permit() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("page");
        store
            .put(&path, Bytes::from_static(b"headerpayload").into())
            .await
            .unwrap();
        let io = Io::new(store, None);
        for (path, range) in [(path, 6..14), (Path::from("missing"), 6..13)] {
            assert!(
                io.read_tail(&path, range, b"header".to_vec())
                    .await
                    .is_err()
            );
            assert_eq!(io.permits.available_permits(), CONCURRENCY);
        }
        assert_eq!(io.metrics.snapshot()["data_requests"], 0);
        assert_eq!(io.metrics.snapshot()["data_bytes"], 0);
    }

    #[tokio::test]
    async fn cancelling_a_streamed_response_releases_its_permit() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};

        let store = object_store::memory::InMemory::new();
        let path = Path::from("page");
        store
            .put(&path, Bytes::from_static(b"headerpayload").into())
            .await
            .unwrap();
        let store = ThrottledStore::new(
            store,
            ThrottleConfig {
                wait_get_per_byte: Duration::from_secs(60),
                ..Default::default()
            },
        );
        let io = Io::new(Arc::new(store), None);
        let mut read = Box::pin(io.read_tail(&path, 6..13, b"header".to_vec()));
        assert!(read.as_mut().now_or_never().is_none());
        assert_eq!(io.permits.available_permits(), CONCURRENCY - 1);
        drop(read);
        assert_eq!(io.permits.available_permits(), CONCURRENCY);
        assert_eq!(io.metrics.snapshot()["data_requests"], 0);
        assert_eq!(io.metrics.snapshot()["data_bytes"], 0);

        let mut output = [0; 7];
        let mut read = Box::pin(io.read_into(&path, 6..13, &mut output));
        assert!(read.as_mut().now_or_never().is_none());
        assert_eq!(io.permits.available_permits(), CONCURRENCY - 1);
        drop(read);
        assert_eq!(io.permits.available_permits(), CONCURRENCY);
        assert_eq!(io.metrics.snapshot()["data_requests"], 0);
        assert_eq!(io.metrics.snapshot()["data_bytes"], 0);
    }

    fn slow_model() -> CostModel {
        CostModel::new(&json!({"latency_ms":220,"mbps":150,"request_overhead_bytes":0})).unwrap()
    }

    #[test]
    fn numerical_budget_charges_response_and_keeps_full_fallback_reserved() {
        let model = CostModel::new(&json!({
            "latency_ms":0,"mbps":8,"request_overhead_bytes":0
        }))
        .unwrap();
        let ordinary = std::iter::once(0..1_000_000).collect::<Vec<_>>();
        // One second of ordinary work allows 50 ms extra, including decision time.
        let mut budget = ProbeBudget::from_estimate(
            &model,
            &ordinary,
            1,
            20_000_000.0,
            Duration::from_millis(10),
        )
        .unwrap();
        assert_eq!(budget.spent, 1_030_000);
        assert_eq!(budget.remaining_bytes, 1_000_000 - u128::from(PREFIX_BYTES));
        assert!(budget.reserve(1, 10000, 1));
        let before = (budget.spent, budget.remaining_bytes, budget.rounds);
        assert!(!budget.reserve(2, 11000, 1));
        assert!(!budget.reserve(0, 1, 1));
        assert_eq!(
            (budget.spent, budget.remaining_bytes, budget.rounds),
            before
        );
        assert!(
            ProbeBudget::from_estimate(
                &model,
                &ordinary,
                1,
                20_000_000.0,
                Duration::from_millis(31)
            )
            .is_none()
        );
        for cost in [f64::NAN, f64::INFINITY, -1.0] {
            assert!(
                ProbeBudget::from_estimate(&model, &ordinary, 1, cost, Duration::ZERO).is_none()
            );
        }
    }

    #[test]
    fn modeled_ranges_preserve_order_and_decline_multi_page_requests() {
        let pages: Vec<_> = [0..10000, 20000..30000]
            .into_iter()
            .map(|range| Page {
                range,
                rows: 1,
                nullable: false,
                selected: vec![0],
            })
            .collect();
        let ranges = vec![20000..30000, 40000..40004, 0..10000];
        let parts: Vec<_> = ranges
            .iter()
            .map(|range| range_parts(range.clone(), &pages))
            .collect();
        assert_eq!(
            modeled_ranges(&ranges, &parts),
            Some(vec![
                cost::ReadRange {
                    bytes: 10000,
                    split: true
                },
                cost::ReadRange {
                    bytes: 4,
                    split: false
                },
                cost::ReadRange {
                    bytes: 10000,
                    split: true
                },
            ])
        );
        assert!(modeled_ranges(&ranges, &parts[..2]).is_none());
        let merged = std::iter::once(0..30000).collect::<Vec<_>>();
        assert!(modeled_ranges(&merged, &[range_parts(0..30000, &pages)]).is_none());
    }

    #[tokio::test]
    async fn measured_profile_uses_numeric_admission_and_fixed_profile_keeps_its_gate() {
        let mut profile = json!({"latency_ms":200,"mbps":150,"request_overhead_bytes":0});
        let fixed = CostModel::new(&profile).unwrap();
        profile["request_durations_ns"] = json!(vec![200_000_000_u64; 192]);
        let measured = CostModel::new(&profile).unwrap();
        let ranges: Vec<_> = (0..32).map(|i| i * 8388608..(i + 1) * 8388608).collect();
        let pages: Vec<_> = ranges
            .iter()
            .map(|range| Page {
                range: range.clone(),
                rows: 1,
                nullable: false,
                selected: vec![0],
            })
            .collect();
        let parts: Vec<_> = ranges
            .iter()
            .map(|range| range_parts(range.clone(), &pages))
            .collect();
        assert!(
            fixed
                .probe_budget(&ranges, &parts)
                .await
                .unwrap()
                .fallback_requests
                .is_some()
        );
        let numeric = measured.probe_budget(&ranges, &parts).await.unwrap();
        assert!(numeric.fallback_requests.is_none());
        assert!(numeric.spent > measured.cost(range_bytes(&ranges), ranges.len()).unwrap());
        let merged = std::iter::once(0..32 * 8388608).collect::<Vec<_>>();
        assert!(
            measured
                .probe_budget(&merged, &[range_parts(merged[0].clone(), &pages)])
                .await
                .is_none()
        );
    }

    #[test]
    fn request_capacity_includes_waiting_and_overlaps_transfer() {
        let model = CostModel::new(&json!({
            "latency_ms": 200, "mbps": 8, "request_overhead_bytes": 200000 / 31
        }))
        .unwrap();
        assert_eq!(model.cost(0, 0), Some(0));
        assert_eq!(model.cost(0, 32), Some(206432));
        assert_eq!(model.cost(10000000, 32), Some(10200000));
    }

    #[test]
    fn unmodeled_request_counts_decline_discovery_without_spending() {
        let costs: Vec<_> = (0..=64_u64).map(|n| n.div_ceil(32) * 200_000_000).collect();
        let model = CostModel::new(&json!({
            "latency_ms": 200, "mbps": 8, "request_overhead_bytes": 0,
            "request_batch_costs_ns": costs
        }))
        .unwrap();
        assert_eq!(model.cost(0, 64), Some(400000));
        assert_eq!(model.cost(0, 65), None);
        let many: Vec<_> = (0..65).map(|i| i * 8388608..(i + 1) * 8388608).collect();
        assert!(ProbeBudget::new(&model, &many, 32, 65).is_none());
        let mut budget = ProbeBudget::new(&model, &many[..32], 32, 32).unwrap();
        let saved = (budget.spent, budget.remaining_bytes);
        assert!(!budget.reserve(0, 33 * 8192, 33));
        assert_eq!((budget.spent, budget.remaining_bytes), saved);
        let mut descending = costs;
        descending[32] = 1;
        assert!(
            CostModel::new(&json!({
                "latency_ms": 200, "mbps": 8, "request_overhead_bytes": 0,
                "request_batch_costs_ns": descending
            }))
            .is_err()
        );
    }

    #[test]
    fn fallback_limit_rejects_unaffordable_probes_and_keeps_large_reads() {
        let small: Vec<_> = (0..32).map(|i| i * 2097152..(i + 1) * 2097152).collect();
        let large: Vec<_> = (0..32).map(|i| i * 4194304..(i + 1) * 4194304).collect();
        for profile in [
            json!({"latency_ms":9,"mbps":5519,"request_overhead_bytes":208746}),
            json!({"latency_ms":200,"mbps":150,"request_overhead_bytes":122671}),
        ] {
            let model = CostModel::new(&profile).unwrap();
            assert!(ProbeBudget::new(&model, &small, 32, 32).is_none());
            assert!(ProbeBudget::new(&model, &large, 32, 32).is_some());
        }
    }

    #[test]
    fn discovery_rejects_small_batches_before_fetching_lengths() {
        let model = slow_model();
        let small: Vec<_> = (0..8).map(|i| i * 1048576..(i + 1) * 1048576).collect();
        assert!(ProbeBudget::new(&model, &small, 8, 8).is_none());
        let large: Vec<_> = (0..32).map(|i| i * 8388608..(i + 1) * 8388608).collect();
        let mut budget = ProbeBudget::new(&model, &large, 32, 32).unwrap();
        assert!(budget.reserve(1, 32 * 8192, 32));
        assert!(budget.reserve(2, 32 * 8192, 32));
        let spent = budget.spent;
        let remaining = budget.remaining_bytes;
        assert!(!budget.reserve(3, 8192, 1));
        assert_eq!(budget.spent, spent);
        assert_eq!(budget.remaining_bytes, remaining);
        assert!(!budget.reserve(9, 1, 1));
    }

    #[test]
    fn sequential_probes_pay_separate_latency_and_request_capacity() {
        let model = slow_model();
        let ranges = vec![0..128 * 1048576, 128 * 1048576..256 * 1048576];
        let mut parallel = ProbeBudget::new(&model, &ranges, 1, 2).unwrap();
        assert!(parallel.reserve(0, 8192, 1));
        let mut sequential = ProbeBudget::new(&model, &ranges, 1, 2).unwrap();
        assert!(sequential.reserve(1, 8192, 1));
        assert!(sequential.spent > parallel.spent);
        let expensive = CostModel {
            request_overhead_bytes: 2000000,
            ..model
        };
        assert!(ProbeBudget::new(&expensive, &ranges, 32, 32).is_none());
    }

    #[test]
    fn fallback_budget_reuses_bytes_and_counts_split_requests() {
        let model = CostModel::new(&json!({
            "latency_ms": 0, "mbps": 1000, "request_overhead_bytes": 0
        }))
        .unwrap();
        let ranges = std::iter::once(0..16384).collect::<Vec<_>>();
        let mut budget = ProbeBudget::new(&model, &ranges, 1, 1).unwrap();
        assert!(budget.reserve(1, 8192, 1));
        assert_eq!(budget.remaining_bytes, 0);
        assert!(!budget.reserve(2, 1, 1));

        let model = CostModel {
            request_overhead_bytes: 100000,
            ..model
        };
        let ranges: Vec<_> = (0..64).map(|i| i * 65536..(i + 1) * 65536).collect();
        assert!(ProbeBudget::new(&model, &ranges, 2, 64).is_some());
        assert!(ProbeBudget::new(&model, &ranges, 2, 68).is_none());
    }

    #[test]
    fn range_parts_preserve_gaps_and_keep_partial_pages_ordinary() {
        let pages: Vec<_> = [10..20, 20..30, 40..50]
            .into_iter()
            .map(|range| Page {
                range,
                rows: 1,
                nullable: false,
                selected: vec![0],
            })
            .collect();
        let parts = range_parts(5..55, &pages);
        assert_eq!(
            parts
                .iter()
                .map(|(range, _)| range.clone())
                .collect::<Vec<_>>(),
            [5..10, 10..20, 20..30, 30..40, 40..50, 50..55]
        );
        assert_eq!(parts.iter().filter(|(_, page)| page.is_some()).count(), 3);
        let parts = range_parts(15..55, &pages);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, 15..55);
        assert!(parts[0].1.is_none());
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
