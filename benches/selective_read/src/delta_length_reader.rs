//! Experimental transport for the generated #452 tables. All query paths share it.

use std::{
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
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
use super::delta_length;
use compact_thrift::{invalid_metadata, read_field, read_i64};

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
}

impl Metrics {
    pub fn snapshot(&self) -> Value {
        let get = |v: &AtomicU64| v.load(Ordering::Relaxed);
        json!({"data_requests":get(&self.requests),"data_bytes":get(&self.bytes),
            "discovery_requests":get(&self.discovery_requests),"partial_pages":get(&self.partial_pages),
            "rejected_pages":get(&self.rejected_pages),"fallback_pages":get(&self.fallback_pages),
            "max_discovery_rounds":get(&self.max_discovery_rounds),"length_and_null_bytes":get(&self.length_and_null_bytes),
            "selected_payload_bytes":get(&self.selected_payload_bytes),"logical_prefix_chunks":get(&self.logical_prefix_chunks)})
    }
}

pub struct Io {
    pub store: Arc<dyn ObjectStore>,
    pub metrics: Metrics,
    permits: Semaphore,
}

impl Io {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            metrics: Metrics::default(),
            permits: Semaphore::new(32),
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
        if experimental {
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

    async fn range(&self, range: Range<u64>) -> Result<Bytes> {
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
                        Some(page) => reader.page(&page).await,
                        None => reader.io.read(&reader.path, range, false).await,
                    }
                }
            })
            .buffered(32)
            .try_collect()
            .await?;
        let mut output = Vec::with_capacity((range.end - range.start) as usize);
        for bytes in pieces {
            output.extend_from_slice(&bytes);
        }
        Ok(output.into())
    }

    async fn page(&self, page: &Page) -> Result<Bytes> {
        let prefix = self
            .io
            .read(&self.path, page.range.start..page.range.start + 8192, true)
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
        let remaining_bytes: usize = ranges.iter().map(|range| range.len()).sum();
        // A few very long selected values can defeat sparse row selection.
        if cached.len() + remaining_bytes > body_size * 3 / 4 {
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
                        body_start + range.start as u64..body_start + range.end as u64,
                        false,
                    )
                    .await?;
                Ok::<_, ParquetError>((range, bytes))
            })
            .buffered(32)
            .try_collect()
            .await?;
        for (range, bytes) in pieces {
            output[header + range.start..header + range.end].copy_from_slice(&bytes);
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
        self.range(range).boxed()
    }
    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            stream::iter(ranges)
                .map(|range| self.range(range))
                .buffered(32)
                .try_collect()
                .await
        }
        .boxed()
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
