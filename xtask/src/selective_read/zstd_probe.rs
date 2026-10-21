use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    mem::MaybeUninit,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::Instant,
};

use clap::Args as ClapArgs;
use parquet::{
    basic::Compression,
    file::reader::{FileReader, SerializedFileReader},
};
use serde_json::{Value, json};

use super::{array, field, network, require, storage, text};
use crate::{Result, artifacts::reader_sha256, selective_read_build::digest};

#[derive(ClapArgs)]
pub(crate) struct Args {
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long)]
    state: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

fn number(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| "expected a nonnegative byte offset".into())
}

fn raw_headers(page: &Value, mut fetch: impl FnMut(u64, u64) -> Result<Vec<u8>>) -> Result<Value> {
    let size = number(&page["page_bytes"])?;
    require(
        (8192..=8 * 1024 * 1024).contains(&size),
        "invalid raw page size",
    )?;
    let prefix = fetch(0, 8192)?;
    require(prefix.len() == 8192, "short range response")?;
    let header_bytes = number(&page["zstd"]["page_header_bytes"])?;
    let frame = prefix
        .get(usize::try_from(header_bytes)?..)
        .ok_or("page header exceeds discovery prefix")?;
    require(
        frame.starts_with(b"\x28\xb5\x2f\xfd") && frame.len() >= 18,
        "invalid Zstd frame",
    )?;
    let descriptor = frame[4];
    require(
        descriptor & 15 == 0,
        "requires raw frames without dictionary or checksum",
    )?;
    let single = descriptor & 32 != 0;
    let size_bytes = [usize::from(single), 2, 4, 8][(descriptor >> 6) as usize];
    let size_start = 5 + usize::from(!single);
    let body_bytes = number(&page["body_bytes"])?;
    if size_bytes > 0 {
        let mut bytes = [0; 8];
        bytes[..size_bytes].copy_from_slice(&frame[size_start..size_start + size_bytes]);
        let decoded = u64::from_le_bytes(bytes);
        require(
            decoded + if size_bytes == 2 { 256 } else { 0 } == body_bytes,
            "frame content size differs",
        )?;
    }
    let mut offset = header_bytes + (size_start + size_bytes) as u64;
    let mut decoded = 0;
    let mut blocks = Vec::new();
    while blocks.len() < 128 {
        require(offset + 3 <= size, "truncated block header")?;
        let header = if offset + 3 <= prefix.len() as u64 {
            prefix[offset as usize..offset as usize + 3].to_vec()
        } else {
            fetch(offset, offset + 3)?
        };
        require(header.len() == 3, "short block header")?;
        let header = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let length = (header >> 3) as u64;
        require(
            (header >> 1) & 3 == 0 && length <= 131072,
            "not a bounded raw block",
        )?;
        require(offset + 3 + length <= size, "raw block exceeds page")?;
        blocks.push(json!({"header_offset":offset-header_bytes,"kind":"raw","stored_bytes":length,"decoded_bytes":length}));
        offset += 3 + length;
        decoded += length;
        require(decoded <= body_bytes, "decoded bytes exceed page body")?;
        if header & 1 != 0 {
            require(
                offset == size && decoded == body_bytes,
                "raw frame length differs",
            )?;
            return Ok(json!(blocks));
        }
    }
    Err("raw-block discovery exceeds 128 blocks".into())
}

fn inside(root: &Path, name: &Value) -> Result<PathBuf> {
    let path = fs::canonicalize(root.join(text(name)?))?;
    require(
        path.starts_with(root) && path.is_file(),
        "fixture path is outside its root",
    )?;
    Ok(path)
}

fn batches(fixture: &Path) -> Result<Value> {
    let fixture = fs::canonicalize(fixture)?;
    let manifest_path = fixture.join("manifest.json");
    let manifest = storage::read_json(&manifest_path)?;
    require(
        manifest["case"] == "long" && manifest["compression"] == "zstd",
        "expected long Zstd fixtures",
    )?;
    for item in array(&manifest["objects"])? {
        require(
            digest(&inside(&fixture, &item["path"])?)? == item["sha256"],
            "fixture object changed",
        )?;
    }
    let prefix = digest(&manifest_path)?;
    let mut groups = Vec::new();
    for layout in array(&manifest["layout"])? {
        let path = inside(&fixture, &layout["file"])?;
        let reader = SerializedFileReader::new(File::open(&path)?)?;
        let mut source = File::open(&path)?;
        for (group, metadata) in reader.metadata().row_groups().iter().enumerate() {
            let mut columns = Vec::new();
            let mut pages = Vec::new();
            for column in metadata.columns().iter().skip(3) {
                require(
                    column.dictionary_page_offset().is_none()
                        && matches!(column.compression(), Compression::ZSTD(_)),
                    "expected dictionary-free Zstd columns",
                )?;
                let start = u64::try_from(column.data_page_offset())?;
                let size = u64::try_from(column.compressed_size())?;
                let end = start
                    .checked_add(size)
                    .ok_or("column byte range overflow")?;
                require(end <= source.metadata()?.len(), "column exceeds its file")?;
                source.seek(SeekFrom::Start(start))?;
                columns.push(json!({"start":start,"end":end,"sha256":reader_sha256((&mut source).take(size))?}));
                let mut offset = start;
                for page in array(&layout["pages"])?.iter().filter(|page| {
                    page["row_group"] == group && page["column"] == column.column_path().string()
                }) {
                    if page["zstd"]["all_raw_without_checksum_or_dictionary"] == true {
                        let mut page = page.clone();
                        page["offset"] = json!(offset);
                        pages.push(page);
                    }
                    offset = offset
                        .checked_add(number(&page["page_bytes"])?)
                        .ok_or("page byte range overflow")?;
                }
                require(offset == end, "page layout differs from its column")?;
            }
            require(
                !pages.is_empty(),
                "row group has no eligible raw Zstd pages",
            )?;
            groups.push(json!({"key":format!("{prefix}/{}",path.file_name().ok_or("missing fixture filename")?.to_str().ok_or("non-UTF8 fixture filename")?),
                "row_group":group,"columns":columns,"raw_pages":pages}));
        }
    }
    require(!groups.is_empty(), "fixture has no row groups")?;
    Ok(json!(groups))
}

fn usage(who: libc::c_int) -> Result<libc::rusage> {
    let mut value = MaybeUninit::uninit();
    // SAFETY: getrusage initializes the writable structure on success.
    require(
        unsafe { libc::getrusage(who, value.as_mut_ptr()) } == 0,
        "getrusage failed",
    )?;
    // SAFETY: the successful call above initialized the structure.
    Ok(unsafe { value.assume_init() })
}

fn cpu_seconds() -> Result<f64> {
    let mut seconds = 0.0;
    for who in [libc::RUSAGE_SELF, libc::RUSAGE_CHILDREN] {
        let usage = usage(who)?;
        for value in [usage.ru_utime, usage.ru_stime] {
            seconds += value.tv_sec as f64 + value.tv_usec as f64 / 1e6;
        }
    }
    Ok(seconds)
}

fn read_group(directory: &Path, group: &Value, connection: &str, mode: &str) -> Result<()> {
    let items = array(
        &group[if mode == "complete" {
            "columns"
        } else {
            "raw_pages"
        }],
    )?;
    let path = format!("/{}/{}", storage::BUCKET, text(&group["key"])?);
    let next = AtomicUsize::new(0);
    thread::scope(|scope| -> Result<()> {
        let workers: Vec<_> = (0..32.min(items.len()))
            .map(|_| {
                scope.spawn(|| -> std::result::Result<(), String> {
                    (|| -> Result<()> {
                        while let Some(item) = items.get(next.fetch_add(1, Ordering::Relaxed)) {
                            let fetch = |start: u64, end: u64| -> Result<Vec<u8>> {
                                require(end > start, "empty range")?;
                                let body = storage::request(
                                    directory,
                                    &path,
                                    &[
                                        "--connect-to",
                                        connection,
                                        "--range",
                                        &format!("{start}-{}", end - 1),
                                    ],
                                )?;
                                require(body.len() as u64 == end - start, "short range response")?;
                                Ok(body)
                            };
                            if mode == "complete" {
                                require(
                                    reader_sha256(
                                        fetch(number(&item["start"])?, number(&item["end"])?)?
                                            .as_slice(),
                                    )? == item["sha256"],
                                    "column checksum differs",
                                )?;
                            } else {
                                let offset = number(&item["offset"])?;
                                require(
                                    raw_headers(item, |start, end| {
                                        fetch(offset + start, offset + end)
                                    })? == item["zstd"]["blocks"],
                                    "discovered raw blocks differ",
                                )?;
                            }
                        }
                        Ok(())
                    })()
                    .map_err(|error| error.to_string())
                })
            })
            .collect();
        for worker in workers {
            worker.join().map_err(|_| "discovery worker panicked")??;
        }
        Ok(())
    })
}

pub(super) fn run(args: Args) -> Result<()> {
    fs::create_dir(&args.output)?;
    let groups = batches(&args.fixture)?;
    let profile = network::config(&args.state)?.ok_or("a recorded network profile is required")?;
    let config = storage::state(&args.state)?;
    let upstream = url::Url::parse(storage::endpoint(&config)?)?;
    let proxy = url::Url::parse(storage::endpoint(&profile)?)?;
    let connection = format!(
        "{}:{}:{}:{}",
        upstream.host_str().ok_or("missing storage host")?,
        upstream.port().ok_or("missing storage port")?,
        proxy.host_str().ok_or("missing proxy host")?,
        proxy.port().ok_or("missing proxy port")?
    );
    storage::save(
        &args.output.join("experiment.json"),
        &json!({
            "fixture_sha256":digest(&args.fixture.join("manifest.json"))?,
            "groups":groups,"network":profile,
            "scope":"Optimistic discovery diagnostic, not queries. Offline inspection supplies raw-page eligibility and page-header lengths; block positions must be discovered over the network. Length/value reads and reconstruction are omitted.",
            "client":"Same curl SigV4 client in both modes; up to 32 worker threads; curl process/connection overhead is included.",
            "acceptance":"Three alternating pairs. If discovery alone consistently exceeds complete payload reads, do not implement this sequential strategy for these fixtures.",
            "memory":"Process RSS is a cumulative high-water mark, not a per-mode comparison."
        }),
    )?;
    let mut samples = Vec::new();
    let _lock = storage::exclusive(&args.state)?;
    storage::verify_server(&args.state)?;
    for repetition in 0..3 {
        for mode in if repetition % 2 == 0 {
            ["complete", "headers"]
        } else {
            ["headers", "complete"]
        } {
            let slot = args.output.join(format!("{repetition}-{mode}"));
            fs::create_dir(&slot)?;
            network::control(&args.state, true, true, 10.0)?;
            let cpu = cpu_seconds()?;
            let started = Instant::now();
            for group in array(&groups)? {
                read_group(&args.state, group, &connection, mode)?;
            }
            let elapsed = started.elapsed().as_secs_f64();
            let cpu = cpu_seconds()? - cpu;
            let capture = network::capture(&args.state, &slot)?;
            let sample = json!({"mode":mode,"repetition":repetition,"seconds":elapsed,"client_cpu_seconds":cpu,
                "process_peak_rss_kib":usage(libc::RUSAGE_SELF)?.ru_maxrss,"network":capture});
            storage::save(&slot.join("sample.json"), &sample)?;
            println!(
                "{mode} {repetition} {elapsed:.3} {} {}",
                field(&capture, "requests")?,
                field(&capture, "response_bytes")?
            );
            samples.push(sample);
        }
    }
    storage::save(&args.output.join("summary.json"), &json!(samples))
}

#[cfg(test)]
mod tests;
