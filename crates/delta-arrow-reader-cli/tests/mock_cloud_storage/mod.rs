//! Mock S3, Azure Blob, and GCS responses shared by direct and Unity Catalog CLI tests.

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use arrow::{
    compute::concat_batches,
    ipc::reader::FileReader,
    record_batch::{RecordBatch, RecordBatchReader},
};
use serde_json::{Value, json};
use url::Url;

/// Selects the listing XML and object URL layout used by the mock storage service.
#[derive(Clone, Copy)]
pub(super) enum CloudStorageProtocol {
    S3,
    AzureBlob,
    Gcs,
}

/// Decodes URL escapes before fixtures inspect an object path.
pub(super) fn requested_object_path(request: &str) -> String {
    let target = request.split_whitespace().nth(1).unwrap();
    let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
    object_store::path::Path::from_url_path(url.path())
        .unwrap()
        .to_string()
}

/// A local S3/Azure Blob/GCS server for log listings and ranged object reads.
pub(super) struct MockCloudStorageServer {
    pub(super) endpoint: String,
    pub(super) requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    deny_requests: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockCloudStorageServer {
    pub(super) fn deny_all_requests(&self) {
        self.deny_requests.store(true, Ordering::Relaxed);
    }

    pub(super) fn new(protocol: CloudStorageProtocol, log: Vec<u8>) -> std::io::Result<Self> {
        Self::with_request_hook(protocol, log, |_| {})
    }

    pub(super) fn with_request_hook(
        protocol: CloudStorageProtocol,
        log: Vec<u8>,
        mut on_request: impl FnMut(&str) + Send + 'static,
    ) -> std::io::Result<Self> {
        Self::with_objects(
            protocol,
            BTreeMap::from([("table/_delta_log/00000000000000000000.json".into(), log)]),
            move |request, _| on_request(request),
        )
    }

    pub(super) fn with_objects(
        protocol: CloudStorageProtocol,
        mut objects: BTreeMap<String, Vec<u8>>,
        mut on_request: impl FnMut(&str, &mut BTreeMap<String, Vec<u8>>) + Send + 'static,
    ) -> std::io::Result<Self> {
        let is_azure_blob = matches!(protocol, CloudStorageProtocol::AzureBlob);
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let deny_requests = Arc::new(AtomicBool::new(false));
        let denied = Arc::clone(&deny_requests);
        let thread = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                let Ok((mut socket, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while request.len() < 16 * 1024 && !request.ends_with(b"\r\n\r\n") {
                    if socket.read_exact(&mut byte).is_err() {
                        break;
                    }
                    request.push(byte[0]);
                }
                let request = String::from_utf8(request).unwrap();
                recorded.lock().unwrap().push(request.clone());
                on_request(&request, &mut objects);
                let Some(target) = request.split_whitespace().nth(1) else {
                    continue;
                };
                let url = Url::parse(&format!("http://localhost{target}")).unwrap();
                // GCS encodes slashes in object keys, unlike the S3 and Azure Blob clients.
                let request_path = object_store::path::Path::from_url_path(url.path()).unwrap();
                let query = url.query_pairs().collect::<HashMap<_, _>>();
                let listing = query.contains_key("list-type")
                    || query.get("comp").is_some_and(|v| v == "list");
                let (mut status, mut body) = if denied.load(Ordering::Relaxed) {
                    (403, b"<Error><Code>AccessDenied</Code><Message>secret-denied-message</Message></Error>".to_vec())
                } else if listing {
                    let mut entries = String::new();
                    for (key, bytes) in &objects {
                        if !key.starts_with(query.get("prefix").map_or("", |v| v.as_ref()))
                            || key.as_str() <= query.get("start-after").map_or("", |v| v.as_ref())
                        {
                            continue;
                        }
                        let entry = if is_azure_blob {
                            format!(
                                "<Blob><Name>{key}</Name><Properties><Last-Modified>Wed, 01 Jan 2020 00:00:00 GMT</Last-Modified><Etag>test</Etag><Content-Length>{}</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType></Properties></Blob>",
                                bytes.len()
                            )
                        } else {
                            format!(
                                "<Contents><Key>{key}</Key><LastModified>2020-01-01T00:00:00Z</LastModified><Size>{}</Size></Contents>",
                                bytes.len()
                            )
                        };
                        entries.push_str(&entry);
                    }
                    let xml = if is_azure_blob {
                        format!(
                            "<EnumerationResults><Blobs>{entries}</Blobs><NextMarker /></EnumerationResults>"
                        )
                    } else {
                        format!(
                            "<ListBucketResult><IsTruncated>false</IsTruncated>{entries}</ListBucketResult>"
                        )
                    };
                    (200, xml.into_bytes())
                } else if let Some(bytes) = request_path
                    .as_ref()
                    .strip_prefix(if is_azure_blob {
                        "container/"
                    } else {
                        "bucket/"
                    })
                    .and_then(|key| objects.get(key))
                {
                    (200, bytes.clone())
                } else {
                    (404, Vec::new())
                };
                let mut content_range = String::new();
                if status == 200 && !listing && !request.starts_with("HEAD ") {
                    let range = request.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        (name.eq_ignore_ascii_case("range")
                            || name.eq_ignore_ascii_case("x-ms-range"))
                        .then(|| value.trim().strip_prefix("bytes="))
                        .flatten()
                    });
                    if let Some(range) = range {
                        let (start, end) = range.split_once('-').unwrap();
                        let start: usize = start.parse().unwrap();
                        let end = if end.is_empty() {
                            body.len() - 1
                        } else {
                            end.parse().unwrap()
                        };
                        assert!(
                            start <= end && end < body.len(),
                            "invalid fixture range {range}"
                        );
                        content_range =
                            format!("Content-Range: bytes {start}-{end}/{}\r\n", body.len());
                        body = body[start..=end].to_vec();
                        status = 206;
                    }
                }
                let length = body.len();
                if request.starts_with("HEAD ") {
                    body.clear();
                }
                let headers = format!(
                    "HTTP/1.1 {status} OK\r\n{content_range}Content-Length: {length}\r\nContent-Type: {}\r\nLast-Modified: Wed, 01 Jan 2020 00:00:00 GMT\r\nETag: test\r\nConnection: close\r\n\r\n",
                    if listing {
                        "application/xml"
                    } else {
                        "application/octet-stream"
                    }
                );
                let _ = socket
                    .write_all(headers.as_bytes())
                    .and_then(|()| socket.write_all(&body));
            }
        });
        Ok(Self {
            endpoint,
            requests,
            stop,
            deny_requests,
            thread: Some(thread),
        })
    }
}

impl Drop for MockCloudStorageServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}

fn partitioned_corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned")
}

pub(super) fn partitioned_objects() -> Result<BTreeMap<String, Vec<u8>>, Box<dyn std::error::Error>>
{
    let table = partitioned_corpus().join("table");
    let log = fs::read(table.join("_delta_log/00000000000000000000.json"))?;
    let mut objects = BTreeMap::new();
    for line in log
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let action: Value = serde_json::from_slice(line)?;
        if let Some(path) = action["add"]["path"].as_str() {
            objects.insert(format!("table/{path}"), fs::read(table.join(path))?);
        }
    }
    objects.insert("table/_delta_log/00000000000000000000.json".into(), log);
    Ok(objects)
}

/// Keeps fixture rows intact but forces a second range request after the initial footer read.
pub(super) fn partitioned_objects_with_large_parquet_footers()
-> Result<BTreeMap<String, Vec<u8>>, Box<dyn std::error::Error>> {
    use parquet::{
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        file::metadata::KeyValue,
    };

    let mut objects = partitioned_objects()?;
    let log_path = "table/_delta_log/00000000000000000000.json";
    let mut actions = Vec::new();
    let padding_length = delta_arrow_reader::DeltaScanExecutionOptions::new()
        .parquet_metadata_size_hint_bytes()
        .unwrap_or(8)
        * 2;
    for line in objects[log_path]
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        actions.push(serde_json::from_slice::<Value>(line)?);
    }
    for action in &mut actions {
        if let Some(path) = action["add"]["path"].as_str() {
            let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(
                partitioned_corpus().join("table").join(path),
            )?)?
            .build()?;
            let mut writer = ArrowWriter::try_new(Vec::new(), reader.schema(), None)?;
            writer.append_key_value_metadata(KeyValue::new(
                "fixture_padding".to_owned(),
                "x".repeat(padding_length),
            ));
            for batch in reader {
                writer.write(&batch?)?;
            }
            let bytes = writer.into_inner()?;
            let size = bytes.len();
            objects.insert(format!("table/{path}"), bytes);
            action["add"]["size"] = json!(size);
        }
    }
    let mut log = Vec::new();
    for action in actions {
        serde_json::to_writer(&mut log, &action)?;
        log.push(b'\n');
    }
    objects.insert(log_path.to_owned(), log);
    Ok(objects)
}

/// A second snapshot drops one partition so a version-selection regression changes the rows.
pub(super) fn append_west_partition_removal_commit(objects: &mut BTreeMap<String, Vec<u8>>) {
    let path = objects
        .keys()
        .find(|key| key.starts_with("table/region=west/"))
        .unwrap();
    let action = json!({"remove": {
        "path": path.strip_prefix("table/").unwrap(), "deletionTimestamp": 1, "dataChange": true
    }});
    objects.insert(
        "table/_delta_log/00000000000000000001.json".into(),
        format!("{action}\n").into_bytes(),
    );
}

pub(super) fn expected_partitioned_rows() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let reader = FileReader::try_new(
        fs::File::open(partitioned_corpus().join("expected.arrow"))?,
        None,
    )?;
    Ok(concat_batches(
        &reader.schema(),
        &reader.collect::<Result<Vec<_>, _>>()?,
    )?)
}
