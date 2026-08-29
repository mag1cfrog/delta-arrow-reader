//! HTTP fixture server with controlled request latency and shared throughput.

use std::{
    collections::BTreeMap,
    error::Error,
    fs::{self, File},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    net::{Shutdown, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::TransportProfile;

#[derive(Debug)]
pub(super) struct ControlledHttpServer {
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    url: String,
    state: Arc<ServerState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ServerStatsSnapshot {
    pub(super) range_requests: u64,
    pub(super) range_bytes: u64,
}

#[derive(Debug, Default)]
struct ServerStats {
    range_requests: AtomicU64,
    range_bytes: AtomicU64,
}

#[derive(Debug)]
struct ServerState {
    profile: TransportProfile,
    shared_bandwidth: Mutex<()>,
    stats: ServerStats,
}

impl ControlledHttpServer {
    pub(super) fn start(root: PathBuf, profile: TransportProfile) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let url = format!("http://{}/", listener.local_addr()?);
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let state = Arc::new(ServerState {
            profile,
            shared_bandwidth: Mutex::new(()),
            stats: ServerStats::default(),
        });
        let worker_state = Arc::clone(&state);
        let handle = thread::spawn(move || {
            serve_http(listener, root, worker_shutdown, worker_state);
        });
        Ok(Self {
            shutdown,
            handle: Some(handle),
            url,
            state,
        })
    }

    pub(super) fn url(&self) -> &str {
        &self.url
    }

    pub(super) fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    pub(super) fn reset_stats(&self) {
        self.state.stats.range_requests.store(0, Ordering::Relaxed);
        self.state.stats.range_bytes.store(0, Ordering::Relaxed);
    }

    pub(super) fn stats(&self) -> ServerStatsSnapshot {
        ServerStatsSnapshot {
            range_requests: self.state.stats.range_requests.load(Ordering::Relaxed),
            range_bytes: self.state.stats.range_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Drop for ControlledHttpServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn serve_http(
    listener: TcpListener,
    root: PathBuf,
    shutdown: Arc<AtomicBool>,
    state: Arc<ServerState>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                let root = root.clone();
                let shutdown = Arc::clone(&shutdown);
                let state = Arc::clone(&state);
                let _ = thread::Builder::new()
                    .name("delta-arrow-reader-range-bench-http".to_owned())
                    .spawn(move || {
                        let _ = handle_http(stream, &root, &shutdown, &state);
                    });
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(_) => break,
        }
    }
}

fn handle_http(
    stream: TcpStream,
    root: &Path,
    shutdown: &AtomicBool,
    state: &ServerState,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    loop {
        let Some(request) = read_http_request(&mut reader)? else {
            return Ok(());
        };
        if shutdown.load(Ordering::Relaxed) {
            let _ = reader.get_mut().shutdown(Shutdown::Both);
            return Ok(());
        }
        let close = request.headers.get("connection").map(String::as_str) == Some("close");
        let stream = reader.get_mut();
        match request.method.as_str() {
            "PROPFIND" => propfind(stream, root, &request),
            "HEAD" => file_response(
                stream,
                root,
                &request.path,
                request.headers.get("range").map(String::as_str),
                true,
                state,
            ),
            "GET" => file_response(
                stream,
                root,
                &request.path,
                request.headers.get("range").map(String::as_str),
                false,
                state,
            ),
            _ => write_response_headers(
                stream,
                405,
                "Method Not Allowed",
                &[("Content-Length", "0".to_owned())],
            ),
        }?;
        if close {
            return Ok(());
        }
    }
}

struct HttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
}

fn read_http_request(reader: &mut impl BufRead) -> io::Result<Option<HttpRequest>> {
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None);
    }
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return Ok(None);
    };
    let mut headers = BTreeMap::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    let path = target.split('?').next().unwrap_or_default();
    let path = percent_decode(path.trim_start_matches('/'))?;
    if path
        .split('/')
        .any(|component| component == ".." || component.contains('\\'))
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid controlled HTTP path",
        ));
    }
    Ok(Some(HttpRequest {
        method: method.to_owned(),
        path,
        headers,
    }))
}

fn propfind(stream: &mut TcpStream, root: &Path, request: &HttpRequest) -> io::Result<()> {
    let requested = root.join(&request.path);
    if !requested.exists() {
        return write_response_headers(
            stream,
            404,
            "Not Found",
            &[("Content-Length", "0".to_owned())],
        );
    }
    let recursive = request.headers.get("depth").map(String::as_str) != Some("0");
    let mut entries = Vec::new();
    collect_entries(root, &request.path, &requested, recursive, &mut entries)?;
    let body = multistatus_xml(&entries)?;
    write_response_headers(
        stream,
        207,
        "Multi-Status",
        &[
            ("Content-Type", "application/xml; charset=utf-8".to_owned()),
            ("Content-Length", body.len().to_string()),
        ],
    )?;
    stream.write_all(body.as_bytes())
}

struct HttpEntry {
    href: String,
    size: u64,
    is_dir: bool,
    modified: SystemTime,
}

fn collect_entries(
    root: &Path,
    relative: &str,
    path: &Path,
    recursive: bool,
    entries: &mut Vec<HttpEntry>,
) -> io::Result<()> {
    let metadata = fs::metadata(path)?;
    entries.push(HttpEntry {
        href: format!("/{}", relative.trim_start_matches('/')),
        size: if metadata.is_file() {
            metadata.len()
        } else {
            0
        },
        is_dir: metadata.is_dir(),
        modified: metadata.modified().unwrap_or(UNIX_EPOCH),
    });
    if metadata.is_dir() && recursive {
        let mut children = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        children.sort_by_key(std::fs::DirEntry::path);
        for child in children {
            let child_path = child.path();
            let child_relative = child_path
                .strip_prefix(root)
                .map_err(|_| io::Error::other("controlled HTTP path escaped root"))?
                .to_string_lossy()
                .replace('\\', "/");
            collect_entries(root, &child_relative, &child_path, true, entries)?;
        }
    }
    Ok(())
}

fn multistatus_xml(entries: &[HttpEntry]) -> io::Result<String> {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?><multistatus>"#);
    for entry in entries {
        let resource_type = if entry.is_dir {
            "<resourcetype><collection/></resourcetype>"
        } else {
            "<resourcetype/>"
        };
        xml.push_str(&format!(
            "<response><href>{}</href><propstat><prop><getlastmodified>{}</getlastmodified><getcontentlength>{}</getcontentlength>{resource_type}<getetag>\"{}\"</getetag></prop><status>HTTP/1.1 200 OK</status></propstat></response>",
            xml_escape(&entry.href),
            http_date(entry.modified),
            entry.size,
            etag(entry.size, entry.modified)?,
        ));
    }
    xml.push_str("</multistatus>");
    Ok(xml)
}

fn file_response(
    stream: &mut TcpStream,
    root: &Path,
    request_path: &str,
    range_header: Option<&str>,
    head_only: bool,
    state: &ServerState,
) -> io::Result<()> {
    let path = root.join(request_path);
    let metadata = match fs::metadata(&path) {
        Ok(metadata) if metadata.is_file() => metadata,
        _ => {
            return write_response_headers(
                stream,
                404,
                "Not Found",
                &[("Content-Length", "0".to_owned())],
            );
        }
    };
    let size = metadata.len();
    let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
    let range = parse_range(range_header, size)?;
    let (status, text, start, end) = match range {
        Some((start, end)) => (206, "Partial Content", start, end),
        None => (200, "OK", 0, size),
    };
    let content_len = end.saturating_sub(start);
    let mut headers = vec![
        ("Accept-Ranges", "bytes".to_owned()),
        ("Content-Length", content_len.to_string()),
        ("Last-Modified", http_date(modified)),
        ("ETag", format!("\"{}\"", etag(size, modified)?)),
    ];
    if status == 206 {
        headers.push((
            "Content-Range",
            format!("bytes {start}-{}/{}", end.saturating_sub(1), size),
        ));
    }
    if !head_only && range.is_some() {
        state.stats.range_requests.fetch_add(1, Ordering::Relaxed);
    }

    thread::sleep(state.profile.request_latency);
    write_response_headers(stream, status, text, &headers)?;
    stream.flush()?;
    if head_only {
        return Ok(());
    }

    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut remaining = content_len;
    let mut buffer = [0_u8; 64 * 1024];
    while remaining != 0 {
        let chunk_len = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        file.read_exact(&mut buffer[..chunk_len])?;
        // Keep the throughput limit shared across concurrent responses. Sending the headers
        // before taking this lock keeps request latency separate from payload delivery time.
        let _bandwidth = state
            .shared_bandwidth
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        thread::sleep(transfer_delay(
            chunk_len,
            state.profile.shared_throughput_bytes_per_second,
        ));
        stream.write_all(&buffer[..chunk_len])?;
        stream.flush()?;
        if range.is_some() {
            state
                .stats
                .range_bytes
                .fetch_add(chunk_len as u64, Ordering::Relaxed);
        }
        remaining = remaining.saturating_sub(chunk_len as u64);
    }
    Ok(())
}

fn parse_range(header: Option<&str>, size: u64) -> io::Result<Option<(u64, u64)>> {
    let Some(range) = header.and_then(|value| value.strip_prefix("bytes=")) else {
        return Ok(None);
    };
    if let Some(suffix) = range.strip_prefix('-') {
        let suffix = suffix
            .parse::<u64>()
            .map_err(|_| invalid("invalid HTTP suffix range"))?;
        return Ok(Some((size.saturating_sub(suffix), size)));
    }
    let (start, end) = range.split_once('-').unwrap_or((range, ""));
    let start = start
        .parse::<u64>()
        .map_err(|_| invalid("invalid HTTP range start"))?;
    let end = if end.is_empty() {
        size
    } else {
        end.parse::<u64>()
            .map_err(|_| invalid("invalid HTTP range end"))?
            .saturating_add(1)
            .min(size)
    };
    if start > end || end > size {
        return Err(invalid("invalid HTTP range bounds"));
    }
    Ok(Some((start, end)))
}

fn write_response_headers(
    stream: &mut TcpStream,
    status: u16,
    text: &str,
    headers: &[(&str, String)],
) -> io::Result<()> {
    write!(stream, "HTTP/1.1 {status} {text}\r\n")?;
    for (key, value) in headers {
        write!(stream, "{key}: {value}\r\n")?;
    }
    write!(stream, "\r\n")
}

fn transfer_delay(bytes: usize, bytes_per_second: u64) -> Duration {
    let nanos = (bytes as u128)
        .saturating_mul(1_000_000_000)
        .checked_div(u128::from(bytes_per_second.max(1)))
        .unwrap_or(0);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn http_date(time: SystemTime) -> String {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    chrono::DateTime::from_timestamp(i64::try_from(seconds).unwrap_or(0), 0)
        .map(|date| date.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .unwrap_or_else(|| "Thu, 01 Jan 1970 00:00:00 GMT".to_owned())
}

fn etag(size: u64, modified: SystemTime) -> io::Result<String> {
    let nanos = modified
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    Ok(format!("{size:x}-{nanos:x}"))
}

fn percent_decode(value: &str) -> io::Result<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(invalid("invalid percent encoding"));
            }
            output.push(hex(bytes[index + 1])? * 16 + hex(bytes[index + 2])?);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).map_err(|error| invalid(error.to_string()))
}

fn hex(byte: u8) -> io::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(invalid("invalid hex digit")),
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;

    const TEST_PROFILE: TransportProfile = TransportProfile {
        name: "test",
        request_latency: Duration::ZERO,
        shared_throughput_bytes_per_second: u64::MAX,
    };

    #[test]
    fn transfer_delay_and_range_parser_are_deterministic() -> io::Result<()> {
        assert_eq!(
            transfer_delay(4 * 1024 * 1024, 4 * 1024 * 1024),
            Duration::from_secs(1)
        );
        assert_eq!(parse_range(Some("bytes=2-5"), 10)?, Some((2, 6)));
        assert_eq!(parse_range(Some("bytes=-4"), 10)?, Some((6, 10)));
        Ok(())
    }

    #[test]
    fn server_handles_multiple_requests_on_one_connection() -> Result<(), Box<dyn Error>> {
        let mut server = ControlledHttpServer::start(PathBuf::new(), TEST_PROFILE)?;
        let address = server
            .url()
            .strip_prefix("http://")
            .and_then(|url| url.strip_suffix('/'))
            .ok_or_else(|| io::Error::other("unexpected controlled server URL"))?;
        let mut stream = TcpStream::connect(address)?;
        stream.write_all(
            b"HEAD /missing HTTP/1.1\r\nHost: localhost\r\n\r\n\
              HEAD /missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )?;
        let mut responses = String::new();
        stream.read_to_string(&mut responses)?;

        assert_eq!(responses.matches("HTTP/1.1 404 Not Found").count(), 2);
        server.stop();
        Ok(())
    }
}
