//! Mock Azure token responses shared by OAuth and managed identity tests.

use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use serde_json::json;

/// Records OAuth and metadata requests, issuing expired tokens to force refresh without sleeps.
pub(super) struct MockAzureTokenServer {
    pub(super) endpoint: String,
    pub(super) requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockAzureTokenServer {
    /// Rejects token requests after the given number of successful responses.
    pub(super) fn new(successful_token_requests: usize) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                let Ok((mut socket, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut headers = Vec::new();
                let mut byte = [0];
                while !headers.ends_with(b"\r\n\r\n") {
                    assert!(headers.len() < 16 * 1024, "fixture headers too large");
                    socket.read_exact(&mut byte).unwrap();
                    headers.push(byte[0]);
                }
                let mut request = String::from_utf8(headers).unwrap();
                let length: usize = request
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                assert!(length < 16 * 1024, "fixture body too large");
                let mut body = vec![0; length];
                socket.read_exact(&mut body).unwrap();
                request.push_str(&String::from_utf8(body).unwrap());
                let count = {
                    let mut requests = recorded.lock().unwrap();
                    requests.push(request);
                    requests.len()
                };
                let (status, body) = if count <= successful_token_requests {
                    (
                        200,
                        // OAuth and metadata clients use different expiry fields.
                        json!({"access_token":format!("secret-oauth-{count}"), "expires_in":0, "expires_on":"0"}),
                    )
                } else {
                    (
                        400,
                        json!({"error":"invalid_client", "error_description":"secret-rejected-client"}),
                    )
                };
                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes());
            }
        });
        Ok(Self {
            endpoint,
            requests,
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for MockAzureTokenServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}
