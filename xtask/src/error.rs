use std::{io, path::PathBuf, process::ExitStatus, string::FromUtf8Error};

use snafu::Snafu;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum Error {
    #[snafu(display("Could not {operation}: {source}"))]
    Io {
        operation: &'static str,
        source: io::Error,
    },
    #[snafu(display("Could not access {}: {source}", path.display()))]
    File { path: PathBuf, source: io::Error },
    #[cfg(target_os = "linux")]
    #[snafu(display("Could not create HTTP fixture response: {source}"))]
    HttpResponse { source: hyper::http::Error },
    #[cfg(target_os = "linux")]
    #[snafu(display("Could not read TLS fixture {}: {source}", path.display()))]
    Pem {
        path: PathBuf,
        source: tokio_rustls::rustls::pki_types::pem::Error,
    },
    #[cfg(target_os = "linux")]
    #[snafu(display("Could not configure fixture TLS: {source}"))]
    Tls { source: tokio_rustls::rustls::Error },
    #[snafu(display("Invalid {subject}: {source}"))]
    Json {
        subject: &'static str,
        source: serde_json::Error,
    },
    #[snafu(display("Invalid Arrow data in {}: {source}", path.display()))]
    Arrow {
        path: PathBuf,
        source: arrow_schema::ArrowError,
    },
    #[cfg(target_os = "linux")]
    #[snafu(display("Could not process Parquet fixture {}: {source}", path.display()))]
    Parquet {
        path: PathBuf,
        source: parquet::errors::ParquetError,
    },
    #[snafu(display("Invalid TOML in {}: {source}", path.display()))]
    Toml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[snafu(display("Could not run {program}: {source}"))]
    CommandStart { program: String, source: io::Error },
    #[snafu(display("{program} failed: {status}"))]
    CommandFailed { program: String, status: ExitStatus },
    #[snafu(display("Invalid UTF-8 in {subject}: {source}"))]
    Utf8 {
        subject: String,
        source: FromUtf8Error,
    },
    #[snafu(display("Invalid ELF inspection pattern: {source}"))]
    Pattern { source: regex::Error },
    #[snafu(display("{message}"))]
    InvalidInput { message: String },
}
