# Installation

Delta Arrow Reader requires Rust 1.94 or newer. The dependencies you need
depend on whether you plan to use the streaming API or DataFusion.

## Streaming reader

For streaming Arrow batches, add the reader, Tokio, and the futures utilities
used by the quickstart:

```bash
cargo add delta-arrow-reader futures-util
cargo add tokio --features macros,rt-multi-thread
```

The [streaming reader quickstart](https://mag1cfrog.github.io/delta-arrow-reader/streaming-reader/)
shows how to load a table and consume its Arrow batches.

## DataFusion adapter

For SQL queries, enable the reader's `datafusion` feature and add DataFusion:

```bash
cargo add delta-arrow-reader --features datafusion
cargo add datafusion@54 --no-default-features --features sql
cargo add tokio --features macros,rt-multi-thread
```

The adapter uses DataFusion 54. Your application's direct DataFusion dependency
must use the same major version because provider types from different majors
are not interchangeable.

The [DataFusion quickstart](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/)
shows how to register a table and query it with SQL.

## Optional Cargo feature

The streaming API and both Parquet backends are always available. Enable the
optional `datafusion` feature to register Delta tables with DataFusion and
expose execution metrics.

The direct Parquet backend is selected by default. Rust callers can choose the
Delta Kernel backend through `DeltaScanExecutionOptions` without changing
Cargo features.

Both APIs run on your application's Tokio runtime. Delta Arrow Reader does not
create a separate runtime.

## HTTPS and build prerequisites

The reader selects rustls for both object-store access and Kernel's HTTPS
reads. Its default and `datafusion` dependency graphs do not require OpenSSL
development libraries. Native build tools are still needed: Kernel's rustls
feature uses AWS-LC, which builds native cryptographic code.

HTTPS connections verify the server's certificate and hostname. Install your
organization's CA in the platform trust store when accessing private endpoints.
On Linux, both HTTP clients load system CA certificates and also honor
`SSL_CERT_FILE` and `SSL_CERT_DIR`. Minimal container images need a CA bundle
such as the distribution's `ca-certificates` package.

Kernel's HTTPS client uses `rustls-platform-verifier`: certificate validation
uses WebPKI on Linux and the platform verifier on macOS and Windows. The
object-store client uses rustls with native root certificates. This replaces
Kernel's previous native-tls/OpenSSL validation on Linux, so certificates must
also satisfy WebPKI's validation rules. Kernel HTTPS connections use TLS 1.2
or 1.3. Storage options and proxy configuration continue to use the underlying
clients' existing interfaces.
