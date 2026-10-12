# Installation

The Rust crate requires Rust 1.94 or newer. The dependencies you need
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

Both Rust APIs run on your application's Tokio runtime. They do not create a
separate runtime.

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

## CLI binaries and container images

The `dar` executable lets Go and other languages read Delta tables through a
subprocess. `inspect` returns JSON; `scan` writes an Arrow IPC stream to stdout.
Installation requires no Rust or Python runtime.

Choose a stable version from [GitHub Releases](https://github.com/mag1cfrog/delta-arrow-reader/releases)
whose CLI section reports successful download verification. Older releases may
have no CLI files. Replace `X.Y.Z` below with that version, without the `v`
prefix. On Linux, run this script in a writable directory; it downloads both architectures
so the complete checksum file can be checked before extraction:

```sh
set -eu
version=X.Y.Z
mkdir "dar-$version"
cd "dar-$version"
release_url="https://github.com/mag1cfrog/delta-arrow-reader/releases/download/v$version"
for file in "dar-v$version-linux-amd64-gnu.tar.gz" \
            "dar-v$version-linux-arm64-gnu.tar.gz" cli-artifacts.json SHA256SUMS; do
  curl --fail --location --output "$file" "$release_url/$file"
done
sha256sum --check SHA256SUMS
case "$(uname -m)" in
  x86_64) architecture=amd64 ;;
  aarch64|arm64) architecture=arm64 ;;
  *) echo 'No release binary for this architecture' >&2; exit 1 ;;
esac
mkdir cli
tar -xzf "dar-v$version-linux-$architecture-gnu.tar.gz" -C cli
./cli/dar --version
```

Add the extracted `cli` directory to your application's executable search path,
or invoke `dar` by its absolute path. Keep `LICENSE` and `NOTICE` beside it when
redistributing the binary. `cli-artifacts.json` records the tested source commit,
toolchain, runtime libraries, and binary/archive hashes. The Release's CLI
section also lists the final release commit and the checksum file's own hash.
The tested PR merge commit and final release commit may differ while containing
the same source tree.

The binaries support Linux amd64 with a baseline x86-64 CPU and Linux arm64 with
a baseline ARMv8-A CPU. Both require glibc 2.36 or newer, libgcc, and CA
certificates for HTTPS. The same Debian 12 builds are tested on Debian 12,
Ubuntu 24.04, and Fedora 44. Install `libc6`, `libgcc-s1`, and `ca-certificates`
on Debian/Ubuntu, or `glibc`, `libgcc`, and `ca-certificates` on Fedora. Alpine's
musl runtime, `scratch`, macOS, and Windows have no official CLI release binary.

The public container includes those runtime dependencies. It runs `dar` as a
command and exits; it does not host an HTTP service. Pin the version:

```sh
docker run --rm --read-only ghcr.io/mag1cfrog/delta-arrow-reader:vX.Y.Z --version
```

For a digest pin, copy the index digest from the Release's CLI section and use
`ghcr.io/mag1cfrog/delta-arrow-reader@sha256:<index-hash>`. The index contains
both `linux/amd64` and `linux/arm64`. Public pulls need no GitHub login.
There are no `latest`, major-version, or minor-version aliases.

See the [CLI guide](cli.md) for table mounts, storage configuration, and scan
options. The [Go consumer](https://github.com/mag1cfrog/delta-arrow-reader/tree/main/examples/go-cli)
shows how to copy the binary and notices from a pinned CLI image into a service
image, read Arrow batches without cgo, and handle process failures. The service
image must provide the same glibc, libgcc, and certificate dependencies.

## Python

The Python package loads Delta snapshots, exposes their versions and Arrow
schemas, and streams rows through PyArrow. The package is not published on PyPI yet.

### Install a CI wheel

The wheel builder targets these systems:

| System target | Wheel platform tag |
| --- | --- |
| Linux x86_64, glibc 2.28 or newer | `manylinux_2_28_x86_64` |
| Windows x86_64 | `win_amd64` |
| macOS arm64, macOS 12.0 or newer | `macosx_12_0_arm64` |

Intel Macs (macOS x86_64) have no official wheels or CI coverage. Source builds
on Intel Macs are untested and unsupported.

Each platform has one `cp310-abi3` wheel. A successful build tests that same
wheel on CPython 3.10 with PyArrow 18.0.0 and CPython 3.14 with the current
compatible PyArrow release. Intermediate Python versions are not separately
tested. Free-threaded CPython, PyPy, and other platform targets are not covered.

Linux runs the full Python test suite. Windows and macOS run the installed
package and Arrow stream smoke checks. Windows checks run on Windows Server
2022. The macOS deployment target is 12.0, but runtime checks use newer macOS
runners. Your Python and PyArrow distributions must also support your OS.

Choose a run where all Python wheel jobs passed, including
`Validate wheel artifact set`. Download the artifact for your platform and
extract its `.whl` file. The
[wheel build instructions](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/contributing.md#build-python-wheels-in-ci)
list the artifact names, download commands, and CI runners.

In an activated virtual environment, replace `/path/to/wheel.whl` with the
full path to the extracted wheel, keeping its original filename:

```console
python -m pip install --only-binary=:all: "/path/to/wheel.whl"
python -I -c "import delta_arrow_reader; print(delta_arrow_reader.__version__)"
delta-arrow-reader --version
```

This installs the wheel and a compatible PyArrow wheel without compiling from
source. Rust and native build tools are not needed for wheel installation.
The same package installs the native `delta-arrow-reader` command alongside
the Python API. For a separate CLI environment, use `uv tool install /path/to/wheel.whl`
or `uvx --from /path/to/wheel.whl delta-arrow-reader --help` with the wheel's full
filename. A uv tool environment does not add the Python API to your project's
environment.

### Build from source

Use Python 3.10 or newer and Rust 1.94.x to install from source. Source builds also
need `curl`, `cargo-about`, and Rust's license documentation. From the repository
root, run these commands on Linux x86_64 or macOS arm64:

```console
python3 -m venv target/python-venv
. target/python-venv/bin/activate
rustup component add rust-docs
cargo install --locked --features cli --version 0.9.2 cargo-about
python -m pip install ./crates/delta-arrow-reader-python
```

Installation also installs PyArrow 18 or newer. The
[Python quickstart](https://mag1cfrog.github.io/delta-arrow-reader/python/)
uses a sample table included in the repository.

### Read a table

To read your own table, replace `/path/to/delta-table` with its local path:

```python
from pathlib import Path

from delta_arrow_reader import DeltaTable

location = Path("/path/to/delta-table")
table = DeltaTable(location)
print(table.version)
print(table.schema)

with table.to_reader() as reader:
    for batch in reader:
        print(batch.num_rows)

original = DeltaTable(location, version=0)
print(original.schema)
```

Omitting `version` or passing `None` loads the latest snapshot. An explicit
version must be an integer from `0` to `2**64 - 1`; booleans are rejected.
Each table keeps the snapshot it loaded, even if new commits arrive. Its
`version` and `schema` properties are read-only, and the `pyarrow.Schema`
remains usable after the table is deleted.

Each `to_reader()` call creates an independent `pyarrow.RecordBatchReader` over
the selected columns and rows of that snapshot. Planning reads Delta metadata;
Parquet reads start when you request a batch. Use a `with` block to close the reader,
including when you stop early. Readers and returned batches remain usable after
the table is deleted. Calling `reader.read_all()` materializes all remaining rows
in memory.

To pass a stream to an Arrow consumer directly, use `table.scan()`. For PyArrow,
the equivalent reader construction is:

```python
import pyarrow as pa

with table.scan() as stream:
    with pa.RecordBatchReader.from_stream(stream, schema=table.schema) as reader:
        for batch in reader:
            print(batch.num_rows)
            break  # Both context managers still close on early exit.
```

`scan()` returns a `RecordBatchStream`, which has no public constructor. A stream
can be exported once. Closing it before export releases the scan; subsequent
export attempts raise `RuntimeError`. After export, the consumer owns cleanup,
so closing the original stream does not close the consumer. Repeated `close()`
calls are harmless, and context-manager exit does not suppress exceptions.

Both methods accept a keyword-only `columns` argument. Omit it or pass `None`
to read all columns. A list or tuple selects columns in the given order, such as
`table.to_reader(columns=["value", "id"])` on a table with those columns. An empty
list or tuple selects no columns while preserving row counts. Bare strings and
non-string entries raise `TypeError`; unknown or repeated column names raise
`DeltaReaderError` with code `invalid_projection`.

Both methods also accept a keyword-only `limit` argument. Omit it or pass `None`
to read all rows, or use a nonnegative integer to cap the result, such as
`table.to_reader(columns=["value", "id"], limit=100)`. The integer must fit Rust's
`usize` on the platform; larger values raise `OverflowError`. Negative values
raise `ValueError`, and booleans and other types raise `TypeError`. `limit=0`
returns an empty result with the selected schema without reading data files.

The optional `schema` request must match the scan schema, including field order,
names, nullability, and metadata. Incompatible schemas raise `ValueError`.
Requests for alternate Arrow representations, such as a different integer width
or string encoding, raise `NotImplementedError`; the exporter does not cast or
project data. A rejected request leaves the stream available for another attempt.

`location` also accepts a string path or a supported storage URL. Pass backend
settings through `storage_options`, a mapping of string keys to string values
that the constructor copies before loading.

Loading and planning failures raise `delta_arrow_reader.DeltaReaderError` with a
redacted message and `phase` and `code` attributes. Failures while consuming a
reader raise PyArrow exceptions with redacted `phase` and `code` text in the
message. Invalid arguments raise Python's `TypeError`, `ValueError`, or
`OverflowError`.

Loading, planning, and batch reads release the GIL so other Python threads can
run. When loading or planning on Python's main thread, Ctrl+C raises
`KeyboardInterrupt` and cancels the pending operation. Interruption during a
batch read reaches the caller as a PyArrow exception.
Synchronous Kernel work already running may still finish after interruption;
cancellation does not stop it immediately.
