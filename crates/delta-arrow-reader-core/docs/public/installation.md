# Installation

Choose the interface you want to use. You only need the installation steps for
that interface:

- [Python](#python): read with PyArrow or run SQL.
- [Command line](#cli-binaries-and-container-images): inspect, preview, and query tables.
- [Rust](#streaming-reader): read batches in an application, with optional SQL support.

## Python

Use CPython 3.10 or newer on one of these platforms:

| System | Requirements |
| --- | --- |
| Linux | x86_64, glibc 2.28 or newer |
| Windows | x86_64 |
| macOS | Apple silicon, macOS 12.0 or newer |

The Python API includes PyArrow support and SQL. The package also installs the
`delta-arrow-reader` command; its available commands depend on the package version,
as described under [CLI installation](#cli-binaries-and-container-images).
You do not need Rust or the separate Python `datafusion` package to install a
wheel.

### Install from PyPI

In your project's activated virtual environment, run:

```console
python -m pip install --only-binary=:all: delta-arrow-reader
python -I -c "import delta_arrow_reader; print(delta_arrow_reader.__version__)"
```

The second command prints the installed version. Installation also downloads a
compatible PyArrow wheel; neither package is compiled from source. For a project
managed by uv, use `uv add delta-arrow-reader` instead.

Continue with the [Python quickstart](python.md)
to read your first rows.

<details markdown="1">
<summary>Supported Python versions and platform details</summary>

The wheel platform tags are `manylinux_2_28_x86_64`, `win_amd64`, and
`macosx_12_0_arm64`, respectively.

These targets apply to both the Python extension and bundled CLI. Linux arm64,
Intel Macs, Windows arm64, and musl systems have no official wheels or wheel CI
coverage. Source builds on Intel Macs are untested and unsupported. The wheel's
Linux baseline is glibc 2.28; the standalone `dar` archives require glibc 2.36.

Each platform has one `cp310-abi3` wheel. A successful build tests that same
wheel on CPython 3.10 with PyArrow 18.0.0 and CPython 3.14 with the current
compatible PyArrow release. Intermediate Python versions are not separately
tested. Free-threaded CPython, PyPy, and other platform targets are not covered.

All three platforms check the installed package, Python Arrow streams, SQL
queries and CLI behavior, including scan output, errors, closed stdout, and
process termination.
Linux also runs the full Python suite and repeats the installed checks in a fresh
glibc 2.28 container. Windows checks run on Windows Server 2022 and use the
CPython/MSVC runtime libraries. The macOS deployment target is 12.0, but runtime
checks use newer macOS runners. Your Python and PyArrow distributions must also
support your OS.

</details>

<details markdown="1">
<summary id="build-from-source">Build the Python package from source instead</summary>

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

</details>

## CLI binaries and container images

The v0.7.0 release provides `inspect` and `scan`. The preview, SQL, discovery,
and saved-manifest workflows on these pages require a build with those features.
If your command's `--help` lists only `inspect` and `scan`, use a
[source build](#build-cli-from-source) to follow those workflows.

If you installed the Python package above, you already have the CLI. The same
command-availability check applies to the bundled command:

```console
delta-arrow-reader --help
```

Use `delta-arrow-reader` in place of `dar` in the
[CLI quickstart](cli-quickstart.md).
Both names accept the same arguments. The wheel does not install a `dar` alias.

Standalone `dar` binaries and container images are available from
[GitHub Releases](https://github.com/mag1cfrog/delta-arrow-reader/releases).
Choose a version whose CLI section reports successful download verification.

<details markdown="1">
<summary id="install-cli-with-uv">Install just the command with uv</summary>

[Install uv](https://docs.astral.sh/uv/getting-started/installation/) if needed,
then install the command from PyPI in a separate tool environment. This uses
the [wheel platforms listed above](#python), including Windows x86_64 and
Apple silicon macOS. Check the command availability note above before using
the newer workflows:

```console
uv tool install --no-build delta-arrow-reader
delta-arrow-reader --version
```

If uv reports that its executable directory is missing from `PATH`, run
`uv tool update-shell` and restart your shell. To run without a persistent tool
installation:

```console
uvx --no-build delta-arrow-reader --help
```

These tool environments do not add the Python API to your project. They still
install the extension, CLI, and PyArrow. Keep the tool environment and its Python
installation available when using the command.

</details>

<details markdown="1">
<summary>Download and verify a standalone Linux release</summary>

Choose a release whose CLI section reports successful download verification.
Replace `X.Y.Z` below with that version, without the `v` prefix. Run this script
in a writable directory. It downloads both architectures so the complete checksum
file can be checked before extraction:

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
export PATH="$PWD/cli:$PATH"
```

The final command adds `dar` to `PATH` for this shell. For later use, add the
extracted `cli` directory to your application's executable search path or use
its absolute path. Keep `LICENSE` and `NOTICE` beside it when redistributing it.

The binaries require Linux amd64 or arm64, glibc 2.36 or newer, libgcc, and CA
certificates for HTTPS. They do not need Rust or Python. They do not run on
Alpine's musl runtime, `scratch`, macOS, or Windows; Python wheels have their own
platform requirements. See [Linux runtime details](cli.md#use-a-linux-binary)
for supported distributions and packages.

`cli-artifacts.json` records the tested source commit, toolchain, runtime
libraries, and hashes. The Release's CLI section lists the final release commit
and checksum file's hash. The tested and final commits can differ while
containing the same source tree.

</details>

<details markdown="1">
<summary>Use a published container</summary>

Choose a release with a verified CLI container, then pin its version:

```sh
docker run --rm --read-only ghcr.io/mag1cfrog/delta-arrow-reader:vX.Y.Z --version
```

The public image includes the runtime libraries and certificates for
`linux/amd64` and `linux/arm64`. It runs a command and exits. Public pulls need
no GitHub login. There are no `latest`, major-version, or minor-version aliases.

For a digest pin, copy the index digest from the Release's CLI section and use
`ghcr.io/mag1cfrog/delta-arrow-reader@sha256:<index-hash>`. See
[container usage](cli.md#use-a-container-or-a-go-service)
for mounting a table or integrating the CLI with a Go service.

</details>

<details markdown="1">
<summary id="build-cli-from-source">Build the CLI from source</summary>

Use a source build when you need a feature that your installed release does not
provide. Install [Git](https://git-scm.com/install/),
[Rust 1.94 or newer with rustup](https://rust-lang.org/tools/install/), and the
[native build prerequisites](#https-and-build-prerequisites). Rustup also
installs Cargo, the build command used below. Restart your terminal after
installation if `cargo` is not found. On Windows, use Git Bash for these Bash
examples and install the MSVC prerequisites linked below.

Then run:

```sh
git clone https://github.com/mag1cfrog/delta-arrow-reader.git
cd delta-arrow-reader
cargo build --locked --release -p delta-arrow-reader-cli
export PATH="$PWD/target/release:$PATH"
dar --version
```

If you already have a checkout, run the build there. The executable is
`target/release/dar` (`dar.exe` on Windows). The `PATH` change applies to this
shell; for later runs, use the executable's path or add its directory to `PATH`.
This builds for the host platform and does not install the Databricks CLI.

</details>

## Rust

Use Rust 1.94 or newer. Run the commands below from your Cargo project.
The examples use your application's Tokio runtime.

### Streaming reader

```bash
cargo add delta-arrow-reader
cargo add tokio --features macros,rt-multi-thread
```

Continue with the [Rust quickstart](streaming-reader.md)
to read your first batch. It uses `next_batch()`, so no separate stream utility
dependency is needed.

### DataFusion adapter

To query the table with SQL in Rust:

```bash
cargo add delta-arrow-reader --features datafusion
cargo add datafusion@54 --no-default-features --features sql
cargo add tokio --features macros,rt-multi-thread
```

The adapter uses DataFusion 54. Your direct DataFusion dependency must use the
same major version. Continue with the [DataFusion quickstart](datafusion.md).

<details markdown="1">
<summary id="optional-cargo-feature">Optional Cargo feature</summary>

The streaming API and both Parquet backends are always available. The optional
`datafusion` feature adds SQL integration. The direct Parquet backend is the
default; choosing the Delta Kernel backend does not require another feature.
See [execution options](reference/execution-options.md)
when you need to change read settings.

</details>

## HTTPS and build prerequisites

When building from source, follow the
[Rust installation instructions](https://doc.rust-lang.org/book/ch01-01-installation.html)
for your operating system, including its compiler/linker prerequisites:

| System | Native tools |
| --- | --- |
| Ubuntu/Debian | Install the `build-essential` package. Other Linux distributions provide GCC or Clang through their package manager. |
| macOS | Run `xcode-select --install` to install the command-line developer tools. |
| Windows | Follow the [rustup MSVC setup](https://rust-lang.github.io/rustup/installation/windows-msvc.html) for C++ build tools and the Windows SDK. |

OpenSSL development libraries are not required. When using HTTPS storage,
ensure your system or container has a CA certificate bundle, such as Linux's
`ca-certificates` package.

<details markdown="1">
<summary>Private endpoints and certificate validation</summary>

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

</details>
