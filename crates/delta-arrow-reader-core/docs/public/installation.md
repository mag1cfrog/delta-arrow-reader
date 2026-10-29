# Installation

Choose the interface you want to use. You only need the installation steps for
that interface:

- [Python](#python): read with PyArrow or run SQL.
- [Command line](#command-line): inspect, preview, and query tables.
- [Rust](#streaming-reader): read batches in an application, with optional SQL support.

## Python

Use CPython 3.10 or newer on one of these platforms:

| System | Requirements |
| --- | --- |
| Linux | x86_64, glibc 2.28 or newer |
| Windows | x86_64 |
| macOS | Apple silicon, macOS 12.0 or newer |

The Python package includes PyArrow support and SQL. Install the
[CLI package](#install-cli-with-uv) for command-line use.
You do not need Rust or the separate Python `datafusion` package to install a
wheel.

### Install from PyPI

In your project's activated virtual environment, run:

```console
python -m pip install --upgrade --only-binary=:all: "delta-arrow-reader>=0.8"
python -I -c "import delta_arrow_reader; print(delta_arrow_reader.__version__)"
```

The second command prints the installed version. Installation also downloads a
compatible PyArrow wheel; neither package is compiled from source. For a project
managed by uv, use `uv add "delta-arrow-reader>=0.8"` instead.

Continue with the [Python quickstart](https://mag1cfrog.github.io/delta-arrow-reader/python/)
to read your first rows.

<details markdown="1">
<summary>Supported Python versions and platform details</summary>

The wheel platform tags are `manylinux_2_28_x86_64`, `win_amd64`, and
`macosx_12_0_arm64`, respectively.

These targets apply to the Python and CLI wheels. Linux arm64,
Intel Macs, Windows arm64, and musl systems have no official wheels or wheel CI
coverage. Source builds on Intel Macs are untested and unsupported. The wheel's
Linux baseline is glibc 2.28; the standalone `dar` archives require glibc 2.36.

Each platform has a `cp310-abi3` Python wheel and a `py3-none` CLI wheel.
A successful build tests the Python wheel on CPython 3.10 with PyArrow 18.0.0
and CPython 3.14 with the current compatible PyArrow release. Intermediate Python versions are not separately
tested. Free-threaded CPython, PyPy, and other platform targets are not covered.

The Linux wheel is tested in a glibc 2.28 container. Windows checks run on
Windows Server 2022 and use the CPython/MSVC runtime libraries. The macOS
deployment target is 12.0, but runtime checks use newer macOS runners. Your Python and PyArrow distributions must also
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

<span id="cli-binaries-and-container-images"></span>

## Command line

Use version 0.8 or newer for the CLI workflows on these pages.

Try the CLI with uv:

```console
uvx --no-build --from delta-arrow-reader-cli dar --help
```

To use `dar` directly in your shell, [install it with uv](#install-cli-with-uv)
or [pipx](#install-cli-with-pipx). Then follow the [CLI quickstart](cli-quickstart.md).

The CLI supports the [wheel platforms listed above](#python).

Standalone `dar` binaries and container images are available from
[GitHub Releases](https://github.com/mag1cfrog/delta-arrow-reader/releases).
Choose a version whose CLI section reports successful download verification.

<details markdown="1">
<summary id="install-cli-with-uv">Install the command with uv</summary>

[Install uv](https://docs.astral.sh/uv/getting-started/installation/) if needed,
then run:

```console
uv tool install --no-build delta-arrow-reader-cli
dar --version
```

If uv reports that its executable directory is missing from `PATH`, run
`uv tool update-shell` and restart your shell.

</details>

<details markdown="1">
<summary id="install-cli-with-pipx">Install the command with pipx</summary>

[Install pipx](https://pipx.pypa.io/stable/how-to/install-pipx.html) if needed,
then install the command:

```console
pipx install delta-arrow-reader-cli
```

If your terminal cannot find `dar`, run `pipx ensurepath` and
restart the terminal. Preview a column from your table:

```console
dar preview /path/to/delta-table --column id --limit 3
```

Replace the path and `id` with your table directory and column name.
Continue with the [CLI quickstart](cli-quickstart.md) to run SQL.

For a single run:

```console
pipx run --spec delta-arrow-reader-cli dar --help
```

</details>

<details markdown="1">
<summary>Upgrade from the combined Python and CLI package</summary>

Version 0.8.0 and earlier bundled the CLI with the Python library. If you use uv
tools, replace that installation with the separate CLI package:

```console
uv tool uninstall delta-arrow-reader
uv tool install --no-build delta-arrow-reader-cli
```

For a pipx installation:

```console
pipx uninstall delta-arrow-reader
pipx install delta-arrow-reader-cli
```

For a pip environment that needs both interfaces, upgrade both packages:

```console
python -m pip install --upgrade --only-binary=:all: delta-arrow-reader
python -m pip install --upgrade --only-binary=:all: delta-arrow-reader-cli
dar --version
```

Projects that only use the Python API need just the first command. In scripts
that call the installed executable, replace `delta-arrow-reader` with `dar`.
Python imports and CLI arguments are unchanged.

For one-off runs, use `uvx --from delta-arrow-reader-cli dar --version` or
`pipx run --spec delta-arrow-reader-cli dar --version`. Replace `--version`
with your command's arguments.

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

<span id="standalone-linux-runtime"></span>

CLI releases provide validated archives for these platforms:

| Platform | Rust target | Minimum runtime |
| --- | --- | --- |
| Linux amd64 | `x86_64-unknown-linux-gnu` | glibc 2.36, baseline x86-64 CPU |
| Linux arm64 | `aarch64-unknown-linux-gnu` | glibc 2.36, baseline ARMv8-A CPU |

Both targets are built on Debian 12. The same binaries are tested on Debian 12,
Ubuntu 24.04, and Fedora 44. They use glibc and libgcc shared libraries. On Debian
or Ubuntu, install `libc6`, `libgcc-s1`, and `ca-certificates`; on Fedora, install
`glibc`, `libgcc`, and `ca-certificates`. The certificate bundle is needed for
HTTPS storage. The archives do not support Alpine Linux's musl runtime or a
`scratch` container. Other platforms can build from source.

The binaries do not need Rust or Python. macOS and Windows users can use
[CLI wheels](#install-cli-with-uv) or a [source build](#build-cli-from-source).

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
[container usage](https://mag1cfrog.github.io/delta-arrow-reader/cli-integration/#run-in-a-container)
for mounting a table or integrating the CLI with a Go service.

</details>

<details markdown="1">
<summary id="build-cli-from-source">Build the CLI from source</summary>

Build from source to use unreleased features or a platform without a published
package. Install [Git](https://git-scm.com/install/),
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

To install `dar` into Cargo's binary directory, usually `~/.cargo/bin`, run:

```sh
cargo install --locked --path crates/delta-arrow-reader-cli
```

The CLI crate is not published to crates.io.

</details>

## Rust

Use Rust 1.94 or newer. Run the commands below from your Cargo project.
The examples use your application's Tokio runtime.

### Streaming reader

```bash
cargo add delta-arrow-reader@0.8
cargo add tokio --features macros,rt-multi-thread
```

Continue with the [Rust quickstart](https://mag1cfrog.github.io/delta-arrow-reader/streaming-reader/)
to read your first batch. It uses `next_batch()`, so no separate stream utility
dependency is needed.

### DataFusion adapter

To query the table with SQL in Rust:

```bash
cargo add delta-arrow-reader@0.8 --features datafusion
cargo add datafusion@54 --no-default-features --features sql
cargo add tokio --features macros,rt-multi-thread
```

The adapter uses DataFusion 54. Your direct DataFusion dependency must use the
same major version. Continue with the [DataFusion quickstart](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/).

<details markdown="1">
<summary id="optional-cargo-feature">Optional Cargo feature</summary>

The streaming API and both Parquet backends are always available. The optional
`datafusion` feature adds SQL integration. The direct Parquet backend is the
default; choosing the Delta Kernel backend does not require another feature.
See [execution options](https://mag1cfrog.github.io/delta-arrow-reader/reference/execution-options/)
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
