# Development

This page is for contributors working on Delta Arrow Reader itself. Applications
that use the crate can start with the [installation guide](../public/installation.md).

## Run the local checks

The repository CI tests every supported feature combination. Run these focused
checks before opening a pull request:

```console
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
cargo package --locked
```

## Test the CLI

Run the CLI checks from the repository root on Linux, with Python 3.12 or
newer and a C compiler available:

```console
cargo build --locked -p delta-arrow-reader-cli
cargo test --locked -p delta-arrow-reader-cli
cargo clippy --locked -p delta-arrow-reader-cli --all-targets -- -D warnings
python3 -m venv target/cli-test-venv
. target/cli-test-venv/bin/activate
python -m pip install --only-binary=:all: 'pyarrow==25.0.1'
python crates/delta-arrow-reader-cli/tests/scan.py target/debug/dar
```

The Cargo tests compare IPC schemas and rows with the Rust core and check
argument validation and process errors. The PyArrow suite independently reads
the output and exercises backpressure, partial results, broken pipes, signals,
and shutdown with pending blocking reads. It uses checked-in fixtures, a local
HTTP server, and Linux process controls; no cloud credentials are needed.

## Build the Python package locally

For source installation and a table reading example, see the
[Python installation guide](../public/installation.md#python).

To build and test the wheel directly, run these commands from the repository
root on Linux or macOS:

```console
python3 -m venv target/python-venv
. target/python-venv/bin/activate
python -m pip install 'maturin>=1.14,<2'
maturin build --locked --out target/python-wheels
python -m pip install --force-reinstall target/python-wheels/*.whl
python -I -m unittest discover -s crates/delta-arrow-reader-python/tests
cargo test --locked -p delta-arrow-reader-python
```

The isolated Python check imports the installed wheel without adding the current
directory to its import path. Default Cargo commands still select the core crate;
use `cargo check -p delta-arrow-reader-python` to check the binding crate directly.

To check a wheel's filename and metadata before installation, run `cargo xtask`
from the repository root with the expected version and platform tag:

```console
cargo xtask check-python-wheel \
  /path/to/delta_arrow_reader-0.6.2-cp310-abi3-manylinux_2_28_x86_64.whl \
  --expected-version 0.6.2 --platform-tag manylinux_2_28_x86_64
```

Use the version and platform tag required by your build. The check compares the
filename with the wheel's `METADATA` and `WHEEL` records, including the expected
package name, version, and `cp310-abi3` tag. The installed-package check below
covers the native module, type files, licenses, and dependencies.

For a small installed-wheel smoke check, run the following from a directory
outside the checkout, using the environment where the wheel is installed.
Replace the test directory with its absolute path:

```console
python -I -m unittest discover \
  -s /absolute/path/to/delta-arrow-reader/crates/delta-arrow-reader-python/tests \
  -k installed
```

This selects the package metadata check and a local Arrow stream read.
The stream check covers both Python entrypoints and both Parquet backends,
selects columns and filters rows, and closes the reader before consuming all results.
It uses the checked-in Spark fixture and checks batch values after closing the
reader. These checks also run in the full Python suite.

Python packaging configuration and the type stub live at the repository root.
Use `maturin sdist --out target/python-sdist` to package Git-tracked workspace
files with their matching lockfile. Track new source files before packaging.

The binding's runtime owner lives in `crates/delta-arrow-reader-python/src/runtime.rs`.
Table loading, scan planning, and batch reads use its `wait` method, which releases
the GIL during bounded waits and checks Python signals on the calling thread.
An interruption drops the pending future; synchronous
Kernel work already running may still finish. The last runtime owner starts
shutdown without waiting for that blocking work.

The Rust tests cover retained ownership and shutdown from a runtime worker.
On POSIX systems, the Python runtime tests delay local HTTP responses during
loading, planning, and batch reads in subprocesses. They send SIGINT and check
for `KeyboardInterrupt` during loading or planning, a terminal PyArrow exception
during a batch read, and clean process exit. The HTTP handlers also verify that
another Python thread can run during these waits.

## Build Python wheels in CI

Pull request CI calls `.github/workflows/pypi-build.yml` for changes covered by
the Python filter in `.github/path-filters.json`. Shared Rust code, binding code,
Python test fixtures, workspace manifests, and package metadata trigger these
checks. Changes confined to CLI implementation, benchmarks, or `docs/` skip them.
The reusable workflow builds Linux x86_64, Windows x86_64, and macOS arm64
wheels. Callers supply two required inputs:

- `source-ref`: the full commit SHA to check out and build.
- `expected-version`: the exact workspace package version, without a tag prefix.

The build fails if the checked-out commit or package version differs from these
inputs. It uses the repository's Rust version and lockfile, repairs the wheel
with Maturin on Linux, delvewheel on Windows, or delocate on macOS, and checks
its metadata and native dependencies. Each wheel is installed outside the
checkout on Python 3.10 with PyArrow 18.0.0 and Python 3.14 with the current
compatible PyArrow release. Linux runs the full Python suite; Windows and macOS
run the installed-package and Arrow stream smoke checks described above.

The Linux wheel is built from an extracted sdist inside a pinned manylinux
container. The separate `Python Package` job runs Clippy and the Rust runtime
ownership tests.

`.github/workflows/cache-warm.yml` calls the same wheel workflow after relevant
main pushes with `cache-only: true`. This mode writes compiler and Cargo registry
caches and runs the wheel checks, but does not upload artifacts or report a
validated artifact set. It accepts only the triggering main commit from a push
or manual run. PR builds only read the caches. The warmer also compiles the
Python runtime tests and retains the two most recently used Windows core target
caches; wheel builds cache the Cargo registry without restoring core target
directories.
Core and wheel cache warming each run one platform at a time to reduce cache
upload bursts. A failed cache write gets one fresh build after a 60-second wait,
reusing entries that were already uploaded. Persistent cache write errors
fail the warmer, except when another build has uploaded the same immutable entry.

The cache helpers are Rust commands in the independent `xtask` crate:
`cargo xtask check-cache-writes` and `cargo xtask prune-ci-caches`.
Run their tests with
`cargo test --locked --manifest-path xtask/Cargo.toml`, without compiling the
reader's dependencies. Cache pruning requires `gh`, `GH_TOKEN`, and `GH_REPO`;
the pruning job alone has `actions: write` permission.

The `xtask` tests also cover Python release validation and workflow gates.
The release planning tests require `release-plz` 0.3.171 on `PATH` and are
ignored by default. After installing that version, run the full suite with
`cargo test --locked --manifest-path xtask/Cargo.toml -- --include-ignored`.
CI runs this full suite. The release planning tests use temporary repositories
and a local baseline without publishing packages.

To populate caches after a toolchain or workflow change, run `Warm CI caches`
manually on main. The first build for new compiler inputs can still be cold.

Each platform job uploads its artifact after both Python checks pass:

| Build environment | Artifact |
| --- | --- |
| Linux x86_64, manylinux_2_28 | `delta-arrow-reader-linux-x86_64-wheel` |
| Windows x86_64, Windows Server 2022 | `delta-arrow-reader-windows-x86_64-wheel` |
| macOS arm64, `macos-latest` runner | `delta-arrow-reader-macos-arm64-wheel` |

The macOS wheel targets macOS 12.0, matching the PyArrow 18.0.0 wheels.
Delocate checks that bundled libraries support the required architecture and
deployment target. Runtime checks run on the runners listed above, not on
macOS 12.0 itself.

The `Validate wheel artifact set` job requires all platform builds to pass,
then downloads the three artifacts from the same workflow run. It rejects
missing or unexpected artifacts, requires exactly one wheel per artifact,
and checks each wheel's version and tags again. Only then does the reusable
workflow return `validated: 'true'`. Callers must require both a successful
workflow result and this output before treating the artifacts as a complete
validated set. A failed run may still contain individual platform artifacts.

Download artifacts from the Actions run page. To download and check the full
set locally, run these commands from the repository root with the expected
package version for that run:

```console
gh run download RUN_ID --pattern 'delta-arrow-reader-*-wheel' --dir wheel-artifacts
cargo xtask check-python-artifacts wheel-artifacts --expected-version 0.6.2
```

The workflow produces build artifacts only. For publisher setup, TestPyPI
rehearsal, stable releases, and upload recovery, see
[Publish Python wheels](python-releases.md).

## Work on the documentation

The Markdown files in `docs/public/` are the source for the documentation
site. The guides listed in `src/guides.rs` are also included in the generated
Rust documentation.

Use absolute links between shared pages. Mark runnable Rust examples as
`no_run`, and mark incomplete snippets as `ignore`, so both documentation
renderers handle them correctly.

Install the documentation dependencies, then build or serve the site:

```console
python -m pip install -r docs/requirements.txt
python -m zensical build --strict -f docs/mkdocs.yml
python -m zensical serve -f docs/mkdocs.yml
```
