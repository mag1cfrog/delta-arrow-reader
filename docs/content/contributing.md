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
