# Development

This page is for contributors working on Delta Arrow Reader itself. Applications
that use the crate can start with the [installation guide](../../crates/delta-arrow-reader-core/docs/public/installation.md).

## Repository layout

The root `Cargo.toml` defines the workspace. Each product lives under `crates/`:

- `delta-arrow-reader-core/` contains the Rust library, its tests, Rust benchmarks,
  and public documentation.
- `delta-arrow-reader-cli/` contains the `dar` command and its release tooling.
- `delta-arrow-reader-python/` contains the Python extension, `pyproject.toml`,
  type stub, and Python tests.

The core crate is published as `delta-arrow-reader` and imported in Rust as
`delta_arrow_reader`.

Shared benchmark tools remain in `benches/`, contributor documentation in `docs/`,
and repository automation in `xtask/`.
Run the commands below from the repository root.

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

Run the CLI checks from the repository root on Linux, with a C compiler and
OpenSSL available. Set `cli_version` to the workspace version in `Cargo.toml`:

```console
cli_version=0.6.2
cargo build --locked -p delta-arrow-reader-cli
cargo test --locked -p delta-arrow-reader-cli
cargo clippy --locked -p delta-arrow-reader-cli --all-targets -- -D warnings
cargo xtask cli test-process target/debug/dar --expected-version "$cli_version" --repository .
cargo xtask cli test-scan target/debug/dar --repository .
```

The Cargo tests compare IPC schemas and rows with the Rust core and check
argument validation and process errors. The xtask suites read the IPC output and
exercise backpressure, partial results, broken pipes, signals, HTTPS, and shutdown
with pending blocking reads. They use checked-in fixtures, local servers, and
Linux process controls; no cloud credentials are needed. These CLI checks run
after approval on release-plz PRs or through a manual release validation run.
Feature PRs skip the CLI builds and acceptance suites. See the
[CLI release tools](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-cli/release/README.md#validate-a-cli-release)
for approval and artifact details.

### Stage the CLI for Python wheel packaging

Run `stage-python-cli` from the repository root using the workspace Rust
toolchain. This prepares native wheel data for the packager; it does not build a
wheel. For a local Linux development build:

```console
cargo +1.94.1 xtask stage-python-cli \
  --target x86_64-unknown-linux-gnu --profile dev \
  --destination target/python-cli
target/python-cli/scripts/delta-arrow-reader --version
```

`--destination` and `--target` are required. The profile is `release` by default;
use `--profile dev` for a faster local build. The three supported native targets
and their output paths are:

| Rust target | Path under `--destination` |
| --- | --- |
| `x86_64-unknown-linux-gnu` | `scripts/delta-arrow-reader` |
| `aarch64-apple-darwin` | `scripts/delta-arrow-reader` |
| `x86_64-pc-windows-msvc` | `scripts/delta-arrow-reader.exe` |

The task builds the current checkout's `dar` target with `--locked`, inherits
Cargo cache and compiler settings, and reads the executable path from Cargo's
build output. It rejects cross compilation and checks the renamed executable's
version before placing it in `scripts/`. Build diagnostics go to stderr.

The two executable paths in the table are reserved for this task. Every run
removes old files at both paths before building, so a failed build cannot leave
an older payload ready for packaging. Other files are left in place. Destination
and `scripts/` directory symlinks are rejected. Use a separate destination for
each concurrent build, under the ignored `target/` directory or outside the
checkout. Its parent must be writable and on the same filesystem. Verification
uses a temporary sibling directory, so an interrupted run cannot leave temporary
files inside wheel data. After verification, an atomic rename places the
executable in `scripts/`.

Generated executables must not be tracked or included in a source archive. The
packager must require a successful staging run before consuming this wheel data
directory.

Release wheel builds must run staging inside the wheel build environment:
manylinux 2.28 on Linux, macOS arm64 with `MACOSX_DEPLOYMENT_TARGET=12.0`, or
Windows x86_64 MSVC. A successful build on a newer Linux host does not establish
manylinux compatibility. Standalone GitHub/GHCR binaries keep their separate
Debian 12 build environment.

The focused tests run without building the reader. To also build and exercise
the native Linux CLI against a checked-in table from outside the checkout, run:

```console
cargo +1.94.1 test --locked --package xtask --bin xtask python_cli::
cargo +1.94.1 test --locked --package xtask --bin xtask \
  python_cli::tests::native_cli_stages_and_reads_a_fixture_outside_the_checkout \
  -- --ignored --exact
```

## Build the Python package locally

For source installation and a table reading example, see the
[Python installation guide](../../crates/delta-arrow-reader-core/docs/public/installation.md#python).

To build and test the wheel directly, run these commands from the repository
root on Linux or macOS:

```console
python3 -m venv target/python-venv
. target/python-venv/bin/activate
python -m pip install 'maturin>=1.14,<2'
maturin build --locked --manifest-path crates/delta-arrow-reader-python/Cargo.toml --out target/python-wheels
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

Python packaging configuration and the type stub live in
`crates/delta-arrow-reader-python/`. Package the binding, its local Rust
dependencies, native build tools, and the workspace lockfile:

```console
python -m pip install 'build>=1.2,<2'
python -m build --sdist --outdir target/python-sdist crates/delta-arrow-reader-python
```

Track new source files before packaging.

The PEP 517 backend delegates package metadata and source selection to Maturin,
then calls `cargo xtask complete-python-sdist` to add the CLI and xtask sources.
The archive retains the original workspace manifest and lockfile, so an extracted
archive builds with `--locked` without pruning dependencies. Building the archive
requires Rust and Cargo, but does not build the native CLI. Use the command above
to include these build tools; invoking `maturin sdist` directly bypasses the backend.

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

## Python checks in pull requests

Pull request CI runs the `Python Package` job for changes covered by
the Python filter in `.github/path-filters.json`. Shared Rust code, binding code,
Python test fixtures, workspace manifests, and package metadata trigger these
checks. Changes confined to CLI implementation, benchmarks, or `docs/` skip them.

The job runs Clippy, the Rust runtime ownership tests, and the full Python suite
on Linux with Python 3.10 and PyArrow 18.0.0. It uses `maturin develop --locked`
with the binding crate's manifest to install a debug build in a virtual
environment. Feature pull requests do not
build the three release wheels.

Release-plz pull requests targeting main also require three-platform wheel
validation. CI identifies them by the release bot's identity and a
`release-plz-` branch in this repository. `Approve release checks` waits for
approval through the `release-checks` environment. One approval starts the
Python wheel and CLI checks in parallel. The required `CI` check succeeds only
after approval and complete validation of both artifact sets, in addition to
the usual checks. Feature PRs skip these release jobs. A new PR revision cancels
the previous run and requires a new approval.

## Build Python wheels in CI

Release PR checks and manual build or TestPyPI runs of `Python release` call
`.github/workflows/pypi-build.yml` to build Linux x86_64, Windows x86_64, and macOS
arm64 wheels. PyPI publication reuses the release PR's validated wheels after
checking that their source tree matches the release tag. To check packaging
manually before a release, follow
[Build without uploading](python-releases.md#build-without-uploading).
Callers of the reusable workflow supply two required inputs:

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
container.

After relevant main pushes, `.github/workflows/cache-warm.yml` warms core test
caches, including the Python runtime tests on Linux, and retains the two most
recently used Windows core target caches. PR builds only read the caches.
Only manual runs of `Warm CI caches` also call the wheel workflow with
`cache-only: true`. This mode accepts only the triggering main commit, writes
compiler and Cargo registry caches, and runs the wheel checks. It does not upload
artifacts or report a validated artifact set. Wheel builds cache the Cargo registry
without restoring core target directories.
Core and wheel cache warming each run one platform at a time to reduce cache
upload bursts. A failed cache write gets one fresh build after a 60-second wait,
reusing entries that were already uploaded. Persistent cache write errors
fail the warmer, except when another build has uploaded the same immutable entry.

The cache helpers are Rust commands in the workspace's private `xtask` crate:
`cargo xtask check-cache-writes` and `cargo xtask prune-ci-caches`.
Run its CLI, cache, and Python release tests with
`cargo test --locked --package xtask`.
Cache pruning requires `gh`, `GH_TOKEN`, and `GH_REPO`;
the pruning job alone has `actions: write` permission.

The `xtask` tests also cover Python release validation and workflow gates.
The release planning tests require `release-plz` 0.3.171 on `PATH` and are
ignored by default. After installing that version, run the PR CI task tests with:

```sh
cargo test --locked --package xtask --bin xtask cache_tests::
cargo test --locked --package xtask \
  --test ci --test cli_workflow --test python_release --test python_upload --test python_wheels \
  --test python_wheel_reuse --test python_workflow --test release_plz -- --include-ignored
```

These PR checks cover workflow behavior, Python, and cache tasks. On release-plz
PRs, the shared `release-checks` approval also starts
`.github/workflows/cli-build.yml`. The `CI` check passes only after CLI unit
tests, release-tool tests, and archive acceptance succeed. Feature PRs skip those
release jobs. The builder's native build test runs in Debian 12 on both supported
architectures, and its archive tests run in parallel on Debian 12, Ubuntu 24.04,
and Fedora 44. **Build CLI binaries** also supports manual runs with an explicit
commit and version. The Python release planning tests use temporary repositories
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
and checks each wheel's version and tags again. For release PRs, it then saves
the complete set as `python-wheels-<git-tree>-<run-id>-<run-attempt>`, preserving
the three platform directories. The Git tree identifies the checked-out files
even when a squash merge changes the commit SHA. This artifact is retained for
90 days, and reruns save a separate copy. A failed upload fails the check.
Only after these steps does the reusable workflow return `validated: 'true'`.
Callers must require both a successful workflow result and this output before
treating the artifacts as a complete validated set. A failed run may still
contain individual platform artifacts.

PyPI preparation uses `cargo xtask reuse-python-wheels` to locate the merged
release PR and requires its latest CI run to have succeeded. It requires a bundle
for the same Git tree, verifies the archive's SHA256 from GitHub, and checks all
three wheels. The upload job downloads that exact artifact ID and verifies the
same SHA256 again. Missing, expired, or mismatched bundles stop publication. These commands
require `gh` and `GH_TOKEN`; the preparation job has read access to Actions and
pull requests, and the PyPI upload job has read access to Actions.

Download artifacts from the Actions run page. To download and check the full
set locally, run these commands from the repository root with the expected
package version for that run:

```console
gh run download RUN_ID --pattern 'delta-arrow-reader-*-wheel' --dir wheel-artifacts
cargo xtask check-python-artifacts wheel-artifacts --expected-version 0.6.2
```

The reusable wheel workflow produces build artifacts only. For publisher setup,
TestPyPI rehearsal, stable releases, and upload recovery, see
[Publish Python wheels](python-releases.md).

## Work on the documentation

The Markdown files in `crates/delta-arrow-reader-core/docs/public/` are the source
for the documentation site. The guides listed in
`crates/delta-arrow-reader-core/src/guides.rs` are also included in the generated Rust
documentation.

Use absolute links between shared pages. Mark runnable Rust examples as
`no_run`, and mark incomplete snippets as `ignore`, so both documentation
renderers handle them correctly.

Install the documentation dependencies, then build or serve the site:

```console
python -m pip install -r docs/requirements.txt
python -m zensical build --strict -f crates/delta-arrow-reader-core/docs/mkdocs.yml
python -m zensical serve -f crates/delta-arrow-reader-core/docs/mkdocs.yml
```
