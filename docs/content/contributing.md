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

## Run scan scheduling benchmarks

Build the harness, then pass the executable path printed by Cargo to the runner:

```console
cargo bench --locked --bench scan_scheduling --no-run
cargo xtask scan-scheduling --binary baseline=/path/to/scan_scheduling \
  --output-dir /tmp/scan-scheduling-results
```

The output directory must not exist. To compare versions, add
`--binary candidate=/path/to/candidate --fixture-binary candidate`. The named
fixture writer must support all selected shapes; every binary reads the same
fixtures. Use `--suite performance` or `--suite progress` to select a suite, and
`--case-filter` with a Rust regular expression to select case names.

The runner saves executable copies and source snapshots, records hashes in
`metadata.json`, writes individual runs to `runs.jsonl`, and updates measured
summaries in `summary.csv`. Each measurement starts a fresh process. Execution
timers exclude startup, fixture generation, table loading and planning; CPU and
peak RSS include setup and the
controlled HTTP server. Performance cases get one warmup per binary. Progress
timeouts are recorded; a failed performance control stops the run and retains
the results collected so far. Any failure in a measured group suppresses its
performance medians. Sampled task and batch counters are observations, not strict
resource bounds.

Metadata schema 2 identifies the Rust runner and its executable. It preserves
counterbalanced binary orders but uses `sha256-seed-case-v1` for case ordering:
sort case names by the SHA256 of `122 + zero_based_repetition`, a colon, and the
case name. This differs from the retired Python shuffle. Compare binaries within
the same campaign, and retain the ordering metadata when comparing campaigns.

## Test the CLI

Run the CLI checks from the repository root on Linux, with a C compiler and
the `openssl` command available for local HTTPS tests. Set `cli_version` to the
workspace version in `Cargo.toml`:

```console
cli_version=X.Y.Z
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
Linux process controls; no cloud credentials are needed. The CLI Source job runs
formatting, Clippy, and unit tests on relevant feature PRs. The process and scan
acceptance suites run after approval on release-plz PRs or through a manual
release validation run. See the
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
rustup component add rust-docs
cargo install --locked --features cli --version 0.9.2 cargo-about
python -m pip install 'build>=1.2,<2'
python -m build --wheel --outdir target/python-wheels crates/delta-arrow-reader-python
python -m pip install --force-reinstall target/python-wheels/*.whl
delta-arrow-reader --version
python -I -m unittest discover -s crates/delta-arrow-reader-python/tests
cargo test --locked -p delta-arrow-reader-python
```

The isolated Python check imports the installed wheel without adding the current
directory to its import path. Default Cargo commands still select the core crate;
use `cargo check -p delta-arrow-reader-python` to check the binding crate directly.

Use the Rust version declared in the workspace manifest, including its latest
patch release. Source builds need `curl`, `cargo-about` at the version declared
under `workspace.metadata.cli-release`, and that toolchain's `rust-docs` component
to collect dependency notices. Installing a completed wheel needs none of these
build tools.

The backend builds the native `delta-arrow-reader` command and dependency notices
before Maturin assembles the wheel. Both native components use the same Cargo
profile and target. Normal builds use `release`; editable installs use `dev`:

```console
python -m pip install --editable crates/delta-arrow-reader-python
```

Reinstall after changing Rust code. Build settings can be passed through pip's
`--config-settings` or build's `--config-setting`, for example
`--config-setting 'build-args=--profile dev --jobs 4'`. Supported Maturin arguments
are `--profile` (`dev` or `release`), `--target`, `--target-dir`, `--jobs`,
`--interpreter`, `--compatibility`, `--auditwheel`, `--strip`, `--locked`,
`--offline`, and `--frozen`. Cargo network restrictions do not disable notice
downloads. Other build arguments are rejected. Native source builds support the
same three targets as the release wheels; cross-compilation is not supported.
macOS defaults to deployment target 12.0, with `MACOSX_DEPLOYMENT_TARGET` applying
to both components.

Use the backend commands above for local builds. Direct `maturin build` and
`maturin develop` bypass native preparation. The release workflow calls
`cargo xtask build-python-wheel --prepare-only` before its Maturin action so it
can retain the pinned manylinux container and existing wheel repair steps.

To check a wheel's filename and metadata before installation, run `cargo xtask`
from the repository root with the expected version and platform tag:

```console
cargo xtask check-python-wheel \
  /path/to/delta_arrow_reader-X.Y.Z-cp310-abi3-manylinux_2_28_x86_64.whl \
  --expected-version X.Y.Z --platform-tag manylinux_2_28_x86_64
```

Use the version and platform tag required by your build. The check compares the
filename with the wheel's `METADATA` and `WHEEL` records, including the expected
package name, version, and `cp310-abi3` tag. It also requires the native CLI and
dependency notices, and verifies the wheel's `RECORD` hashes. The installed
checks below exercise both native components.

For the installed-wheel acceptance checks, run the following from a directory
outside the checkout, using the environment where the wheel is installed.
Replace the test directory with its absolute path:

```console
python -I -m unittest discover \
  -s /absolute/path/to/delta-arrow-reader/crates/delta-arrow-reader-python/tests \
  -k installed
```

This selects package metadata, native CLI, SQL, and local Arrow stream checks.
The stream check covers both Python entrypoints and both Parquet backends,
selects columns and filters rows, and closes the reader before consuming all results.
It uses the checked-in Spark fixture and checks batch values after closing the
reader. These checks also run in the full Python suite.
The CLI checks resolve the executable from that Python environment's scripts
directory. They cover help, the exact package version, snapshot inspection,
projection and row limits, and a complete Arrow IPC stream with matching values.
They also check redacted errors, empty stdout on input failures, status 3 for a
closed stdout pipe, and termination of a scan blocked on stdout. Unix checks
exercise SIGINT and SIGTERM; Windows checks terminate the process. Each child
has a timeout and runs outside the checkout without build-tool library paths.

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
Python test fixtures, workspace manifests, package metadata, CLI source, and
native packaging tools trigger these checks. The generated Python API file under
`crates/delta-arrow-reader-core/docs/python_api/` also triggers them. Unrelated
release tools, benchmarks, and `docs/` skip them.

The job runs Clippy, the Rust runtime ownership tests, and the full Python suite
on Linux with Python 3.10 and PyArrow 18.0.0. It uses the PEP 517 backend's editable
install to build both the extension and CLI in a virtual environment. Feature pull requests do not
build the three release wheels.

The same installed bindings also check the generated Python API documentation.
After changing Rust binding comments or Python signatures, follow the
[regeneration instructions](../README.md#python-api-reference) and include the
updated generated file in the pull request.

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

The three platform jobs run in parallel. The build fails if the checked-out
commit or package version differs from these inputs. Both the Python extension
and native CLI use the repository's Rust version, lockfile, target, and release
profile. Linux builds both from an extracted sdist inside the pinned
manylinux_2_28 container. Do not copy a standalone Debian-built `dar` into a
wheel: that binary's glibc 2.36 requirement exceeds the wheel's glibc 2.28 baseline.

Maturin on Linux, delvewheel on Windows, or delocate on macOS repairs the wheel.
The workflow checks the final wheel's metadata and extension dependencies, then
installs it outside the checkout on Python 3.10 with PyArrow 18.0.0 and Python
3.14 with the current compatible PyArrow release. Linux runs the full Python
suite; Windows and macOS run all the installed-package, Python stream, SQL, and CLI
checks described above.

Wheel repair does not replace the separate audit of the CLI in `.data/scripts`.
`cargo xtask audit-python-cli` checks the installed executable from the final
wheel. Linux requires x86_64 ELF, permitted runtime libraries, and GLIBC symbol
versions no newer than 2.28. macOS requires an arm64 executable with a deployment
target no newer than 12.0, system library paths, and no rpath. Windows requires
an x86_64 executable whose normal and delay-loaded imports use only supported
system and CPython/MSVC runtime DLLs. To repeat the audit locally, use the
installed executable's absolute path and the wheel's platform tag:

```console
cargo xtask audit-python-cli /absolute/path/to/venv/bin/delta-arrow-reader \
  --platform-tag manylinux_2_28_x86_64
```

Linux repeats the installed checks with both Python versions in a fresh
glibc 2.28 container. Only the final wheel, tests, and fixture are mounted into
that container. CLI subprocesses have no `LD_*` or `DYLD_*` overrides and a
restricted `PATH`; Windows keeps the system and CPython runtime directories.

Each platform also installs the same final wheel through `uv tool install` and
an isolated `uv tool run` environment, the command behind `uvx`. Both environments
run the installed acceptance checks on Python 3.14, and both CLI commands
must report the exact package version. These checks disable source builds and
use temporary uv tool and cache directories. No repair, repackaging, or rebuild
occurs between acceptance and artifact upload.

After relevant main pushes, `.github/workflows/cache-warm.yml` warms core test
caches, including the Python runtime tests on Linux. Core cache warming runs one
platform at a time to limit sccache upload bursts. A failed cache write gets one
fresh build after a 60-second wait, reusing entries already uploaded. Persistent
write errors fail the warmer, except when another build has uploaded the same
immutable entry.

`Warm release build caches` runs when a release commit reaches main, or when
started manually on main. It calls the CLI and wheel builders with
`cache-only: true`, building both CLI architectures and all three wheel platforms
in parallel. This mode accepts only the triggering main commit, saves Cargo
registry and compiled dependency caches, and skips release acceptance and artifact
uploads. It never reports a validated artifact set. Future release PRs can read
these main caches; release publication still uses the artifacts validated before
merge. Manual runs of `Warm CI caches` also warm wheel dependencies.

Release PRs can save their own dependency caches for reruns. Linux wheel builds
keep host tools, container tools, and wheel outputs in separate directories so
Maturin's cleanup preserves the tool caches. macOS wheel builds share Maturin's
dependency linker flags; the CLI build script still rejects unresolved symbols
when linking the executable. Both wheel build stages remove xtask's inherited
Cargo package metadata so it does not invalidate dependency caches. Different
dependency features can still require separate compilations.

Cache pruning retains the most recently used main cache for each build family and
platform, and removes managed caches belonging to closed PRs. Open PR caches and
unrelated cache families are preserved.

The cache helpers are Rust commands in the workspace's private `xtask` crate:
`cargo xtask check-cache-writes` and `cargo xtask prune-ci-caches`.
Run its CLI, cache, and Python release tests with
`cargo test --locked --package xtask`.
Cache pruning requires `gh`, `GH_TOKEN`, and `GH_REPO`;
the pruning jobs have `actions: write` and `pull-requests: read` permission.

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

To populate core caches after a toolchain or workflow change, run `Warm CI caches`
manually on main. Use `Warm release build caches` for CLI and wheel dependencies.
The first build for new compiler inputs can still be cold.

Each platform job uploads its final wheel only after the Python, CLI, native
dependency, and uv checks pass:

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
and checks each wheel's version, tags, CLI payload, notices, and `RECORD` again.
A missing CLI fails validation even when the wheel and artifact hashes match.
For release PRs, it saves the complete set as
`python-wheels-<git-tree>-<run-id>-<run-attempt>`, preserving
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
same SHA256 again. Production uploads reuse these exact wheel bytes without
rebuilding or repeating the installed acceptance suites. Missing, expired, or
mismatched bundles stop publication. These commands require `gh` and `GH_TOKEN`;
the preparation job has read access to Actions and
pull requests, and the PyPI upload job has read access to Actions.

Download artifacts from the Actions run page. To download and check the full
set locally, run these commands from the repository root with the expected
package version for that run:

```console
gh run download RUN_ID --pattern 'delta-arrow-reader-*-wheel' --dir wheel-artifacts
cargo xtask check-python-artifacts wheel-artifacts --expected-version X.Y.Z
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

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then build or
serve the site from the repository root. uv prepares the environment automatically
using the versions in `docs/uv.lock`:

```console
uv run --locked --project docs zensical build --strict -f crates/delta-arrow-reader-core/docs/mkdocs.yml
uv run --locked --project docs zensical serve -f crates/delta-arrow-reader-core/docs/mkdocs.yml
```
