# CLI release tools

These tools build and validate the Linux CLI archives. Builds, ELF checks,
archive operations, dependency notice collection, and archive acceptance run
through `cargo xtask`, a Cargo alias for the repository's private Rust maintenance
crate. Process checks run in Rust through `xtask cli test-process`. Scan checks run
through `xtask cli test-scan`, which still requires Python 3.11 or newer and PyArrow
for the groups awaiting migration to Rust.

## Run the CI build

`.github/workflows/cli-build.yml` runs on native amd64 and arm64 runners. Both
platforms build and execute the archive's binary in the pinned Debian 12 image.
The workflow has read-only repository permissions and uploads Actions artifacts.
It does not publish releases or container images.

Call it from another workflow in this repository:

```yaml
jobs:
  cli:
    uses: ./.github/workflows/cli-build.yml
    with:
      source-ref: ${{ github.sha }}
```

`source-ref` must be a full lowercase commit SHA. The checkout must match it
exactly and contain these tools. The optional `expected-version` input requires
an exact workspace version, without `v`. A missing commit, a version mismatch,
or an unsupported historical checkout fails the build; there is no fallback to
`main`. The prepare job resolves the workspace's Rust version once, then both
platform jobs install and use that exact patch release.

For a manual run, select **Build CLI binaries** in Actions and supply the same
inputs. The successful job summary links the complete artifact set. PR CI calls
this workflow too. The required `CI` job checks its result and `validated` output;
a failed, skipped, or cancelled architecture cannot pass.

The reusable workflow returns:

| Output | Meaning |
| --- | --- |
| `validated` | `true` only after both platforms pass and the complete set is uploaded |
| `source-commit`, `version`, `rust-toolchain` | Resolved source SHA, workspace version, and exact Rust release |
| `artifact-id`, `artifact-name`, `artifact-url` | The complete set's immutable Actions ID, run-specific name, and download page |

Consumers must depend on the successful job and require `validated == 'true'`.
Within the same workflow run, download that exact artifact with
[`actions/download-artifact@v6`](https://github.com/actions/download-artifact/tree/v6),
using either `artifact-ids` or `name` from the outputs. Downloads across runs also
require `run-id` and `github-token`. Check out the returned source commit and run
`cargo xtask cli artifacts verify` with the expected version, source SHA, and Rust
release from the workflow outputs. Intermediate artifacts ending in `-amd64` or
`-arm64` are inputs to assembly, not complete sets for downstream consumers.

Actions retention settings apply. Missing, expired, incomplete, or mismatched
artifacts must stop downstream work. Rerun this builder for the explicit source
commit to obtain a new validated set and reference. Container and release jobs
must consume its existing archive bytes without rebuilding or repacking them.

## Build on Debian 12

`cargo xtask cli build` builds one architecture from an explicit, clean source
commit. Use the Debian 12 image returned by `cargo xtask cli prepare`, on the
matching architecture. The builder checks the OS, glibc version, and Rust compiler
host before compiling. It reads the workspace version and Rust toolchain from
`Cargo.toml`, uses the locked
release build, and supplies baseline CPU flags for Rust and C/C++ dependencies.
Compiler flags and wrapper environment variables supplied by the caller are
excluded from the build environment.

`prepare` requires the workspace's declared Rust toolchain to be installed. It
returns the source SHA, workspace version, exact installed Rust release, pinned
Debian image, and cargo-about version as JSON. Add `--expected-version X.Y.Z` to
require a specific workspace version. `build`, `build-archive`, and `check-binary`
also write their validated metadata as JSON; progress messages go to stderr.

For a local amd64 build, use a normal Git clone with the requested commit checked
out and no modified or untracked files. Install the workspace's Rust toolchain
and `jq` on the host. A Git worktree also needs its shared Git directory mounted
inside the container. This example uses rootless Podman:

```sh
cli_source_commit=FULL_SOURCE_COMMIT
cli_build_info=$(cargo xtask cli prepare --source-commit "$cli_source_commit")
cli_image=$(printf '%s\n' "$cli_build_info" | jq -r '.image')
cli_rust_toolchain=$(printf '%s\n' "$cli_build_info" | jq -r '.["rust-toolchain"]')
podman run --rm -it --platform linux/amd64 \
  --volume "$PWD:/source:z" --workdir /source \
  --env CLI_SOURCE_COMMIT="$cli_source_commit" \
  --env CLI_RUST_TOOLCHAIN="$cli_rust_toolchain" "$cli_image" bash
```

Inside the container, install the build prerequisites and the workspace's Rust
toolchain using [rustup](https://rust-lang.github.io/rustup/installation/index.html):

```sh
set -eu
apt-get update
apt-get install -y --no-install-recommends \
  build-essential binutils ca-certificates curl git pkg-config
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain "$CLI_RUST_TOOLCHAIN"
. "$HOME/.cargo/env"
cargo xtask cli build --source-commit "$CLI_SOURCE_COMMIT" \
  --architecture amd64 --rust-toolchain "$CLI_RUST_TOOLCHAIN"
```

For arm64, use `--platform linux/arm64` and `--architecture arm64`, with a native
ARM64 runner or configured emulation. Cross-compilation alone does not satisfy
the execution check.

The executable is `target/cli-build/<rust-target>/release/dar`. The adjacent
`target/cli-build/<rust-target>/build-info.json` records the source commit, exact
Rust release, binary hash, ELF interpreter, required shared libraries, highest
GLIBC symbol version, and required runtime packages. `--target-dir` selects a
different Cargo build directory; keep it outside the checkout or under an
ignored path. Keep compiler caches out of uploaded artifacts.

The ELF check rejects an unexpected architecture or loader, RPATH/RUNPATH,
unsupported dynamic libraries, GLIBC requirements above 2.36, and x86 ISA notes
that require more than the baseline CPU. It then runs `dar --version` and checks
the binary hash before and after execution.

## Check the runtime independently

Use a fresh container from the same pinned image, with only `dar`, test inputs,
and the runtime packages `ca-certificates`, `libc6`, and `libgcc-s1`. Run version,
inspection, and scan checks there before installing test instrumentation. The
binary is dynamically linked to glibc and libgcc; this does not support Alpine
or a `scratch` image.

For an explicit ELF and version check, install `binutils` in the test container
and mount the Debian-built xtask executable read-only at `/tools/xtask`.
Pass the version and hash from the earlier validated build:

```sh
/tools/xtask cli check-binary /path/to/dar \
  --architecture amd64 \
  --expected-version "$version" --expected-sha256 "$dar_sha256"
```

This command requires Debian 12 and the matching CPU architecture. It reports
the observed ELF metadata as JSON. A successful version check still requires
the scan, process lifecycle, and HTTPS acceptance tests before release.

The existing CLI test harnesses accept an executable path. With the test
prerequisites installed (`build-essential`, OpenSSL, Python, and the PyArrow
version used by CLI CI), run them against that same binary:

```sh
cargo xtask cli test-process /path/to/dar --expected-version "$version" --repository .
cargo xtask cli test-scan /path/to/dar --repository .
```

`test-process` requires Linux. It checks help, version, inspection, historical
snapshots, diagnostics, thread failures, and input validation in Rust, including
JSON size limits and terminal output rejection. Rust also covers predicate and
projection planning, duplicate columns, stdout write failures, HTTP/S3 access, and
signal handling. These checks need a C compiler for the fault library. Each test
uses an isolated temporary directory, and each CLI invocation has a timeout. CI
runs this command separately from `cargo test -p delta-arrow-reader-cli`.

`test-scan` also requires Linux. Rust checks IPC output against Spark fixtures,
including column projection, row limits, nested schemas, deletion vectors,
filtering, execution options, and empty tables. Rust also checks predicates on
adjacent Float64 values, scan thread creation failures, and HTTPS trust for both
inspection and scanning. Read failure checks verify that a scan can return
decodable partial batches while exiting unsuccessfully without an IPC end marker.
The thread checks compile a C fault library with `cc`.
The HTTPS checks use OpenSSL to generate temporary certificates and configure
trust only for the CLI subprocess. The command then runs the remaining
`scan.py` groups using `python3` from `PATH`. Those groups still need PyArrow
and a C compiler.

In a runtime container without Cargo, use `/tools/xtask cli test-process` and
`/tools/xtask cli test-scan` with `--repository /checks`. The source checkout
supplies fixtures, the remaining Python checks, and the C fault library.

## Build and test a complete platform archive locally

After installing the build prerequisites and cargo-about described here, run
`cargo xtask cli build-archive` from the clean source checkout inside the pinned
build container. This compiles the CLI, checks its identity and ELF metadata, collects
notices, and packages the validated binary:

```sh
rustup toolchain install "$rust_toolchain" --profile minimal
cargo run --locked --package xtask --target-dir target/cli-build -- cli build-archive \
  --source-commit "$source_commit" --architecture amd64 \
  --version "$version" --rust-toolchain "$rust_toolchain" \
  --target-dir target/cli-build --output target/cli-inputs/amd64
```

Set these variables to the full source SHA, workspace version, and installed
exact Rust release, such as `1.94.1`. Use the same values for both architectures.
The command places xtask itself and the CLI build under `target/cli-build`.
The output directory must not exist. Before execution, extract the validated
platform archive to a new directory outside the checkout:

```sh
target/cli-build/debug/xtask cli artifacts extract target/cli-inputs/amd64 \
  --destination /tmp/dar-amd64-check --architecture amd64 \
  --version "$version" --source-commit "$source_commit" --rust-toolchain "$rust_toolchain"
```

Start a fresh pinned Debian container on the matching architecture. Use read-only
mounts for the source checkout at `/checks`, `target/cli-inputs/amd64` at
`/artifacts`, `/tmp/dar-amd64-check` at `/package`, and the Debian-built
`target/cli-build/debug/xtask` at `/tools/xtask`. Pass `CLI_ARCHITECTURE=amd64`,
`CLI_SOURCE_COMMIT`, `CLI_VERSION`, and `CLI_RUST_TOOLCHAIN` with those values,
plus `PYTHONDONTWRITEBYTECODE=1`, and run:

```sh
bash /checks/crates/delta-arrow-reader-cli/release/validate-runtime.sh
```

The script records runtime package versions and checks version, inspection, and
scan output before adding test tools. It then installs the test prerequisites
and runs `xtask cli test-archive`. This command verifies the archive and extracted
file permissions, checks the executable's hash and ABI, and validates the saved
inspection and Arrow IPC output. It then runs the same process and scan suites
as `test-process` and `test-scan`. The script sets `PATH` to the PyArrow virtual
environment for the remaining Python checks. These tests cover external-writer
fixtures, filtering and execution options, pipes, partial failures, signals,
redaction, and trusted/untrusted HTTPS certificates. The archive and executable
hashes are checked again afterward.
CI uses these same commands for both architectures; see the workflow for the
complete container invocations.

To rerun acceptance after the script has installed the test tools and saved the
smoke output, use the same mounts and environment:

```sh
PATH="/tmp/cli-runtime-venv/bin:$PATH" /tools/xtask cli test-archive /artifacts \
  --extracted /package --smoke-output /tmp/cli-runtime-checks --repository /checks \
  --architecture "$CLI_ARCHITECTURE" --version "$CLI_VERSION" \
  --source-commit "$CLI_SOURCE_COMMIT" --rust-toolchain "$CLI_RUST_TOOLCHAIN"
```

`--repository` points to the mounted checkout containing the tests and fixtures.
The command checks the archive and executable again after both test suites pass.
Test output goes to stderr; stdout contains the acceptance result.

## Generate dependency notices

Use the same checkout and installed Rust toolchain as the binary build. Install
the pinned [cargo-about](https://embarkstudios.github.io/cargo-about/) version
and make `curl` available on `PATH`, then collect notices for the architecture
being packaged. `rust_toolchain` is the exact Rust release used by the build:

```sh
cargo install --locked --version 0.9.2 --features cli cargo-about
dependency_notices=target/cli-dependency-notices-amd64.txt
cargo xtask cli notices --rust-toolchain "$rust_toolchain" \
  --architecture amd64 --output "$dependency_notices"
```

The pinned cargo-about version is recorded in
`workspace.metadata.cli-release.cargo-about-version` in the root `Cargo.toml`.
CI reads that value and the generator checks the installed version. Omitting
`--rust-toolchain` uses the root package's `rust-version`.

The output file must not exist. The generator uses the CLI's default features,
the committed lockfile, and the selected Rust target. `about.toml` lists accepted
license identifiers. Build dependencies are included; dependencies used only
by tests are excluded. Unknown licenses or incomplete reports fail generation.

The output contains cargo-about's license texts and the resolved dependencies'
original license, copyright, and NOTICE files, including nested files. This
retains attribution that a generic license template can omit. It also includes
the installed Rust toolchain's standard-library copyright document and license
files, since Cargo does not list the standard library as a dependency. The Rust
copyright document keeps its original HTML.

Some published crates omit files that are present in their source repository.
`extra-notices.toml` records those files by crate version, source commit, URL,
and SHA-256. Downloads require HTTPS, must match the crate's recorded source
commit, and must pass the checksum check. A dependency upgrade that invalidates
an entry fails until its upstream files and checksums have been reviewed and
updated. Network failures also fail generation.

Repeat with `--architecture arm64` and a separate output file when packaging
that target. The generated file is input to the archive's `NOTICE`; it is not
an additional public artifact.

## Package one architecture

Run the tool from the source checkout used to build the binary. Supply its
workspace version, full lowercase commit SHA, and exact Rust release version
(for example, `1.94.0`). The destination must not exist.

```sh
cargo xtask cli artifacts package target/cli-inputs/amd64 \
  --version "$version" --source-commit "$source_commit" --rust-toolchain "$rust_toolchain" \
  --architecture amd64 --binary "$validated_binary" \
  --dependency-notices "$dependency_notices" \
  --runtime-dependency libc.so.6 --runtime-dependency libgcc_s.so.1
```

Set the shell variables from the validated build. Replace the example library
list with every `DT_NEEDED` library reported by that binary's ELF inspection,
using one `--runtime-dependency` argument per library. These are shared library
names, not Debian package names. The tool sorts the names and rejects duplicates.

Use the dependency notices file generated for this build. The packager appends
it to the repository's `NOTICE` and includes the repository's `LICENSE` unchanged.
The packager requires non-whitespace text; collection and
dependency coverage checks belong to `cargo xtask cli notices`.

Repeat for `arm64`, using that architecture's validated binary and dependencies.
Each platform directory contains one archive and a `cli-artifacts.json` with
one target. Archive members are regular files in this order: `dar`, `LICENSE`,
`NOTICE`. Their modes are `0755`, `0644`, and `0644`. Archive owner IDs and timestamps
are zero. Identical input bytes produce identical archives with the same tools;
separate compiler runs are not assumed to produce identical binaries.

## Assemble and verify the complete set

Place both platform directories under `target/cli-inputs`, named `amd64` and
`arm64`. Extra or missing directories and files cause assembly to fail.

```sh
cargo xtask cli artifacts assemble target/cli-artifacts \
  --inputs target/cli-inputs \
  --version "$version" --source-commit "$source_commit" --rust-toolchain "$rust_toolchain"

cargo xtask cli artifacts verify target/cli-artifacts \
  --version "$version" --source-commit "$source_commit" --rust-toolchain "$rust_toolchain"
```

Assembly requires matching build information and copies the original archive
bytes. Its new destination contains exactly four files:

- `dar-vX.Y.Z-linux-amd64-gnu.tar.gz`
- `dar-vX.Y.Z-linux-arm64-gnu.tar.gz`
- `cli-artifacts.json`, schema version 1, with both targets and archive/binary hashes
- `SHA256SUMS`, covering both archives and the combined manifest

`verify` checks the exact file set, safe archive members, permissions, repository
license and notices, metadata, and SHA-256 hashes. Supply the expected identity
from the requested build, rather than reading it from the downloaded manifest.
Keep the complete set unchanged when passing it to downstream jobs. A failed
command may leave an incomplete destination; use a new directory for the retry.

All four artifact commands write the validated manifest as JSON to stdout and
return a nonzero exit status on failure. To verify a single platform directory,
add `--architecture amd64` or `--architecture arm64` to `verify`. Without that
option, `verify` requires the complete four-file set. Extraction requires Unix
file permissions and refuses an existing destination.

Build xtask from the source checkout used for artifact validation. It embeds
that checkout's `LICENSE` and `NOTICE`, so the same executable can validate
archives in the runtime container without a Rust installation. The build workflow
mounts the Debian-built tool at `/tools/xtask`; it uploads only the public
archives, manifest, and checksums. The aggregation job runs `cargo xtask` after
installing the resolved Rust toolchain.

## Test the build and packaging helpers

```sh
cargo test --locked --package xtask
cargo clippy --locked --package xtask --all-targets -- -D warnings
```

The unit tests require Git, a C compiler, `objcopy`, `readelf`, and `sha256sum`.
They use synthetic archive payloads, license fixtures, and a small compiled ELF
fixture to exercise packaging, notice collection, source checks, ABI validation,
and rejection paths. Notice tests use supplied download responses and do not
require cargo-about or network access.

The native build test compiles a small Rust fixture inside Debian 12. It checks
the generated metadata and failure paths, including source changes during the
build and binary replacement before packaging. Run it with the exact Rust release
installed through rustup:

```sh
cargo test --locked --package xtask \
  native_build_preserves_metadata_and_rejects_source_changes -- --ignored
```

The CLI build workflow runs this test on both architectures. These helper tests
do not build the full Rust CLI or replace its runtime acceptance tests.
