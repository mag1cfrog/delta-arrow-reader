# CLI release tools

These tools build and validate the Linux CLI archives and build local container
images from them. Builds, ELF checks, archive operations, dependency notice
collection, and archive acceptance run through `cargo xtask`, a Cargo alias for
the repository's private Rust maintenance crate. Process and scan checks run in
Rust through `xtask cli test-process` and `xtask cli test-scan`.

## Validate a CLI release

`.github/workflows/cli-build.yml` validates a CLI release candidate. It runs the
CLI unit tests, Clippy, and release-tool tests alongside native amd64 and arm64
builds. Both platforms build once in the pinned Debian 12 image. After both builds
finish, six parallel jobs test the original archives on Debian 12, Ubuntu 24.04,
and Fedora 44, on both architectures. Each job downloads its architecture's
archive and the xtask executable built on Debian; runtime jobs do not compile
Rust. All three runtime images are pinned by digest. Available runner capacity
determines how many jobs can start at once.
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

Release-plz PRs targeting main must pass this workflow before merging. CI
identifies them by the release bot's identity and a `release-plz-` branch in this
repository. When ready to validate a release, open its CI run, choose **Review
deployments**, select `release-checks`, and click **Approve and deploy**.
This approval starts **Release CLI binaries** and the Python wheel checks in
parallel. Both build the PR's merge candidate at its exact commit SHA and
workspace version. After binary validation, **Release CLI images and Go consumer**
uses those binaries on native amd64 and arm64 runners in parallel, with the same
approval. The required `CI` check waits for all three artifact sets and the
separate CLI validation record. New PR commits cancel the old CI run and require
a new approval and validation.

Feature PRs skip release approval, builds, and image/Go acceptance. They still
run the small workflow tests alongside the shared xtask checks for Python and
cache tasks. CLI unit tests, native builds, and runtime acceptance run only for
approved release PRs or an explicit manual build. To run the builder manually
during development, select
**Build CLI binaries** in Actions and supply the commit as `source-ref` and its
version as `expected-version`. The successful job summary links the complete
artifact set.

Configure `release-checks` in **Settings > Environments** with the maintainer
as a required reviewer, administrator bypass disabled, and self-review allowed
so the maintainer can approve a run they restart. Allow the deployment branch
pattern `refs/pull/*/merge`. This environment needs no secrets or publishing
permissions; it approves the release PR's CLI and Python builds and tests.

The workflow runs only when dispatched manually or called by another workflow.
It reports `validated=true` only after source checks, both builds, all six runtime
checks, and artifact assembly succeed. Release consumers must require this result
before distributing the artifacts.

The reusable workflow returns:

| Output | Meaning |
| --- | --- |
| `validated` | `true` only after source checks, both builds, and all six runtime checks pass and the complete set is uploaded |
| `source-commit`, `version`, `rust-toolchain` | Resolved source SHA, workspace version, and exact Rust release |
| `artifact-id`, `artifact-name`, `artifact-url` | The complete set's immutable Actions ID, run-specific name, and download page |
| `manifest-sha256` | SHA256 of the uploaded complete `cli-artifacts.json` |

Consumers must depend on the successful job and require `validated == 'true'`.
Within the same workflow run, download that exact artifact with
[`actions/download-artifact@v6`](https://github.com/actions/download-artifact/tree/v6),
using either `artifact-ids` or `name` from the outputs. Downloads across runs also
require `run-id` and `github-token`. Check out the returned source commit and run
`cargo xtask cli artifacts verify` with the expected version, source SHA, and Rust
release from the workflow outputs. Intermediate platform archives and
`dar-cli-tools-<source-sha>-<architecture>` artifacts support runtime checks.
They are not complete, validated sets for downstream consumers.

CLI artifacts are retained for 90 days. Missing, expired, incomplete, or mismatched
artifacts must stop downstream work. Rerun this builder for the explicit source
commit to obtain a new validated set and reference. Container and release jobs
must consume its existing archive bytes without rebuilding or repacking them.

Release PR artifacts record the tested merge candidate's source SHA. A future
publisher must verify that the release tag has the same source tree and version
and preserve that build identity; the final merge commit can have a different
SHA. Automatic CLI publication is tracked in issue #435.

## Validate CLI images in Actions

`.github/workflows/cli-images.yml` takes the complete binary artifact returned
by a successful `cli-build.yml` validation. It verifies the exact source commit,
workspace version, Rust release, archive checksums, and both architectures before
building images. The images and the Go consumer use the existing `dar` bytes;
only host-side xtask tools are compiled in Rust.

Release PR CI calls this workflow after the approved binary job reports success
and `validated=true`. Native amd64 and arm64 jobs each build a CLI image, run its
runtime acceptance, build and run the Go example, compare the copied executable
and notices, and export/reimport the tested image as OCI. A final job verifies
both platform artifacts, copies their archive bytes into a complete set, and
uploads it. Feature PRs do not run these jobs, regardless of changed paths.

The reusable workflow accepts these inputs:

| Input | Source |
| --- | --- |
| `binary-artifact-id` | Binary workflow's immutable `artifact-id` output for the complete four-file set |
| `binary-run-id` | Run that owns the binary artifact; omit only when calling within that same run |
| `source-ref` | Binary workflow's full `source-commit` output |
| `expected-version` | Binary workflow's `version` output, without `v` |
| `rust-toolchain` | Binary workflow's exact `rust-toolchain` output |

Callers need `contents: read` and `actions: read` permissions and must require
successful binary validation before calling. Missing or expired artifacts,
partial platform uploads, and mismatched identities fail validation. There is
no fallback to an artifact name or a different source revision.

For development or recovery, manually dispatch **Validate CLI images** using
the values from a successful binary run. The workflow revision selected for
dispatch and the requested source commit must both contain the image tools.
For example, after those tools are on main:

```sh
gh workflow run cli-images.yml --ref main \
  -f binary-artifact-id="$binary_artifact_id" -f binary-run-id="$binary_run_id" \
  -f source-ref="$source_commit" -f expected-version="$version" \
  -f rust-toolchain="$rust_toolchain"
```

The complete Actions artifact is named
`dar-cli-images-<source-sha>-<run-id>-<attempt>` and contains the two OCI archives
and `cli-images.json` described below. Intermediate platform uploads are not
validated complete sets. Both binary and image uploads have a 90-day retention
period; publication requires both sets to remain available.
If either has expired, validate the requested source again.

Outputs match the binary workflow: `validated`, `source-commit`, `version`,
`rust-toolchain`, `artifact-id`, `artifact-name`, `artifact-url`, and
`manifest-sha256`. The manifest hash covers the uploaded complete `cli-images.json`.
`validated=true` is emitted only after both native image/Go jobs pass and the
complete upload returns an artifact ID and URL. Required `CI` also rejects
failed, skipped, or canceled image jobs, missing outputs, and a source/version
that differs from the binary job. The job summary links the complete artifact.

This workflow has read-only permissions and publishes no registry tags.
Issue #435 owns the validation record, final release/source-tree binding,
GHCR authentication, and multi-platform index. Publication must preserve these
OCI archives and their tested source labels.

## Record validated CLI artifacts

On approved release PRs, the **Record validated CLI artifacts** job follows both
CLI workflows. It uses their immutable artifact IDs and manifest hashes, checks
their successful results and matching source/version/toolchain, and writes
`cli-validation.json`. The job checks out the tested merge candidate and requires
its two parents to match the event's base and PR head commits. It reads the source
tree from Git and the workspace version from that clean checkout.

The record job compiles only xtask. It does not download the binary/image sets
or repeat their acceptance checks. Each producer hashes its complete manifest
after artifact upload and returns that hash only as part of a successful handoff.

The schema-1 record contains:

| Field | Meaning |
| --- | --- |
| `repository`, `pull_request`, `pull_request_head_sha` | Upstream release PR and its validated head revision |
| `source_commit`, `source_tree`, `version` | Actual tested merge candidate, Git tree, and workspace version |
| `run_id`, `run_attempt` | CI run and attempt that created the record |
| `binary_artifact.id`, `binary_artifact.manifest_sha256` | Complete binary set's Actions ID and `cli-artifacts.json` hash |
| `image_artifact.id`, `image_artifact.manifest_sha256` | Complete OCI set's Actions ID and `cli-images.json` hash |

Existing manifests remain the source of per-file hashes and platform digests.
The record does not modify them or replace the tested source SHA with a later
release commit. Source-tree equality lets publication compare the tested merge
candidate with a final squash or merge commit even when their SHAs differ.

The upload is named `dar-cli-validation-<source-sha>-<run-id>-<attempt>`, contains
only `cli-validation.json`, and is retained for 90 days, like the complete binary
and image sets. It never overwrites an earlier record. Required `CI` rejects a
failed, canceled, skipped, or missing record upload, missing upload outputs, and
a record associated with a different source, PR head, or version. Ordinary
feature PRs skip this job.

To reproduce record creation from a CI checkout with the original event and
the record job's serialized `needs` context:

```sh
cargo xtask cli record-validation "$RUNNER_TEMP/cli-validation.json" \
  --event "$GITHUB_EVENT_PATH" --job-results "$RUNNER_TEMP/cli-job-results.json" \
  --source-commit "$GITHUB_SHA" --run-id "$GITHUB_RUN_ID" --run-attempt "$GITHUB_RUN_ATTEMPT"
```

The output path must be new. The command checks the supplied event and results;
it does not authenticate them against GitHub. Publication must verify the record
against GitHub's PR, approval, run, job, and artifact metadata, then verify the
retained files before writing to a release or registry. That publication verifier
is part of #435; a locally generated JSON record alone does not authorize release.

## Select artifacts for an existing release

`cargo xtask cli select-release-artifacts` checks the release and validation
provenance through GitHub's API and writes a new local JSON report. Install and
authenticate `gh` with read access to this repository's releases, PRs, Actions,
and environments. The command downloads only the small validation record. It
does not download the binary/image sets, execute their contents, or publish.

Supply an existing published stable release tag:

```sh
cargo xtask cli select-release-artifacts selection.json --tag vX.Y.Z
```

Selection requires all of the following:

- The tag resolves to a commit reachable from upstream `main`, contains the CLI,
  and matches the workspace version. Drafts, prereleases, and non-version tags
  are rejected. Lightweight and annotated tags are supported.
- The commit belongs to one merged release-plz PR, identified by its bot author,
  upstream head/base repositories, release branch prefix, and `main` target.
- The validation comes from successful upstream `ci.yml` for that PR's merged
  head revision. The tested source tree and version equal the release's, even
  when the tested merge commit and final release commit have different SHAs.
- GitHub reports successful change detection, approval, binary/image handoff,
  record, and aggregate CI jobs, plus an approved `release-checks` review.
  Partial reruns may reuse earlier successful producer jobs within the same
  run and PR revision; each artifact must match its producer's actual attempt.
- The record ZIP matches GitHub's SHA256 and size, contains only
  `cli-validation.json`, and names complete binary/image artifacts owned by that
  run and repository. All three artifacts must remain available and unexpired.

The selector compares the record with GitHub metadata, including immutable
repository, workflow, run, and artifact IDs. It reads the release manifests at
the resolved commit, without fetching or relying on a surviving temporary PR
merge commit. It checks the release and tag again before writing the report.

If multiple eligible records remain, selection fails and lists their run and
record artifact IDs. Select both explicitly to resolve the ambiguity:

```sh
cargo xtask cli select-release-artifacts selection.json --tag vX.Y.Z \
  --run-id VALIDATION_RUN_ID --record-artifact-id RECORD_ARTIFACT_ID
```

Explicit selection applies the same checks. Missing artifacts stop selection;
the command cannot rebuild them or fall back to current main. An API or download
failure also stops selection, so an unavailable candidate cannot silently remove
an ambiguity. The output path must be new.

The report contains the release ID, tag, commit, tree, and version; the original
validation record; and GitHub metadata for the three selected artifacts,
including their ZIP SHA256 values. It is a provenance report, not an upload plan
or publication authorization. Full binary/OCI download and integrity checks,
publication planning, and upload remain part of #435. A publisher must perform
those checks and recheck current release/artifact state before writing.

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

Use a fresh container from one of the pinned runtime images. Install
`ca-certificates`, `libc6`, and `libgcc-s1` on Debian or Ubuntu, or
`ca-certificates`, `glibc`, and `libgcc` on Fedora. Run version, inspection, and
scan checks with these runtime packages before installing test instrumentation.
The binary is dynamically linked to glibc and libgcc; this does not support
Alpine's musl runtime or a `scratch` image. These containers test distribution
userland and shared libraries while sharing the runner's kernel.

For an explicit ELF and version check, install `binutils` in the test container
and mount the Debian-built xtask executable read-only at `/tools/xtask`.
Pass the version and hash from the earlier validated build:

```sh
/tools/xtask cli check-binary /path/to/dar \
  --architecture amd64 \
  --expected-version "$version" --expected-sha256 "$dar_sha256"
```

This command requires glibc 2.36 or newer and the matching CPU architecture,
without requiring a particular distribution. The binary's own GLIBC symbol
requirements must still stay at or below 2.36. It reports the observed ELF
metadata as JSON. A successful version check still requires the scan, process
lifecycle, and HTTPS acceptance tests before release.

The existing CLI test harnesses accept an executable path. With the test
prerequisites installed (`build-essential` and `openssl` on Debian or Ubuntu,
or `gcc` and `openssl` on Fedora), run them against that same binary:

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
Checks with stdout blocked cover early pipe closure and SIGINT/SIGTERM termination.
Signal checks also hold data requests, read the IPC schema, and verify that
termination closes the held connection without writing stderr.
Scheduling checks cover partition targets and read capacities for both Parquet
backends, bounded reads while stdout is blocked, and incremental output. Slow
consumer checks hold stdout beyond the request timeout after a pending response
has completed. Prefetch checks verify that row limits, read errors, and output
errors stop the process even when a blocking file open is still pending.
The thread checks compile a C fault library with `cc`.
The HTTPS checks use OpenSSL to generate temporary certificates and configure
trust only for the CLI subprocess.

In a runtime container without Cargo, use `/tools/xtask cli test-process` and
`/tools/xtask cli test-scan` with `--repository /checks`. The source checkout
supplies fixtures and the C fault library source.

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

Start a fresh container on the matching architecture, using one of the Debian,
Ubuntu, or Fedora image digests in the workflow. Use read-only mounts for the
source checkout at `/checks`, `target/cli-inputs/amd64` at
`/artifacts`, `/tmp/dar-amd64-check` at `/package`, and the Debian-built
`target/cli-build/debug/xtask` at `/tools/xtask`. Pass `CLI_ARCHITECTURE=amd64`,
`CLI_SOURCE_COMMIT`, `CLI_VERSION`, and `CLI_RUST_TOOLCHAIN` with those values
and run:

```sh
bash /checks/crates/delta-arrow-reader-cli/release/validate-runtime.sh
```

The script records runtime package versions and checks version, inspection, and
scan output before adding test tools. It then installs the test prerequisites
and runs `xtask cli test-archive`. This command verifies the archive and extracted
file permissions, checks the executable's hash and ABI, and validates the saved
inspection and Arrow IPC output. It then runs the same process and scan suites
as `test-process` and `test-scan`. These tests cover external-writer
fixtures, filtering and execution options, pipes, partial failures, signals,
redaction, and trusted/untrusted HTTPS certificates. The archive and executable
hashes are checked again afterward.
CI uses these same commands in all six architecture/distribution combinations;
see the workflow for the complete container invocations.

To rerun acceptance after the script has installed the test tools and saved the
smoke output, use the same mounts and environment:

```sh
/tools/xtask cli test-archive /artifacts \
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
archives in the runtime container without a Rust installation. Each build uploads
the Debian-built tool as a separate intermediate Actions artifact. Runtime jobs
download it and mount it at `/tools/xtask`. The complete artifact set contains
only the public archives, manifest, and checksums. The aggregation job runs
`cargo xtask` after installing the resolved Rust toolchain.

## Build a local container image

Use the complete four-file binary artifact set with its expected source SHA,
workspace version, and exact Rust release. The host needs Rust to run xtask,
`readelf` from binutils, and Docker. Run on the target architecture, or configure
container emulation before building for another architecture.

```sh
cargo xtask cli build-image target/cli-artifacts \
  --architecture amd64 --tag dar:local-amd64 \
  --version "$version" --source-commit "$source_commit" --rust-toolchain "$rust_toolchain"
docker run --rm --read-only dar:local-amd64 --version
docker run --rm --read-only \
  --mount type=bind,src=/absolute/path/to/table,dst=/table,readonly \
  dar:local-amd64 inspect /table
```

For arm64, use `--architecture arm64` and a separate local tag. An existing local
tag is replaced on a successful build. The command verifies both archives and
the combined manifest/checksums before extracting the selected binary. Its ELF
architecture, libraries, and hash must match the manifest. Missing archives or
mismatched source/version metadata stop the command before Docker runs.

The temporary build context contains only the Dockerfile, `dar`, `LICENSE`,
`NOTICE`, and a derived runtime package list. The pinned Debian 12 slim image
installs those runtime libraries and CA certificates. It contains no compiler or
source checkout. The executable and its notices retain their archive contents
and permissions. OCI labels record the repository, tested source SHA, workspace
version, Apache-2.0 license, and pinned base image.

The image uses UID/GID `65532:65532`, working directory `/work`, and executable
entrypoint `/usr/local/bin/dar`. Omitting arguments prints help. Fixture and
configuration mounts must be readable by that user. Ordinary reads work with a
read-only root filesystem; supply credentials and storage configuration at
runtime. Copying the binary into another image requires glibc 2.36 or newer and
its declared libraries. Alpine and `scratch` are unsupported.

This command builds a local image. It does not export OCI artifacts or publish
to GHCR.

## Check a local image at runtime

Run the image on its matching Linux architecture. The host needs Docker, Rust
for xtask, and OpenSSL for temporary test certificates. Docker must run on the
same host as xtask: the tests bind local fixture directories and use host
networking to reach a local HTTP/TLS fixture server.

```sh
cli_image_id="$(docker image inspect --format '{{.Id}}' dar:local-amd64)"
cargo xtask cli test-image "$cli_image_id" \
  --architecture amd64 --expected-version "$version" --repository .
```

Use the same image ID for the OCI export below and for the Go consumer build.
The test command also accepts a local tag, resolves it once to an immutable
image ID, and checks its platform before running it. It does not pull images.

Every container uses a read-only root filesystem and read-only fixture mounts.
Checks cover the default user and working directory, help/version, inspection,
redacted errors, and Arrow IPC results against the Spark corpus. The binary and
image suites share the projection, limit, typed predicate, execution-options,
nested mapping, and deletion-vector assertions.

The TLS checks require the image's default CA bundle, reject a locally generated
CA by default, accept it through `SSL_CERT_FILE`/`SSL_CERT_DIR`, and reject a
certificate issued by another CA. Certificates and options are generated at
runtime; no external storage or credentials are required. The suite also closes
the CLI's output pipe inside the container to avoid Docker buffering hiding a
write failure. Only this pipe fixture gets a small writable `/tmp` tmpfs.

Signal checks hold a table request open, verify that `dar` is PID 1, and send
SIGINT and SIGTERM. Both inspection and scan must exit promptly with status 130
and 143, without output, and close their network connection. PID 1 installs
termination handlers because Linux otherwise ignores its default termination
signals. Normal CLI subprocesses retain their existing signal behavior.

Checks have deadlines and remove their test containers on failure. Fixture
mount paths must be UTF-8 and cannot contain commas. This command tests runtime
behavior; `cli images` separately verifies artifact identity and packaging.
The Go consumer runs in its own workflow step after these runtime checks.

## Export and verify image artifacts

Use `cli images` to prepare container artifacts for later publication. In addition
to the complete binary artifact set, these commands need Skopeo and umoci on a
Unix host. `export` also needs Docker and a local image from `build-image`.
On Debian or Ubuntu, install the container tools with
`sudo apt-get install skopeo umoci`.

Export each architecture on its build host, using the image ID tested above.
For example, after the amd64 build and runtime checks:

```sh
cargo xtask cli images export target/cli-image-platforms/amd64 \
  --binary-artifacts target/cli-artifacts \
  --architecture amd64 --image "$cli_image_id" \
  --version "$version" --source-commit "$source_commit" --rust-toolchain "$rust_toolchain"
```

Use `arm64` and its tested image ID for the other build. Each platform directory
contains its OCI archive and a single-platform `cli-images.json`. Collect the
two directories under `target/cli-image-platforms`, then assemble the complete
set:

```sh
cargo xtask cli images assemble target/cli-images \
  --inputs target/cli-image-platforms --binary-artifacts target/cli-artifacts \
  --version "$version" --source-commit "$source_commit" --rust-toolchain "$rust_toolchain"
cargo xtask cli images verify target/cli-images \
  --binary-artifacts target/cli-artifacts \
  --version "$version" --source-commit "$source_commit" --rust-toolchain "$rust_toolchain"
```

The complete directory contains exactly:

```text
dar-vX.Y.Z-linux-amd64.oci.tar
dar-vX.Y.Z-linux-arm64.oci.tar
cli-images.json
```

The schema-1 manifest records the workspace version, tested source SHA, Rust
release, and pinned base image. Each target records its architecture, binary
archive hash, `dar` hash, OCI archive hash, OCI platform manifest digest, and
container configuration including labels. `manifest_digest` identifies the OCI
platform manifest, not a Docker image ID or a multi-platform index.

Export uses `docker save` and Skopeo's OCI conversion. Verification imports the
archive into a temporary OCI layout with `--all --preserve-digests` and rejects
indexes or archives containing multiple images. It checks the platform, labels,
non-root user, entrypoint, default arguments, working directory, and environment.
umoci then unpacks the layers without root privileges. The `dar`, `LICENSE`, and
`NOTICE` contents and permissions must match the corresponding binary archive.
Assembly copies the verified OCI archives byte for byte and checks their hashes
again; it does not rebuild or repack them.

All three commands print the validated manifest as JSON to stdout and send
container tool output to stderr. Add `--architecture amd64` or `arm64` to
`verify` for a single-platform directory. Export and assembly refuse an existing
destination. A failed command can leave a partial destination without a complete
manifest; use a new directory when retrying. Container tool paths must be UTF-8
and cannot contain `:`.

These commands validate packaging and file identity. The image workflow also
runs CLI and Go acceptance before uploading the complete set. None of these
commands publish to GHCR.

## Test the build and packaging helpers

```sh
cargo test --locked --package xtask
cargo clippy --locked --package xtask --all-targets -- -D warnings
```

The CLI release workflow runs the CLI crate's unit tests and Clippy, plus the
xtask unit tests and the `artifacts`, `notices`, `cli_workflow`,
`cli_images_workflow`, and `images` integration tests. Record unit tests and the
`cli_validation_workflow` checks also run in ordinary PR CI. The small workflow and
`images` tests also run in ordinary PR CI. The image tests use a small compiled
ELF fixture and stub container commands; they do not build containers or
require Docker, Skopeo, or umoci.
The native build test below runs separately in each Debian build job.

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
