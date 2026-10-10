# CLI release tools

The scripts in this directory build, inspect, and package CLI binaries for issue
#433. They require Python 3.11 or newer and use only the standard library.

## Build on Debian 12

`build.py` builds one architecture from an explicit, clean source commit. Use the
Debian 12 image pinned by digest in `build.py`, on the matching architecture. The
script checks the OS, glibc version, and Rust compiler host before compiling. It
reads the workspace version and Rust toolchain from `Cargo.toml`, uses the locked
release build, and supplies baseline CPU flags for Rust and C/C++ dependencies.
Compiler flags and wrapper environment variables supplied by the caller are
excluded from the build environment.

For a local amd64 build, use a normal Git clone with the requested commit checked
out and no modified or untracked files. A Git worktree also needs its shared Git
directory mounted inside the container. This example uses rootless Podman:

```sh
cli_source_commit=FULL_SOURCE_COMMIT
cli_image=$(python3 -c 'import sys; sys.path.insert(0, "crates/delta-arrow-reader-cli/release"); from build import DEBIAN_IMAGE; print(DEBIAN_IMAGE)')
podman run --rm -it --platform linux/amd64 \
  --volume "$PWD:/source:z" --workdir /source \
  --env CLI_SOURCE_COMMIT="$cli_source_commit" "$cli_image" bash
```

Inside the container, install the build prerequisites and the workspace's Rust
toolchain using [rustup](https://rust-lang.github.io/rustup/installation/index.html):

```sh
set -eu
apt-get update
apt-get install -y --no-install-recommends \
  build-essential binutils ca-certificates curl git pkg-config python3
cli_rust_toolchain=$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["package"]["rust-version"])')
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain "$cli_rust_toolchain"
. "$HOME/.cargo/env"
python3 crates/delta-arrow-reader-cli/release/build.py \
  --source-commit "$CLI_SOURCE_COMMIT" --architecture amd64
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

For an explicit ELF and version check, install `binutils` and `python3` in the
test container and mount the source checkout read-only at `/checks`. Keep `dar`
outside that checkout and pass the version and hash from the earlier validated
build:

```sh
python3 /checks/crates/delta-arrow-reader-cli/release/check_binary.py /path/to/dar \
  --architecture amd64 \
  --expected-version "$version" --expected-sha256 "$dar_sha256"
```

This command requires Debian 12 and the matching CPU architecture. It reports
the observed ELF metadata as JSON. A successful version check still requires
the scan, process lifecycle, and HTTPS acceptance tests before release.

The existing CLI test harnesses accept an executable path. With the test
prerequisites installed (`build-essential`, Python, and the PyArrow version used
by CLI CI), run them against that same binary:

```sh
python3 crates/delta-arrow-reader-cli/tests/process.py /path/to/dar "$version"
python3 crates/delta-arrow-reader-cli/tests/scan.py /path/to/dar
```

## Generate dependency notices

Use the same checkout and installed Rust toolchain as the binary build. Install
the pinned [cargo-about](https://embarkstudios.github.io/cargo-about/) version,
then collect notices for the architecture being packaged:

```sh
cargo install --locked --version 0.9.2 --features cli cargo-about
dependency_notices=target/cli-dependency-notices-amd64.txt
python3 crates/delta-arrow-reader-cli/release/notices.py \
  --architecture amd64 --output "$dependency_notices"
```

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
python3 crates/delta-arrow-reader-cli/release/artifacts.py package target/cli-inputs/amd64 \
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
The packager checks only that the supplied text is nonempty; collection and
dependency coverage checks belong to `notices.py`.

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
python3 crates/delta-arrow-reader-cli/release/artifacts.py assemble target/cli-artifacts \
  --inputs target/cli-inputs \
  --version "$version" --source-commit "$source_commit" --rust-toolchain "$rust_toolchain"

python3 crates/delta-arrow-reader-cli/release/artifacts.py verify target/cli-artifacts \
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

## Test the build and packaging helpers

```sh
python3 -m unittest discover -s crates/delta-arrow-reader-cli/release -v
```

The unit tests require Git, a C compiler, `objcopy`, `readelf`, and `sha256sum`.
They use synthetic archive payloads, license fixtures, and a small compiled ELF
fixture to exercise packaging, notice collection, source checks, ABI validation,
and rejection paths. Notice tests mock downloads and do not require cargo-about.
They do not build the Rust CLI or replace its runtime acceptance tests.
