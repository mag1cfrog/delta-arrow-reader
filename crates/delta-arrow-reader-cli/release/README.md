# CLI artifact tools

`artifacts.py` packages CLI binaries and checks the artifact format for issue
#433. It requires Python 3.11 or newer and uses only the standard library.

This layer checks file contents, declared build information, and hashes. It does
not build or execute `dar`, inspect ELF dependencies, or establish Debian 12
compatibility. The native build steps must supply and validate those inputs
before an artifact set can be released.

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

The dependency notices file must contain the license and copyright text needed
to redistribute the executable's dependencies. The tool appends it to the
repository's `NOTICE` and includes the repository's `LICENSE` unchanged. It checks
that the supplied notice text is nonempty; it cannot determine whether that text
covers every dependency.

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

## Test the packaging layer

```sh
python3 -m unittest discover -s crates/delta-arrow-reader-cli/release -v
```

The tests use synthetic binary bytes and exercise packaging and rejection paths.
They also check `SHA256SUMS` with the system `sha256sum` command. They do not test
the executables' runtime behavior.
