# Ordinary release notices for a linked Spark build (#1558)

MacBook Pro (Mac14,6), Apple M2 Max, native arm64 macOS 26.7.3 (25G342).
Rust 1.96.0 and cargo-about 0.9.2. The release notice target is
`x86_64-unknown-linux-gnu`.

## Baseline

The linked proof starts at staging `b768c75e`. It keeps the four root
workspace members and selects the fifteen dependency patches from the
Spark workspace. The ordinary scan rejected the LLVM exception on
`ar_archive_writer` and could not find license metadata or crate-local
license files for eight Sail packages. The ordinary renderer stopped at
that scan. When the historical benchmark clarifications were applied
temporarily in the proof checkout, the unchanged renderer reached the local
dependencies and rejected `arrow-arith`. The proof restored the ordinary
configuration after this isolation check.
[Baseline diagnostics](linked-notices-1558-baseline-stderr.txt)

## Release configuration

The ordinary scan uses the shared, checksum-pinned Sail license and accepts
the LLVM exception for `ar_archive_writer`. The renderer checks each reviewed
local package against its version and manifest path before printing the
upstream project, commit and license files. The Arrow Rust, DataFusion,
rust-lexical and Sail license texts were checked against their upstream
commits. [Source provenance](../../UPSTREAM.md)

The release configuration pins the original legal files omitted from the
published `datasketches`, `next-gen-proc_macros` and `regexml` archives.
The published `datasketches` and `regexml` manifests and library entries
match their pinned source trees. The canonical `next-gen` parent manifest
and its macro member's manifest and three source files match the published
archives at the shared commit. All nine downloaded supplements matched
their checksums. [Published-source evidence from #1381](footprint-1381-preparation.json)

The stock CLI NOTICE matches byte for byte.

| Stock CLI | SHA-256 |
| --- | --- |
| Staging before | `e5f3665695974d2f69a94ee15059b66d0d63cbc22208748c6a16281fb50b4111` |
| Final release configuration | `e5f3665695974d2f69a94ee15059b66d0d63cbc22208748c6a16281fb50b4111` |

The ten notice unit tests and both notice integration tests passed.

## Linked NOTICE and receipt

The renderer got past the local attribution check with the new release
configuration and then stopped at `genawaiter`, whose archive has no
original legal files. The separate xot change removes all three
genawaiter packages. The linked root selects sixteen source patches,
including xot, and retains all four workspace members.
[Iterator proof](xot-1558-iterators.md) and [linked proof branch](https://github.com/mag1cfrog/delta-arrow-reader/tree/chore/1558-linked-notices-proof)

The ordinary scan and renderer passed for 449 packages, including 24 local
third-party packages. The receipt accounts for every selected license text,
785 original package files and 13 Rust toolchain files. Each original-file
hash was checked against its package file or pinned supplement. The linked
NOTICE is 4,820,402 bytes and its SHA-256 is
`937a6dd243e3f1a20ee4116771b301fdae8ded7aec68d36be684677f48f7f7db`.
[Full NOTICE](linked-notices-1558-dependency-notices.txt.gz) and [license receipt](linked-notices-1558-receipt.json.gz)

## Open legal question

`regexml` 0.2.2 declares `Apache-2.0 OR MPL-2.0`. The scanner selected
Apache-2.0. The original COPYRIGHT identifies Saxonica code under MPL 2.0
and includes separate Unicode and W3C notices. The release configuration
retains the complete original COPYRIGHT, both license texts and CREDITS.
Which license should the project select, and what do the Saxonica, Unicode
and W3C notices require for our distribution?
[Original COPYRIGHT](https://github.com/Paligo/regexml/blob/2d113d8efe426810e076e15e1376bcc1637c9707/COPYRIGHT)

## Reproduce the linked receipt

Use the repository's pinned cargo-about 0.9.2 and run these commands from
the linked proof's root. Use the same `xtask cli notices` command without
the root Spark link for the stock before-and-after check.

```sh
mkdir -p target
cargo +1.96.0 about generate --locked --fail \
  --manifest-path crates/delta-arrow-reader-cli/Cargo.toml \
  --config crates/delta-arrow-reader-cli/release/about.toml \
  --target x86_64-unknown-linux-gnu --format json > target/linked-license-scan.json
cargo +1.96.0 run --locked -p xtask -- cli notices \
  --architecture amd64 --rust-toolchain 1.96.0 --output target/linked-notice.txt
python3 crates/delta-arrow-reader-spark-sql/benches/linked-notice-receipt.py \
  --scan target/linked-license-scan.json --notice target/linked-notice.txt \
  --config crates/delta-arrow-reader-cli/release/about.toml \
  --output target/linked-license-receipt.json.gz
```
