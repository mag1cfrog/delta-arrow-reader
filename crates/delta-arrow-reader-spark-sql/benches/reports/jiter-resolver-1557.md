# Linked workspace resolver proof (#1557)

MacBook Pro (Mac14,6), Apple M2 Max, native arm64 macOS 26.7.3 (25G342).
Rust 1.96.0. This item is a build and correctness check.

## Resolver conflict and fix

The first proof commit adds the Spark crate as a CLI dependency and selects the
same fifteen dependency patches at the root. The Python member stays in the
four-member workspace. `cargo metadata` then rejects the competing
`links = "python"` packages. The old jiter release optionally depends on
PyO3 0.28, and the Python member requires PyO3 0.29.
[First proof commit](https://github.com/mag1cfrog/delta-arrow-reader/commit/ae35bddffadf6caf6faef4c647ff3ae23e400581)
and [exact stderr](https://github.com/mag1cfrog/delta-arrow-reader/blob/380e4ee3/crates/delta-arrow-reader-spark-sql/benches/reports/jiter-resolver-1557-error.txt)

The first option in #1557 was to try an upstream release. jiter 0.16.0 moves
the optional PyO3 dependencies to 0.29 and adds an optional Serde adapter.
The only parser implementation change is the visibility of a string-decoder
field. Sail uses `num-bigint` with default features disabled, so both `python`
and `serde` are off. This release is the product fix. The fifteen local
patches and the Python package manifest stay as they were.
[Upstream source comparison](https://github.com/pydantic/jiter/compare/v0.15.0...v0.16.0)

The proof lock file contains jiter 0.16.0 and one PyO3 release, 0.29.3.
Cargo's normal and build dependency tree for the CLI on
`aarch64-apple-darwin` contains the Spark crate and no PyO3 package.
The root dependency and lock changes live on the proof branch.
[Proof branch](https://github.com/mag1cfrog/delta-arrow-reader/tree/chore/1557-spark-link-proof)

## Reproduction commands

In the proof worktree, run the first command at the first proof commit to
reproduce the conflict. Run the remaining commands at the proof branch tip.

```sh
cargo +1.96.0 metadata --format-version 1
cargo +1.96.0 metadata --locked --format-version 1
cargo +1.96.0 tree --locked -p delta-arrow-reader-cli --target aarch64-apple-darwin -e normal,build
cargo +1.96.0 check --workspace --locked
cargo +1.96.0 test --locked -p delta-arrow-reader-python
```

Use a Python 3.12 virtual environment with Maturin 1.14.1 and PyArrow 18.0.0.
Use the repository's pinned cargo-about 0.9.2 for the editable wheel build.
The first build stopped because this Mac had 0.9.3 on its path.
[cargo-about 0.9.2 release](https://github.com/EmbarkStudios/cargo-about/releases/tag/0.9.2)

On this Mac, the default temporary directory is reached through a symlink.
The build-backend test compares the temporary archive's full path, so the
second command resolves the directory before running the test.

```sh
uv pip install maturin==1.14.1 pyarrow==18.0.0
rustup run 1.96.0 python -I -c 'import pathlib, runpy, tempfile; tempfile.tempdir = str(pathlib.Path(tempfile.gettempdir()).resolve()); runpy.run_module("unittest", run_name="__main__")' discover -s crates/delta-arrow-reader-python/build_tests
rustup run 1.96.0 uv pip install --no-build-isolation --no-deps --editable crates/delta-arrow-reader-python
```

The editable build with the pinned tool stops at `unreviewed local dependency:
arrow-arith`. This is the linked workspace's license-review prerequisite in
#1558. Build the Python extension with Maturin for the local runtime tests.
The distribution test for generated third-party notices depends on the
repository's packaging backend.

```sh
rustup run 1.96.0 maturin build --profile dev --locked --manifest-path crates/delta-arrow-reader-python/Cargo.toml --interpreter python --out wheel-output
uv pip install --no-deps wheel-output/delta_arrow_reader_py-*.whl
python -I -m unittest discover -s /tmp/dar-1557-proof/crates/delta-arrow-reader-python/tests -p 'test_[!p]*.py'
```

The Python extension built and the 72 API tests passed. For test discovery,
use the `/tmp` alias to the proof checkout. The logging assertions check for
the word `private` in captured records, which also include the test runner's
source path.

The linked root passed metadata resolution, the workspace check, the Python
crate tests and the four Python build-backend tests.

In the standalone Spark workspace, run the parser comparison, formatting,
Clippy and the full tests. The runtime tests include the committed SQL captures
and their value and executed-type checks in both ANSI modes.

The parser comparison agreed on all 123 distinct inputs. It compares decoded
values, parser errors, end-of-input checks and streaming scans across four
committed captures and thirteen added edge cases, including malformed and
truncated inputs, deep nesting, and strings up to 1 MiB.

```sh
rustfmt +1.96.0 --edition 2024 --check ci/jiter-upgrade-probe.rs
rustup run 1.96.0 python3 ci/check-jiter-upgrade.py
rustup run 1.96.0 python3 ci/check-rustfmt.py
cargo +1.96.0 tree --locked -e features -i jiter
cargo +1.96.0 test --locked --workspace --no-fail-fast
cargo +1.96.0 clippy --locked -p delta-arrow-reader-spark-sql --all-targets --no-deps -- -D warnings
```

The Spark workspace passed formatting, Clippy and the full test suite on this
Mac. The accepted-runtime test checked 25,180 recorded SQL outcomes. This
change leaves the committed inputs and expected outcomes intact.
