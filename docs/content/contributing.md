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

## Build the Python package locally

The Python package loads the latest Delta snapshot with `DeltaTable(location)`
and exposes its read-only `version` property. Pass `version=0` to load a specific
snapshot; omitting `version` or passing `None` loads the latest snapshot.
Run these commands from the repository root on Linux or macOS:

```console
python3 -m venv target/python-venv
. target/python-venv/bin/activate
python -m pip install 'maturin>=1.13,<2'
maturin build --locked --manifest-path crates/delta-arrow-reader-python/Cargo.toml --out target/python-wheels
python -m pip install --force-reinstall target/python-wheels/*.whl
python -I -m unittest discover -s crates/delta-arrow-reader-python/tests
```

The isolated Python check imports the installed wheel without adding the current
directory to its import path. Default Cargo commands still select the core crate;
use `cargo check -p delta-arrow-reader-python` to check the binding crate directly.

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
