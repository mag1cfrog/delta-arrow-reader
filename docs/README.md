# Repository documentation

The [documentation site](https://mag1cfrog.github.io/delta-arrow-reader/) covers
using the reader, its execution model, API options, and performance reports.

Contributor instructions and development records are kept in this repository:

- [Local checks and documentation builds](content/contributing.md)
- [Benchmark setup and reproduction](../benches/selective_read/README.md)
- [Lazy and eager metadata comparison](content/benchmarks/eager-metadata.md)
- [Cumulative partial-page comparison](content/benchmarks/intra-page-types-cumulative.md)
- [CLI release approval, publication, and recovery](content/cli-release.md)
- [Project direction and language support](content/project-direction.md)
- [Frozen Spark SQL experiment](content/spark-sql-experiment.md)
- [Extraction provenance](content/provenance.md)
- [Extraction parity](content/benchmark-parity.md)

## Site content

Edit the Markdown in `crates/delta-arrow-reader-core/docs/public/` and the navigation
in `crates/delta-arrow-reader-core/docs/mkdocs.yml`.
Keep benchmark results under Performance. Setup instructions, execution
contracts, capacity checks, and issue histories belong in repository docs.

`crates/delta-arrow-reader-core/docs/public/` is the only directory published by the
site build. Keep repository-only material in `docs/content/`, where existing benchmark documents
retain their paths. Some are hashed inputs to reader builds, so preserve their
bytes when reproducing recorded runs. Removing a page from navigation alone
does not remove it from the site or its search index.

Build the site with the same strict check used by CI:

```console
python -m pip install -r docs/requirements.txt
python -m zensical build --strict -f crates/delta-arrow-reader-core/docs/mkdocs.yml
```

## Python API reference

The site reads
`crates/delta-arrow-reader-core/docs/python_api/delta_arrow_reader/__init__.pyi`,
a generated copy of the Python type stubs with docstrings from the compiled Rust bindings.
Ordinary site builds do not import the reader or compile Rust.

Edit API descriptions in the Rust binding comments under
`crates/delta-arrow-reader-python/src/`, and signatures in
`crates/delta-arrow-reader-python/delta_arrow_reader.pyi`. After rebuilding and
installing the Python package from this checkout, regenerate the API file with
the same Python environment:

```console
python crates/delta-arrow-reader-python/generate_api_docs.py
python crates/delta-arrow-reader-python/generate_api_docs.py --check
```

Commit the generated file with the source change. The existing `Python Package`
CI job checks it against the bindings it already builds, so this check adds no
second Rust build.
