# Repository documentation

The [documentation site](https://mag1cfrog.github.io/delta-arrow-reader/) covers
using the reader, its execution model, API options, and performance reports.

Contributor instructions and development records are kept in this repository:

- [Local checks and documentation builds](content/contributing.md)
- [Benchmark setup and reproduction](../benches/selective_read/README.md)
- [Lazy and eager metadata comparison](content/benchmarks/eager-metadata.md)
- [Project direction and Python bindings](content/project-direction.md)
- [Frozen Spark SQL experiment](content/spark-sql-experiment.md)
- [Extraction provenance](content/provenance.md)
- [Extraction parity](content/benchmark-parity.md)

## Site content

Edit the Markdown in `docs/public/` and the navigation in `docs/mkdocs.yml`.
Keep benchmark results under Performance. Setup instructions, execution
contracts, capacity checks, and issue histories belong in repository docs.

`docs/public/` is the only directory published by the site build. Keep
repository-only material in `docs/content/`, where existing benchmark documents
retain their paths. Some are hashed inputs to reader builds, so preserve their
bytes when reproducing recorded runs. Removing a page from navigation alone
does not remove it from the site or its search index.

Build the site with the same strict check used by CI:

```console
python -m pip install -r docs/requirements.txt
python -m zensical build --strict -f docs/mkdocs.yml
```
