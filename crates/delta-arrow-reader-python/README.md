# Delta Arrow Reader for Python

Read Delta Lake tables as PyArrow record batches with the Rust Delta Arrow Reader.

```console
python -m pip install delta-arrow-reader
delta-arrow-reader --help
```

The same package includes the native `delta-arrow-reader` command. To install
it in a separate tool environment, use `uv tool install delta-arrow-reader` or
run `uvx delta-arrow-reader --help`. Tool environments do not install the Python
API into your project's environment.

```python
from delta_arrow_reader import DeltaTable

table = DeltaTable("/path/to/delta-table")
with table.to_reader() as reader:
    for batch in reader:
        print(batch)
```

See the [Python quickstart](https://mag1cfrog.github.io/delta-arrow-reader/python/)
for column selection, filters, and snapshot refresh. The
[contributor guide](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/contributing.md)
covers local builds and tests.
