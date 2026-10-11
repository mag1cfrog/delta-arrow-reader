# Delta Arrow Reader for Python

Read Delta Lake tables as PyArrow record batches with the Rust Delta Arrow Reader.

```console
python -m pip install delta-arrow-reader
```

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
