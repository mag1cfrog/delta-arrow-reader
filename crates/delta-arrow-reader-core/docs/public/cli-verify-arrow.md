# Verify a saved Arrow result

Use these examples after [saving a complete result](cli-automation.md#save-a-complete-result).
The script checks integrity and counts;
also compare the output path, scope, snapshot bindings, and request provenance
with your intended request before accepting the result.

Use an environment with PyArrow installed. For `totals.arrow`, check the
manifest against the actual Arrow stream and its end marker:

```sh
python - totals.arrow.manifest.json <<'PY'
import hashlib, json, pathlib, sys
import pyarrow.ipc as ipc

manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert manifest['format_version'] == 1 and manifest['kind'] == 'result_manifest'
assert manifest['completion'] == 'complete' and manifest['result']['format'] == 'arrow'
path = pathlib.Path(manifest['result']['path'])
digest = hashlib.sha256()
with path.open('rb') as source:
    for chunk in iter(lambda: source.read(65536), b''):
        digest.update(chunk)
    source.seek(-8, 2)
    assert source.read() == bytes([255, 255, 255, 255, 0, 0, 0, 0])
assert digest.hexdigest() == manifest['result']['sha256']
assert path.stat().st_size == int(manifest['output_bytes'])
rows = batches = 0
with ipc.open_stream(path) as reader:
    for batch in reader:
        rows += batch.num_rows
        batches += 1
assert rows == int(manifest['rows']) and batches == int(manifest['batches'])
print(json.dumps({'verified_rows': str(rows)}))
PY
```

Also check any exact values the task requires. Arrow preserves source batches.
These checks verify a saved file, not whether you selected the right data. For an interrupted invocation, follow the
[publication and retry checks](cli-automation.md#verify-before-accepting-or-retrying).
