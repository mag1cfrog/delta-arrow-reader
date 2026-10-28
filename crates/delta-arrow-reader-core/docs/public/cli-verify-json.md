# Audit a saved JSON result

Use this example for an integrity audit, such as checking a transferred file
against the manifest requested at export with `--manifest`. For ordinary analysis, [read the saved result](cli-automation.md#read-a-saved-result)
and decode the requested values. The audit script checks integrity and counts;
compare scope, snapshot bindings and request provenance when they are part of
the audit.

For `totals.json`, verify its manifest against the actual saved bytes:

```sh
python - totals.json.manifest.json <<'PY'
import hashlib, json, pathlib, sys

manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert manifest['format_version'] == 1 and manifest['kind'] == 'result_manifest'
assert manifest['completion'] == 'complete' and manifest['result']['format'] == 'json'
result = pathlib.Path(manifest['result']['path'])
digest = hashlib.sha256()
with result.open('rb') as source:
    for chunk in iter(lambda: source.read(65536), b''):
        digest.update(chunk)
assert digest.hexdigest() == manifest['result']['sha256']
assert result.stat().st_size == int(manifest['output_bytes'])
with result.open() as source:
    rows = json.load(source)
assert rows['completion'] == 'complete' and rows['scope'] == 'query'
assert rows['bindings'] == manifest['bindings']
assert len(rows['rows']) == int(rows['row_count']) == int(manifest['rows'])
print(json.dumps({'verified_rows': manifest['rows']}))
PY
```

Decode values using this result's `schema.fields`, in field order. SQL can
produce a different Arrow representation from the table: `Utf8View`, `Utf8`,
and `LargeUtf8` all encode string values as JSON strings. The predicate input
type list does not define SQL output types. Require a particular Arrow type
only when the task requires it.

For exact numeric checks, keep integer strings as integers, never binary64
floats. Decimal strings are unscaled integers: `"12345"` with `Decimal128(18,4)`
means `1.2345`. `Date32` strings count days since `1970-01-01`; JSON null stays
null. Request an unfamiliar encoding separately, such as
`dar capabilities --schema rows --detail decimal`.

Print the check outcome or requested answer, not the full manifest or result.
For the expected replay fields, use the request's applicable options and
documented defaults; a field guide can list options belonging to other commands.

For an interrupted invocation, follow the
[publication and retry checks](cli-automation.md#verify-before-accepting-or-retrying).
