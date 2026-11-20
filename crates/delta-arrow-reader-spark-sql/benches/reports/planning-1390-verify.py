#!/usr/bin/env python3
"""Verify completed planning evidence; never launches a query or compiler."""
import gzip, hashlib, json, pathlib
root = pathlib.Path(__file__).resolve().parent
manifest = json.loads((root / 'planning-1390-manifest.json').read_text())
for name, record in manifest['artifacts'].items():
    raw = (root / name).read_bytes()
    assert len(raw) == record['bytes'], name
    assert hashlib.sha256(raw).hexdigest() == record['sha256'], name
    if record.get('encoding', '').startswith('gzip;'):
        assert hashlib.sha256(gzip.decompress(raw)).hexdigest() == record['source_sha256'], name
def rows(name):
    with gzip.open(root / name, 'rt') as source:
        return [json.loads(line) for line in source]
samples = rows('planning-1390-samples.jsonl.gz')
assert len(samples) == 2100 and len({row['process'] for row in samples}) == 70
assert sum(len(row['logical_ns']) for row in samples) == 18900
assert all(len(row[key]) == 9 for row in samples for key in ('logical_ns', 'physical_ns', 'total_ns', 'events'))
assert all(row['total_ns'][i] == row['logical_ns'][i] + row['physical_ns'][i] for row in samples for i in range(9))
plans = rows('planning-1390-plans.jsonl.gz')
assert len(plans) == 1356
indexed = {(row['mode'], row['configuration'], row['id']): {key: value for key, value in row.items() if key != 'mode'} for row in plans}
assert len(indexed) == 1356
for (mode, configuration, query), row in indexed.items():
    assert row == indexed[('observed' if mode == 'canonical' else 'canonical', configuration, query)]
    assert all(scan['bytes_received'] == 0 for scan in row['delta_io'])
for mode in ('canonical', 'observed'):
    with gzip.open(root / ('planning-1390-' + mode + '-receipt.json.gz'), 'rt') as source:
        receipt = json.load(source)
    assert receipt['baseline'] == manifest['baseline'] and receipt['protocol_commit'] == manifest['protocol_commit']
print('Verified artifact hashes, 70 processes, 18,900 samples, and 1,356 matching public plan/schema records. No query or build ran.')
