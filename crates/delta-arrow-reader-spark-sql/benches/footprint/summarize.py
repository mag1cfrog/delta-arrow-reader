#!/usr/bin/env python3
"""Aggregate every frozen native CLI process without selecting new samples."""

import collections
import csv
import gzip
import json
import pathlib
import statistics
import sys

from native import HERE, MANIFEST, SCHEDULE, digest


def spread(values):
    quartiles=statistics.quantiles(values,n=4,method='inclusive')
    return dict(median_ns=statistics.median(values),minimum_ns=min(values),maximum_ns=max(values),iqr_ns=quartiles[2]-quartiles[0])


def slower_than_noise(stock, forks, noise):
    return forks>1.05*stock and forks-stock>noise


def aggregate(records):
    assert len(records)==len(SCHEDULE['runs'])
    assert len({r['process'] for r in records})==len(records), 'duplicate process record'
    schedule={r['process']:r for r in SCHEDULE['runs']}
    assert {r['process'] for r in records}==set(schedule), 'missing/unknown process record'
    groups=collections.defaultdict(list)
    for record in records:
        expected=schedule[record['process']]
        assert all(record[key]==value for key,value in expected.items())
        assert record['status']=='completed' and record['exit_code']==0
        assert isinstance(record['elapsed_ns'],int) and record['elapsed_ns']>0
        if record['series']!='warmup':
            groups[record['series'],record['case'],record['build'],record.get('arm')].append(record['elapsed_ns'])
    assert all(len(v)==(20 if k[0]=='main' else 10) for k,v in groups.items())
    table=[]
    for case in MANIFEST['cases']:
        name=case['id']
        stock=spread(groups['main',name,'stock',None]);forks=spread(groups['main',name,'forks-only',None])
        noise={build:{arm:spread(groups['noise',name,build,arm]) for arm in ('a','b')} for build in ('stock','forks-only')}
        floor=max(abs(arms['a']['median_ns']-arms['b']['median_ns']) for arms in noise.values())
        table.append(dict(case=name,stock=stock,forks_only=forks,noise=noise,noise_floor_ns=floor,forks_over_stock=forks['median_ns']/stock['median_ns'],difference_ns=forks['median_ns']-stock['median_ns'],above_threshold=slower_than_noise(stock['median_ns'],forks['median_ns'],floor)))
    return table


def summarize(run_directory, output):
    assert json.loads((run_directory/'complete.json').read_text())['process_records']==656
    binding=json.loads((run_directory/'binding.json').read_text())
    assert binding['queries_sha256']==digest(HERE/'queries.json') and binding['schedule_sha256']==digest(HERE/'schedule.json')
    assert binding['preflight_sha256']==digest(binding['preflight'])
    preflight=json.loads(pathlib.Path(binding['preflight']).read_text())
    expected_rows={r['case']:r['rows'] for r in preflight['comparisons']}
    with gzip.open(run_directory/'samples.jsonl.gz','rt') as source:records=[json.loads(line) for line in source]
    for record in records:
        if expected_rows[record['case']] is not None:
            assert len(record['diagnostics'])==1 and int(record['diagnostics'][0]['rows'])==expected_rows[record['case']]
    table=aggregate(records);output.mkdir(parents=True,exist_ok=True)
    (output/'footprint-1381-native-summary.json').write_text(json.dumps(dict(process_records=len(records),cases=table),indent=2)+'\n')
    rows=[]
    for case in table:
        for build,key in [('stock','stock'),('forks-only','forks_only')]:
            rows.append(dict(case=case['case'],build=build,**case[key],noise_a_median_ns=case['noise'][build]['a']['median_ns'],noise_b_median_ns=case['noise'][build]['b']['median_ns'],noise_floor_ns=case['noise_floor_ns'],forks_over_stock=case['forks_over_stock'],difference_ns=case['difference_ns'],above_threshold=case['above_threshold']))
    with (output/'footprint-1381-native.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    print(json.dumps(dict(cases=len(table),csv_rows=len(rows),process_records=len(records),flags=[r['case'] for r in table if r['above_threshold']])))


def self_check():
    assert not slower_than_noise(100,105,0)
    assert not slower_than_noise(100,106,6)
    assert slower_than_noise(100,106,5)
    records=[dict(r,status='completed',exit_code=0,elapsed_ns=100 if r['build']=='stock' else 110) for r in SCHEDULE['runs']]
    assert len(aggregate(records))==8 and all(r['above_threshold'] for r in aggregate(records))
    for invalid in (records[:-1],records+[records[0]]):
        try:
            aggregate(invalid)
        except AssertionError:
            pass
        else:
            raise AssertionError('missing/duplicate record accepted')


if __name__=='__main__':
    self_check()
    if sys.argv[1:]==['--check']:
        print('native aggregation, inventory and strict noise threshold checks passed')
    elif len(sys.argv)==3:
        summarize(*map(lambda p:pathlib.Path(p).resolve(),sys.argv[1:]))
    else:
        raise SystemExit('usage: summarize.py --check | RUN_DIRECTORY REPORT_DIRECTORY')
