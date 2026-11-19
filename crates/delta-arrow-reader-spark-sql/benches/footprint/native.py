#!/usr/bin/env python3
"""Check and measure the frozen stock/forks-only native CLI process set."""

import collections
import datetime
import gzip
import hashlib
import itertools
import json
import os
import pathlib
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
MANIFEST = json.loads((HERE / 'queries.json').read_text())
SCHEDULE = json.loads((HERE / 'schedule.json').read_text())


def digest(path):
    with pathlib.Path(path).open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def read_host_metadata():
    return dict(uname=platform.uname()._asdict(),cpuinfo=pathlib.Path('/proc/cpuinfo').read_text(),meminfo=pathlib.Path('/proc/meminfo').read_text(),governors={str(cpu):pathlib.Path(f'/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor').read_text().strip() for cpu in SCHEDULE['cpus']})


def read_measurement_authorization():
    sources = json.loads((HERE/'sources.json').read_text())
    path = pathlib.Path(sources['evidence_root'])/'authorization.json'
    authorization = json.loads(path.read_text())
    assert authorization['authorized'] is True
    root = HERE.parents[3]
    assert authorization['protocol_commit'] == subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
    assert authorization['source_commits'] == sources['builds']
    assert authorization['files']
    for relative, sha256 in authorization['files'].items():
        assert digest(root/relative) == sha256, 'changed frozen source: '+relative
    protocol = HERE.parent/'reports/footprint-1381-protocol.md'
    assert authorization['protocol_sha256'] == digest(protocol)
    return dict(protocol_commit=authorization['protocol_commit'],protocol_sha256=authorization['protocol_sha256'],authorization_sha256=digest(path))


class TypedJsonReader:
    """Read the existing CLI typed-JSON encoding without retaining whole results."""

    def __init__(self, path, chunk=65536):
        self.source = pathlib.Path(path).open(encoding='utf-8')
        self.chunk, self.buffer, self.count = chunk, '', 0
        marker = ',"rows":['
        while marker not in self.buffer:
            self.read_chunk()
        header, self.buffer = self.buffer.split(marker, 1)
        self.header = json.loads(header + '}')
        assert self.header['format_version'] == 1 and self.header['kind'] == 'rows'
        self.schema = [dict(name=f['name'], data_type='STRING' if f['data_type'] in ('Utf8', 'LargeUtf8', 'Utf8View') else f['data_type']) for f in self.header['schema']['fields']]

    def read_chunk(self):
        assert len(self.buffer) < 1024**2, 'row/header exceeds bounded comparison buffer'
        chunk = self.source.read(self.chunk)
        assert chunk, 'truncated typed JSON'
        self.buffer += chunk

    def rows(self):
        decoder = json.JSONDecoder()
        try:
            while True:
                self.buffer = self.buffer.lstrip()
                if not self.buffer:
                    self.read_chunk()
                    continue
                if self.buffer.startswith(']'):
                    footer = self.buffer[1:] + self.source.read(65536)
                    assert not self.source.read(1), 'oversized JSON footer'
                    assert footer.startswith(','), 'missing JSON footer'
                    self.footer = json.loads('{' + footer[1:])
                    assert self.footer['completion'] == 'complete' and self.footer['scope'] == 'query'
                    assert int(self.footer['row_count']) == self.count
                    return
                if self.buffer.startswith(','):
                    self.buffer = self.buffer[1:]
                try:
                    value, end = decoder.raw_decode(self.buffer)
                except json.JSONDecodeError:
                    self.read_chunk()
                    continue
                assert isinstance(value, list) and len(value) == len(self.schema)
                self.buffer = self.buffer[end:]
                self.count += 1
                yield value
        finally:
            self.source.close()


def compare(left, right, left_chunk=65536, right_chunk=65536, deadline=None):
    a, b = TypedJsonReader(left, left_chunk), TypedJsonReader(right, right_chunk)
    assert a.schema == b.schema, 'executed SQL types/names differ'
    missing = object()
    for x, y in itertools.zip_longest(a.rows(), b.rows(), fillvalue=missing):
        if deadline is not None and min(a.count,b.count)%10000 == 0:
            remaining(deadline,300)
        assert x == y, f'decoded row/value/multiplicity differs at row {min(a.count,b.count)}'
    assert a.count == b.count
    if deadline is not None:
        remaining(deadline,300)
    return dict(rows=a.count, executed_schema=a.schema, status='equal')


def check_schedule():
    cases = [q['id'] for q in MANIFEST['cases']]
    assert len(cases) == len(set(cases)) == 8
    runs = SCHEDULE['runs']
    assert len(runs) == len({r['process'] for r in runs}) == 656
    for build in ('stock', 'forks-only'):
        for case in cases:
            selected = [r for r in runs if r['case'] == case and r['build'] == build]
            assert collections.Counter(r['series'] for r in selected) == dict(warmup=1, noise=20, main=20)
            assert collections.Counter(r['arm'] for r in selected if r['series'] == 'noise') == dict(a=10, b=10)
    assert all(r['build'] in ('stock', 'forks-only') and r['case'] in cases for r in runs)


def self_check():
    check_schedule()
    assert success_diagnostics(b'', 'version') == []
    done = b'{"kind":"completion","command":"query"}\n'
    assert len(success_diagnostics(done,'query')) == 1
    for data,kind in [(done,'version'),(done+done,'query'),(b' ','query'),(b' '* (1024**2+1),'version')]:
        try:
            success_diagnostics(data,kind)
        except AssertionError:
            pass
        else:
            raise AssertionError('invalid diagnostic count/bound accepted')
    with tempfile.TemporaryDirectory() as temp:
        temp = pathlib.Path(temp)
        def write(name, rows, kind='Utf8', count=None):
            fields = [dict(name='value', data_type=kind, nullable=True)]
            text = json.dumps(dict(format_version=1, kind='rows', bindings=[], schema=dict(fields=fields), rows=rows, row_count=str(len(rows) if count is None else count), completion='complete', scope='query'), separators=(',', ':'))
            path = temp/name
            path.write_text(text)
            return path
        left = write('left', [[None], ['a\n"b'], ['c']])
        right = write('right', [[None], ['a\n"b'], ['c']], 'Utf8View')
        assert compare(left,right,1,7)['rows'] == 3
        for name, rows, kind, count in [('cell',[[None],['changed'],['c']],'Utf8',None),('null',[['value'],['a\n"b'],['c']],'Utf8',None),('type',[[None],['a\n"b'],['c']],'Int64',None),('missing',[[None],['a\n"b']],'Utf8',None),('duplicate',[[None],['a\n"b'],['c'],['c']],'Utf8',None),('count',[[None],['a\n"b'],['c']],'Utf8',4)]:
            try:
                compare(left,write(name,rows,kind,count),5,11)
            except AssertionError:
                pass
            else:
                raise AssertionError('negative comparison accepted: '+name)


def remaining(deadline, limit):
    seconds = deadline-time.monotonic()
    if seconds <= 0:
        raise TimeoutError('overall fixed native deadline expired')
    return min(seconds,limit)


def fixture_check():
    receipt = HERE.parent/'reports/execution-1380-fixture.json'
    fixture = json.loads(receipt.read_text())
    snapshot = pathlib.Path(MANIFEST['snapshot'])
    assert digest(snapshot/'_delta_log/00000000000000000000.json') == fixture['derived_delta_log_sha256']
    for file in fixture['files']:
        assert digest(snapshot/file['path']) == file['sha256']
    return dict(receipt_sha256=digest(receipt), log_sha256=fixture['derived_delta_log_sha256'], parquet_sha256={f['path']:f['sha256'] for f in fixture['files']})


def write_query_inputs(destination):
    destination.mkdir()
    table = destination/'tables.json'
    table.write_text(json.dumps(dict(format_version=1,tables=[dict(alias='bench',location=MANIFEST['snapshot'],table_version='0')]))+'\n')
    for case in MANIFEST['cases']:
        if case['kind'] == 'query':
            (destination/(case['id']+'.sql')).write_text(case['sql']+'\n')
            order = ','.join(key+' NULLS FIRST' for key in case['preflight_order'])
            (destination/(case['id']+'.preflight.sql')).write_text('SELECT * FROM ('+case['sql']+') AS preflight_result ORDER BY '+order+'\n')
    return {str(p):digest(p) for p in destination.iterdir()}


def command(binary, case, input_dir, preflight_output=None):
    result = ['taskset','-c',','.join(map(str,SCHEDULE['cpus'])),str(binary)]
    if case['kind'] == 'version':
        return result+['--version']
    suffix = '.preflight.sql' if preflight_output else '.sql'
    result += ['--diagnostics=json','query','--tables-file',str(input_dir/'tables.json'),'--sql-file',str(input_dir/(case['id']+suffix)),'--format','json' if preflight_output else 'arrow','--memory-limit-bytes',str(SCHEDULE['memory_limit_bytes']),'--max-rows',str(SCHEDULE['max_rows']),'--max-output-bytes',str(SCHEDULE['max_output_bytes']),'--deadline-ms',str(SCHEDULE['process_timeout_seconds']*1000)]
    return result+(['--output',str(preflight_output)] if preflight_output else [])


def success_diagnostics(stderr, kind):
    assert len(stderr) <= 1024**2, 'CLI diagnostics exceed their retained bound'
    values = [json.loads(line) for line in stderr.decode().splitlines() if line.strip()]
    assert len(values) == (0 if kind == 'version' else 1), 'unexpected diagnostic record count'
    assert all(value.get('kind') == 'completion' and value.get('command') == 'query' for value in values)
    return values


def invoke(args, record_dir, timeout, timed=False):
    record_dir.mkdir()
    env = {k:v for k,v in os.environ.items() if k in ('PATH','HOME','LANG','LC_ALL')}
    env['LC_ALL'] = 'C'
    def limit():
        resource.setrlimit(resource.RLIMIT_AS,(SCHEDULE['address_space_bytes'],SCHEDULE['address_space_bytes']))
    (record_dir/'request.json').write_text(json.dumps(dict(command=args,timeout_seconds=timeout,timed=timed),indent=2)+'\n')
    with (record_dir/'stdout.txt').open('xb') if not timed else open(os.devnull,'wb') as output:
        started_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
        start = time.perf_counter_ns() if timed else None
        try:
            process = subprocess.run(args,stdout=output,stderr=subprocess.PIPE,env=env,preexec_fn=limit,timeout=timeout)
        except subprocess.TimeoutExpired as error:
            elapsed = time.perf_counter_ns()-start if timed else None
            stderr = error.stderr or b''
            result = dict(elapsed_ns=elapsed,exit_code=None,status='timeout',command=args)
        else:
            elapsed = time.perf_counter_ns()-start if timed else None
            stderr = process.stderr
            result = dict(elapsed_ns=elapsed,exit_code=process.returncode,status='completed',command=args)
    result.update(started_utc=started_utc,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),driver_sha256=digest(__file__))
    (record_dir/'stderr.jsonl').write_bytes(stderr)
    (record_dir/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    assert result['status'] == 'completed' and result['exit_code'] == 0, 'CLI failure retained at '+str(record_dir)
    result['diagnostics'] = success_diagnostics(stderr, 'version' if args[-1] == '--version' else 'query')
    return result


def bind_measurement_inputs(builds):
    result = dict(authorization=read_measurement_authorization(),queries_sha256=digest(HERE/'queries.json'),schedule_sha256=digest(HERE/'schedule.json'),driver_sha256=digest(__file__),fixture=fixture_check(),builds=builds)
    for name in ('stock','forks-only'):
        assert builds[name]['source_commit'] == json.loads((HERE/'sources.json').read_text())['builds'][name]
        assert digest(builds[name]['binary']) == builds[name]['binary_sha256']
    return result


def preflight(builds_path, destination):
    builds = json.loads(pathlib.Path(builds_path).read_text())['builds']
    proof = bind_measurement_inputs(builds)
    destination = pathlib.Path(destination).resolve(); destination.mkdir()
    proof['input_files'] = write_query_inputs(destination/'inputs')
    deadline = time.monotonic()+SCHEDULE['preflight_timeout_seconds']
    proof['results'], proof['comparisons'] = [], []
    for case in MANIFEST['cases']:
        outputs = {}
        for name in ('stock','forks-only'):
            work = destination/(case['id']+'-'+name)
            output = work/'rows.json' if case['kind'] == 'query' else None
            record = invoke(command(builds[name]['binary'],case,destination/'inputs',output),work,remaining(deadline,SCHEDULE['preflight_process_timeout_seconds']))
            record.update(case=case['id'],build=name)
            if output:
                outputs[name] = output
                record['export_sha256'] = digest(output)
            else:
                record['version'] = (work/'stdout.txt').read_text()
                outputs[name] = record['version']
            proof['results'].append(record)
        if case['kind'] == 'query':
            comparison = compare(outputs['stock'],outputs['forks-only'],deadline=deadline)
        else:
            assert outputs['stock'] == outputs['forks-only'] == 'dar 0.9.2\n'
            comparison = dict(status='equal',rows=None)
        proof['comparisons'].append(dict(case=case['id'],**comparison))
        print('preflight',case['id'],'equal',flush=True)
    proof['status'] = 'passed'
    (destination/'preflight.json').write_text(json.dumps(proof,indent=2)+'\n')


def measure(preflight_path, destination):
    preflight_path = pathlib.Path(preflight_path).resolve()
    proof = json.loads(preflight_path.read_text()); assert proof['status'] == 'passed'
    for key,value in bind_measurement_inputs(proof['builds']).items():
        assert proof[key] == value, 'changed binding: '+key
    for path,value in proof['input_files'].items():
        assert digest(path) == value
    expected = {r['case']:r['rows'] for r in proof['comparisons']}
    destination = pathlib.Path(destination).resolve(); destination.mkdir()
    (destination/'binding.json').write_text(json.dumps(dict(preflight=str(preflight_path),preflight_sha256=digest(preflight_path),host=read_host_metadata(),queries_sha256=proof['queries_sha256'],schedule_sha256=proof['schedule_sha256'],builds=proof['builds']),indent=2)+'\n')
    deadline = time.monotonic()+SCHEDULE['native_schedule_timeout_seconds']
    cases = {q['id']:q for q in MANIFEST['cases']}
    with (destination/'samples.jsonl').open('x') as raw:
        for run in SCHEDULE['runs']:
            print(run['process'],flush=True)
            record = invoke(command(proof['builds'][run['build']]['binary'],cases[run['case']],preflight_path.parent/'inputs'),destination/run['process'],remaining(deadline,SCHEDULE['process_timeout_seconds']),timed=True)
            if cases[run['case']]['kind'] == 'query':
                completions = [r for r in record['diagnostics'] if r.get('kind') == 'completion']
                assert len(completions) == 1
                done = completions[0]
                assert done['completion'] == 'complete' and done['scope'] == 'query' and done['stop_reason'] == 'end_of_input'
                assert int(done['rows']) == expected[run['case']], 'timed rows differ from exact preflight'
            record.update(run)
            raw.write(json.dumps(record,separators=(',',':'))+'\n');raw.flush()
    with (destination/'samples.jsonl').open('rb') as source, (destination/'samples.jsonl.gz').open('xb') as output:
        with gzip.GzipFile(fileobj=output,mode='wb',mtime=0) as compressed:
            compressed.write(source.read())
    (destination/'complete.json').write_text(json.dumps(dict(process_records=len(SCHEDULE['runs'])))+'\n')


if __name__ == '__main__':
    self_check()
    if sys.argv[1:] == ['--check']:
        print('native inventory and streaming semantic comparator checks passed')
    elif len(sys.argv) == 4 and sys.argv[1] in ('preflight','measure'):
        (preflight if sys.argv[1] == 'preflight' else measure)(*sys.argv[2:])
    else:
        raise SystemExit('usage: native.py --check | preflight BUILDS_JSON NEW_DIRECTORY | measure PREFLIGHT_JSON NEW_DIRECTORY')
