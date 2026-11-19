#!/usr/bin/env python3
"""Prepare selected graphs, measure three clean CLI builds, and check Rust floors."""

import collections
import datetime
import json
import os
import pathlib
import re
import shutil
import signal
import subprocess
import sys
import time

from native import digest, read_host_metadata, read_measurement_authorization, SCHEDULE

HERE = pathlib.Path(__file__).resolve().parent
SOURCES = json.loads((HERE/'sources.json').read_text())
SCRATCH = pathlib.Path(SOURCES['scratch_worktree'])
TARGET = pathlib.Path(SOURCES['target_root'])


def build_environment(toolchain):
    allowed = {'PATH','HOME','CARGO_HOME','RUSTUP_HOME','http_proxy','https_proxy','no_proxy'}
    env = {k:v for k,v in os.environ.items() if k in allowed or k.lower() in allowed}
    env.update(LC_ALL='C',RUSTUP_TOOLCHAIN=toolchain,CARGO_BUILD_JOBS=str(SCHEDULE['build_jobs']),CARGO_NET_OFFLINE='true',RUSTFLAGS='-Ctarget-cpu=x86-64',CFLAGS='-march=x86-64 -mtune=generic',CXXFLAGS='-march=x86-64 -mtune=generic')
    return env


def assert_clean_source(name):
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=SCRATCH,text=True).strip(), 'dirty owned scratch source'
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=SCRATCH,text=True).strip() == SOURCES['builds'][name]


def checkout_source(name):
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=SCRATCH,text=True).strip(), 'dirty owned scratch source'
    commit = SOURCES['builds'][name]
    subprocess.run(['git','checkout','--detach',commit],cwd=SCRATCH,check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=SCRATCH,text=True).strip() == commit


def restore_scratch_branch():
    subprocess.run(['git','checkout',SOURCES['scratch_branch']],cwd=SCRATCH,check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)


def run_command(args, record_dir, timeout, env, timed=False):
    record_dir.mkdir(parents=True)
    (record_dir/'request.json').write_text(json.dumps(dict(command=args,timeout_seconds=timeout,environment={k:('<present>' if k.lower() in ('http_proxy','https_proxy','no_proxy') else v) for k,v in env.items()}),indent=2)+'\n')
    started_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
    start = time.perf_counter_ns() if timed else None
    process = subprocess.Popen(args,cwd=SCRATCH,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        elapsed = time.perf_counter_ns()-start if timed else None
        status = 'completed'
    except subprocess.TimeoutExpired:
        os.killpg(process.pid,signal.SIGKILL)
        stdout, stderr = process.communicate()
        elapsed = time.perf_counter_ns()-start if timed else None
        status = 'timeout'
    (record_dir/'stdout.log').write_bytes(stdout);(record_dir/'stderr.log').write_bytes(stderr)
    receipt = dict(started_utc=started_utc,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),driver_sha256=digest(__file__),command=args,status=status,exit_code=process.returncode,elapsed_ns=elapsed,stdout_sha256=digest(record_dir/'stdout.log'),stderr_sha256=digest(record_dir/'stderr.log'))
    (record_dir/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    return receipt


def selected_packages(metadata, tree):
    packages = collections.defaultdict(list)
    for package in metadata['packages']:
        packages[package['name'],package['version']].append(package)
    selected = {}
    for line in tree.splitlines():
        match = re.match(r'^(\S+) v(\S+)(?: \((.*?)\))?\t(.*?)(?: \(\*\))?$',line)
        if not match:
            assert not re.match(r'^\S+ v\d',line), 'unparsed Cargo tree package: '+line
            continue
        candidates = packages[match[1],match[2]]
        if len(candidates)>1:
            candidates = [p for p in candidates if str(pathlib.Path(p['manifest_path']).parent)==match[3]]
        assert len(candidates)==1, 'ambiguous selected package identity: '+line
        package = candidates[0]
        entry = selected.setdefault(package['id'],dict(package,selected_features=[]))
        entry['selected_features'] = sorted(set(entry['selected_features'])|set(filter(None,match[4].split(','))))
    assert selected
    return sorted(selected.values(),key=lambda p:(p['name'],p['version'],p['id']))


def prepare_graphs(destination):
    destination.mkdir()
    try:
        for name in SCHEDULE['build_order']:
            checkout_source(name)
            work = destination/name;work.mkdir()
            env = build_environment(SCHEDULE['toolchain'])
            prefix = ['rustup','run',SCHEDULE['toolchain'],'cargo']
            command = prefix+['fetch','--locked','--target',SCHEDULE['target'],'--manifest-path','crates/delta-arrow-reader-cli/Cargo.toml']
            fetch_env = dict(env);fetch_env.pop('CARGO_NET_OFFLINE')
            assert run_command(command,work/'fetch',1800,fetch_env)['exit_code']==0
            command = prefix+['metadata','--locked','--format-version','1','--filter-platform',SCHEDULE['target'],'--manifest-path','crates/delta-arrow-reader-cli/Cargo.toml']
            assert run_command(command,work/'metadata',300,env)['exit_code']==0
            command = prefix+['tree','--locked','-p','delta-arrow-reader-cli','--target',SCHEDULE['target'],'--edges','normal,build','--prefix','none','--format','{p}\t{f}']
            assert run_command(command,work/'tree',300,env)['exit_code']==0
            metadata = json.loads((work/'metadata/stdout.log').read_text())
            packages = selected_packages(metadata,(work/'tree/stdout.log').read_text())
            names = {p['name'] for p in packages}
            assert not any('pyo3' in n or n=='delta-arrow-reader-python' for n in names)
            assert ('delta-arrow-reader-spark-sql' in names)==(name=='linked')
            forks = [p['name'] for p in packages if '/vendor/dependencies/' in p['manifest_path']]
            assert len(forks)==(0 if name=='stock' else 15)
            graph = dict(build=name,source_commit=SOURCES['builds'][name],package_count=len(packages),packages=packages,selected_forks=sorted(forks),lock_sha256=digest(SCRATCH/'Cargo.lock'),manifest_sha256=digest(SCRATCH/'Cargo.toml'))
            (work/'graph.json').write_text(json.dumps(graph,indent=2)+'\n')
            print(name,'selected packages',len(packages),flush=True)
    finally:
        restore_scratch_branch()


def cargo_build_args(toolchain, target_dir):
    return ['rustup','run',toolchain,'cargo','build','--release','--locked','--package','delta-arrow-reader-cli','--target',SCHEDULE['target'],'--target-dir',str(target_dir)]


def clean_builds(destination):
    authorization = read_measurement_authorization()
    destination.mkdir()
    (destination/'authorization.json').write_text(json.dumps(authorization,indent=2)+'\n')
    (destination/'host.json').write_text(json.dumps(read_host_metadata(),indent=2)+'\n')
    builds = {}
    try:
        for name in SCHEDULE['build_order']:
            checkout_source(name)
            target = TARGET/('clean-'+name)
            assert not target.exists(), 'retain existing clean target; never repeat this unchanged build'
            work = destination/name;work.mkdir()
            print('clean build',name,flush=True)
            result = run_command(cargo_build_args(SCHEDULE['toolchain'],target),work/'cargo',SCHEDULE['clean_build_timeout_seconds'],build_environment(SCHEDULE['toolchain']),timed=True)
            assert result['status']=='completed' and result['exit_code']==0, 'clean build failed; retain evidence, stop without repeating'
            assert_clean_source(name)
            binary = target/SCHEDULE['target']/'release/dar'
            original = work/'dar-original';shutil.copyfile(binary,original);original.chmod(0o555)
            stripped = work/'dar-stripped';shutil.copyfile(original,stripped)
            subprocess.run(['strip','--strip-all',str(stripped)],check=True)
            receipt = dict(authorization=authorization,source_commit=SOURCES['builds'][name],source_tree=subprocess.check_output(['git','rev-parse','HEAD^{tree}'],cwd=SCRATCH,text=True).strip(),binary=str(original),binary_sha256=digest(original),unstripped_bytes=original.stat().st_size,stripped_bytes=stripped.stat().st_size,stripped_sha256=digest(stripped),target_directory=str(target),clean_build=result,compiler=subprocess.check_output(['rustup','run',SCHEDULE['toolchain'],'rustc','-vV'],text=True),strip_version=subprocess.check_output(['strip','--version'],text=True).splitlines()[0])
            builds[name]=receipt
            (destination/'builds.json').write_text(json.dumps(dict(status='partial' if len(builds)<3 else 'complete',sources_sha256=digest(HERE/'sources.json'),schedule_sha256=digest(HERE/'schedule.json'),builds=builds),indent=2)+'\n')
    finally:
        restore_scratch_branch()


def rust_version_matrix(builds_path, destination):
    builds = json.loads(builds_path.read_text())['builds'];destination.mkdir();results=[]
    try:
        for name in SCHEDULE['build_order']:
            checkout_source(name)
            for toolchain in SCHEDULE['msrv_candidates']:
                if toolchain==SCHEDULE['toolchain']:
                    assert builds[name]['source_commit']==SOURCES['builds'][name] and digest(builds[name]['binary'])==builds[name]['binary_sha256']
                    result=dict(build=name,toolchain=toolchain,status='successful full CLI build reused',proof=builds[name]['clean_build'],reused_clean_build=True)
                    results.append(result);break
                target=TARGET/('msrv-'+name+'-'+toolchain)
                assert not target.exists()
                result=run_command(cargo_build_args(toolchain,target),destination/(name+'-'+toolchain),SCHEDULE['msrv_build_timeout_seconds'],build_environment(toolchain))
                result.update(build=name,toolchain=toolchain,reused_clean_build=False)
                results.append(result)
                (destination/'results.json').write_text(json.dumps(results,indent=2)+'\n')
                if result['status']=='completed' and result['exit_code']==0:
                    assert_clean_source(name)
                    binary=target/SCHEDULE['target']/'release/dar'
                    result.update(binary_sha256=digest(binary),binary_bytes=binary.stat().st_size,compiler=subprocess.check_output(['rustup','run',toolchain,'rustc','-vV'],text=True))
                    break
        (destination/'results.json').write_text(json.dumps(results,indent=2)+'\n')
    finally:
        restore_scratch_branch()


def test_forks_only(destination):
    destination.mkdir();results=[]
    try:
        checkout_source('forks-only')
        target_dir=TARGET/'native-tests';assert not target_dir.exists()
        for name,flags in [('core-direct',['-p','delta-arrow-reader']),('core-datafusion',['-p','delta-arrow-reader','--all-features']),('cli',['-p','delta-arrow-reader-cli'])]:
            command=['rustup','run',SCHEDULE['toolchain'],'cargo','test','--locked','--no-fail-fast',*flags,'--target-dir',str(target_dir)]
            result=run_command(command,destination/name,5400,build_environment(SCHEDULE['toolchain']))
            result['suite']=name;results.append(result)
            (destination/'results.json').write_text(json.dumps(results,indent=2)+'\n')
    finally:
        restore_scratch_branch()


def attribute_linked_code_size(builds_path, destination):
    builds=json.loads(builds_path.read_text())['builds'];destination.mkdir()
    linked=builds['linked'];original=pathlib.Path(linked['binary'])
    assert digest(original)==linked['binary_sha256']
    try:
        checkout_source('linked')
        probe_env=build_environment(SCHEDULE['toolchain'])
        probe_env['DAR_1381_SPARK_SQL']='SELECT CAST(1 AS BIGINT) AS value'
        probe=run_command([str(original),'--version'],destination/'reachable-probe',30,probe_env)
        assert probe['status']=='completed' and probe['exit_code']==0
        symbols=subprocess.check_output(['nm','-C',str(original)],text=True)
        retained=[line for line in symbols.splitlines() if 'delta_arrow_reader_spark_sql::' in line or 'plan_spark_query_for_measurement' in line]
        assert retained, 'no retained frontend symbols'
        (destination/'retained-symbols.txt').write_text('\n'.join(retained)+'\n')
        binary=pathlib.Path(linked['target_directory'])/SCHEDULE['target']/'release/dar'
        before=dict(sha256=digest(binary),bytes=binary.stat().st_size)
        command=['rustup','run',SCHEDULE['toolchain'],'cargo','bloat','--release','--locked','--bin','dar','--package','delta-arrow-reader-cli','--target',SCHEDULE['target'],'--target-dir',linked['target_directory'],'--crates','-n','20','--message-format','json']
        result=run_command(command,destination/'cargo-bloat',1800,build_environment(SCHEDULE['toolchain']))
        assert result['status']=='completed' and result['exit_code']==0
        after=dict(sha256=digest(binary),bytes=binary.stat().st_size)
        assert digest(original)==linked['binary_sha256']
        assert_clean_source('linked')
        (destination/'attribution.json').write_text(json.dumps(dict(cargo_bloat_version=subprocess.check_output(['rustup','run',SCHEDULE['toolchain'],'cargo','bloat','--version'],text=True).strip(),original_binary_sha256=linked['binary_sha256'],tool_binary_before=before,tool_binary_after=after,tool_rebuilt=before!=after,attribution_artifact=str(binary),note='Any changed tool artifact is separate from the immutable measured original; no clean build repeated.'),indent=2)+'\n')
    finally:
        restore_scratch_branch()


def self_check():
    packages=[dict(id='a',name='a',version='1.0.0',manifest_path='/root/a/Cargo.toml'),dict(id='unused',name='optional-python',version='1.0.0',manifest_path='/root/python/Cargo.toml')]
    values=selected_packages(dict(packages=packages),'a v1.0.0 (/root/a)\tdefault\na v1.0.0 (/root/a)\tdefault (*)\n')
    assert len(values)==1 and values[0]['id']=='a' and values[0]['selected_features']==['default']
    env=build_environment('1.96.0');assert 'RUSTC_WRAPPER' not in env and 'CARGO_PROFILE_RELEASE_DEBUG' not in env
    import tempfile
    from unittest.mock import patch
    calls=[]
    def record_test_command(args, record_dir, timeout, env):
        calls.append(args)
        return dict(status='completed',exit_code=1 if len(calls)==1 else 0)
    with tempfile.TemporaryDirectory() as temp:
        temp=pathlib.Path(temp)
        with patch.dict(globals(),dict(TARGET=temp/'targets',checkout_source=lambda name:None,restore_scratch_branch=lambda:None,run_command=record_test_command)):
            test_forks_only(temp/'receipts')
        assert len(calls)==3
        assert all('--no-fail-fast' in args and args[-1]==str(temp/'targets/native-tests') for args in calls)
        assert [args[args.index('-p')+1] for args in calls]==['delta-arrow-reader','delta-arrow-reader','delta-arrow-reader-cli']
        assert '--all-features' in calls[1]
        assert len(json.loads((temp/'receipts/results.json').read_text()))==3


if __name__=='__main__':
    self_check()
    if sys.argv[1:]==['--check']:
        print('selected graph and release environment checks passed')
    elif len(sys.argv)==3 and sys.argv[1] in ('prepare','clean','test'):
        {'prepare':prepare_graphs,'clean':clean_builds,'test':test_forks_only}[sys.argv[1]](pathlib.Path(sys.argv[2]).resolve())
    elif len(sys.argv)==4 and sys.argv[1] in ('msrv','bloat'):
        (rust_version_matrix if sys.argv[1]=='msrv' else attribute_linked_code_size)(pathlib.Path(sys.argv[2]).resolve(),pathlib.Path(sys.argv[3]).resolve())
    else:
        raise SystemExit('usage: build.py --check | prepare/clean/test NEW_DIRECTORY | msrv/bloat BUILDS_JSON NEW_DIRECTORY')
