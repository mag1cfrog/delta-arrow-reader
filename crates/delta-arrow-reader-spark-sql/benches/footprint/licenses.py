#!/usr/bin/env python3
"""Collect measurement-local license evidence and comparable three-member archives."""

import gzip
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import tarfile
import tomllib
import urllib.request
import zlib

from build import SCRATCH, SOURCES, TARGET, build_environment, checkout_source, restore_scratch_branch, run_command
from native import digest, SCHEDULE

HERE = pathlib.Path(__file__).resolve().parent
CARGO_ABOUT = TARGET/'tools/bin/cargo-about'
SAIL_PACKAGES = {'sail-common','sail-common-datafusion','sail-function','sail-logical-plan','sail-plan','sail-sql-analyzer','sail-sql-macro','sail-sql-parser'}
DAR_PACKAGES = {'delta-arrow-reader','delta-arrow-reader-cli','delta-arrow-reader-spark-sql'}


def install_tool(destination):
    destination.mkdir()
    env = build_environment(SCHEDULE['toolchain']);env.pop('CARGO_NET_OFFLINE')
    command = ['rustup','run',SCHEDULE['toolchain'],'cargo','install','--locked','--version','0.9.2','--features','cli','--root',str(TARGET/'tools'),'--target-dir',str(TARGET/'tool-build'),'cargo-about']
    result = run_command(command,destination/'cargo-about',1800,env)
    assert result['status']=='completed' and result['exit_code']==0
    assert subprocess.check_output([str(CARGO_ABOUT),'--version'],text=True).strip()=='cargo-about 0.9.2'
    (destination/'tool.json').write_text(json.dumps(dict(version='cargo-about 0.9.2',binary=str(CARGO_ABOUT),binary_sha256=digest(CARGO_ABOUT)),indent=2)+'\n')


def original_license_files(package):
    root = pathlib.Path(package['manifest_path']).parent
    paths = set()
    for directory, directories, files in os.walk(root):
        directory = pathlib.Path(directory)
        assert not any((directory/name).is_symlink() for name in directories+files)
        for name in files:
            path = directory/name;relative = path.relative_to(root)
            prefix = re.split('[-_.]',name.lower())[0]
            if prefix in {'license','licenses','licence','licences','copying','copyright','copyrights','notice','notices'} or any(part.lower() in {'licenses','licences'} for part in relative.parts[:-1]):
                paths.add(path)
    if package.get('license_file'):
        paths.add(root/package['license_file'])
    return sorted(paths)


def download_pinned_extra_notices(packages, destination):
    extra = tomllib.loads((SCRATCH/'crates/delta-arrow-reader-cli/release/extra-notices.toml').read_text())['files']
    extra += tomllib.loads((HERE/'extra-notices.toml').read_text())['files']
    locked = tomllib.loads((SCRATCH/'Cargo.lock').read_text())['package']
    result = {}
    for entry in extra:
        matching = [p for p in packages if p['name']==entry['package']]
        if not matching:
            continue
        assert len(matching)==1, 'review extra notice for multiple selected versions: '+entry['package']
        package = matching[0]
        assert package['version']==entry['version']
        assert re.fullmatch(r'https://raw\.githubusercontent\.com/[^/]+/[^/]+/[0-9a-f]{40}/[^?#]+',entry['url'])
        if 'crate_checksum' in entry:
            assert any(x['name']==package['name'] and x['version']==package['version'] and x.get('source')==package['source'] and x.get('checksum')==entry['crate_checksum'] for x in locked)
        else:
            vcs = json.loads((pathlib.Path(package['manifest_path']).parent/'.cargo_vcs_info.json').read_text())
            assert vcs['git']['sha1']==entry['url'].split('/')[5]
        path = destination/(package['name']+'-'+entry['sha256']+'.txt')
        with urllib.request.urlopen(entry['url'],timeout=60) as source:
            data = source.read(4*1024**2+1)
        assert 0<len(data)<=4*1024**2 and hashlib.sha256(data).hexdigest()==entry['sha256']
        path.write_bytes(data)
        result.setdefault(package['id'],[]).append((entry['url'],data.decode('utf-8')))
    return result


def validate_attribution_gap(package, gap, locked, vcs):
    assert package['name']==gap['name'] and package['version']==gap['version']
    assert package['license']==gap['declared_license']
    assert package['source']=='registry+https://github.com/rust-lang/crates.io-index'
    assert any(x['name']==package['name'] and x['version']==package['version'] and x.get('source')==package['source'] and x.get('checksum')==gap['registry_checksum'] for x in locked)
    assert vcs['git']['sha1']==gap['source_vcs_commit']


def read_known_attribution_gaps(packages):
    records=json.loads((HERE/'attribution-gaps.json').read_text())['packages']
    assert {r['name'] for r in records}=={'genawaiter','genawaiter-macro','genawaiter-proc-macro'} and len(records)==3
    locked=tomllib.loads((SCRATCH/'Cargo.lock').read_text())['package']
    gaps={}
    for package in packages:
        matches=[r for r in records if r['name']==package['name']]
        if matches:
            gap=matches[0]
            vcs=json.loads((pathlib.Path(package['manifest_path']).parent/'.cargo_vcs_info.json').read_text())
            validate_attribution_gap(package,gap,locked,vcs)
            gaps[package['id']]=gap
    return gaps


def read_package_original_notices(package, extra, known_gaps):
    files=[(str(path.relative_to(pathlib.Path(package['manifest_path']).parent)),path.read_text()) for path in original_license_files(package)]
    files+=extra.get(package['id'],[])
    if files:
        return files,None
    assert package['id'] in known_gaps, 'unrecorded original-attribution gap: '+package['name']
    return [],known_gaps[package['id']]


def render_notices(report, packages, extra, known_gaps):
    expected = {(p['name'],p['version']) for p in packages}
    actual = {(c['package']['name'],c['package']['version']) for c in report['crates']}
    assert actual==expected, 'license scanner graph differs from actual selected CLI tree'
    text = 'Dependency license texts generated with cargo-about 0.9.2\n'
    covered = set()
    for license in report['licenses']:
        uses = [x['crate'] for x in license['used_by']]
        covered.update((x['name'],x['version']) for x in uses)
        assert license['text'].strip() and uses
        text+='\n'+license['id']+'\nUsed by: '+', '.join(x['name']+' '+x['version'] for x in uses)+'\n\n'+license['text']+'\n'
    assert covered==expected
    originals,unavailable = [],[]
    for package in packages:
        if package['name'] in DAR_PACKAGES or package['name'] in SAIL_PACKAGES:
            continue
        files,gap=read_package_original_notices(package,extra,known_gaps)
        if gap:
            unavailable.append(gap)
            text+='\n'+package['name']+' '+package['version']+': ORIGINAL ATTRIBUTION UNAVAILABLE\n'+gap['status']+'\nPinned source: '+gap['source_vcs_commit']+'\n'
            continue
        for name,content in files:
            assert content.strip()
            text+='\n'+package['name']+' '+package['version']+': '+name+'\n\n'+content+'\n'
            originals.append(dict(package=package['id'],file=name,sha256=hashlib.sha256(content.encode()).hexdigest()))
    if any(p['name'] in SAIL_PACKAGES for p in packages):
        path=SCRATCH/'crates/delta-arrow-reader-spark-sql/vendor/sail/LICENSE'
        text+='\nVendored Sail 0.7.1 selected crates: shared LICENSE\n\n'+path.read_text()+'\n'
        originals.append(dict(package='selected Sail crates',file='vendor/sail/LICENSE',sha256=digest(path)))
    sysroot = pathlib.Path(subprocess.check_output(['rustup','run',SCHEDULE['toolchain'],'rustc','--print','sysroot'],text=True).strip())
    docs = sysroot/'share/doc/rust'
    for path in [docs/'COPYRIGHT-library.html',*sorted((docs/'licenses').iterdir())]:
        assert path.is_file() and path.read_text().strip()
        text+='\nRust standard library: '+path.name+'\n\n'+path.read_text()+'\n'
    return text, originals, unavailable


def collect_notices(graphs, destination):
    destination.mkdir()
    assert subprocess.check_output([str(CARGO_ABOUT),'--version'],text=True).strip()=='cargo-about 0.9.2'
    try:
        for name in SCHEDULE['build_order']:
            checkout_source(name)
            work=destination/name;work.mkdir()
            graph=json.loads((graphs/name/'graph.json').read_text())
            prefix=[str(CARGO_ABOUT),'generate','--locked','--fail','--manifest-path',str(SCRATCH/'crates/delta-arrow-reader-cli/Cargo.toml')]
            suffix=['--target',SCHEDULE['target'],'--format','json']
            ordinary=run_command(prefix+['--config',str(SCRATCH/'crates/delta-arrow-reader-cli/release/about.toml')]+suffix,work/'ordinary-config',1800,build_environment(SCHEDULE['toolchain']))
            configured=run_command(prefix+['--config',str(HERE/'about.toml')]+suffix,work/'measurement-config',1800,build_environment(SCHEDULE['toolchain']))
            assert configured['status']=='completed' and configured['exit_code']==0
            report=json.loads((work/'measurement-config/stdout.log').read_text())
            extra=download_pinned_extra_notices(graph['packages'],work)
            known_gaps=read_known_attribution_gaps(graph['packages'])
            text,originals,unavailable=render_notices(report,graph['packages'],extra,known_gaps)
            (work/'dependency-notices.txt').write_text(text)
            ids=set()
            for package in graph['packages']:
                expression=package.get('license')
                if expression is None and package['name'] in SAIL_PACKAGES:
                    expression='Apache-2.0'
                assert expression, 'unresolved license identifier for '+package['name']
                ids.update(x for x in re.findall(r'[A-Za-z0-9][A-Za-z0-9.+-]*',expression) if x not in {'AND','OR','WITH'})
            audit=dict(build=name,original_attribution_complete=not unavailable,unavailable_original_attribution=unavailable,attribution_gaps_sha256=digest(HERE/'attribution-gaps.json'),cargo_about_binary_sha256=digest(CARGO_ABOUT),graph_sha256=digest(graphs/name/'graph.json'),config_sha256=digest(HERE/'about.toml'),measurement_extra_notices_sha256=digest(HERE/'extra-notices.toml'),ordinary_config_exit=ordinary['exit_code'],measurement_config_exit=configured['exit_code'],license_identifiers_and_exceptions=sorted(ids),chosen_license_ids=sorted(x['id'] for x in report['licenses']),original_files=originals,dependency_notices_sha256=digest(work/'dependency-notices.txt'),missing_declared_metadata_clarified=sorted(p['name'] for p in graph['packages'] if not p.get('license')))
            (work/'audit.json').write_text(json.dumps(audit,indent=2)+'\n')
            print(name,'license evidence complete',flush=True)
    finally:
        restore_scratch_branch()


def write_archive(destination, members):
    with destination.open('xb') as output, gzip.GzipFile(filename='',fileobj=output,mode='wb',compresslevel=9,mtime=0) as compressed:
        for name,path,mode in members:
            header=tarfile.TarInfo(name);header.mode=mode;header.uid=header.gid=header.mtime=0;header.size=path.stat().st_size
            compressed.write(header.tobuf(format=tarfile.USTAR_FORMAT))
            with path.open('rb') as source:
                while chunk:=source.read(1024**2):compressed.write(chunk)
            compressed.write(b'\0'*((-header.size)%512))
        compressed.write(b'\0'*1024)
    with destination.open('rb') as source:
        header = source.read(10)
    assert header[4:8]==b'\0'*4 and header[9]==255


def package_archives(notices, builds_path, destination):
    destination.mkdir();builds=json.loads(builds_path.read_text())['builds'];results=[]
    for name in SCHEDULE['build_order']:
        original=pathlib.Path(builds[name]['binary']);assert digest(original)==builds[name]['binary_sha256']
        work=destination/name;work.mkdir()
        license_path=work/'LICENSE';license_path.write_bytes((SCRATCH/'LICENSE').read_bytes())
        notice=work/'NOTICE';notice.write_bytes((SCRATCH/'NOTICE').read_bytes()+b'\n'+(notices/name/'dependency-notices.txt').read_bytes())
        archive=work/'dar-linux-amd64-gnu.tar.gz'
        members=[('dar',original,0o755),('LICENSE',license_path,0o644),('NOTICE',notice,0o644)]
        write_archive(archive,members)
        audit=json.loads((notices/name/'audit.json').read_text())
        results.append(dict(build=name,archive_kind='measurement-local known-text archive' if not audit['original_attribution_complete'] else 'measurement-local original-text archive',original_attribution_complete=audit['original_attribution_complete'],complete_release_archive_size_verified=False,unavailable_original_attribution=[g['name'] for g in audit['unavailable_original_attribution']],source_commit=builds[name]['source_commit'],archive_bytes=archive.stat().st_size,archive_sha256=digest(archive),members=[dict(name=n,bytes=p.stat().st_size,sha256=digest(p),mode=oct(m)) for n,p,m in members],compressor=dict(python=sys.version,zlib=zlib.ZLIB_VERSION,zlib_runtime=zlib.ZLIB_RUNTIME_VERSION),note='Measurement-local Python zlib best compression, with the official member/header policy; not an official Debian release artifact.'))
    (destination/'archives.json').write_text(json.dumps(results,indent=2)+'\n')


def self_check():
    import tempfile
    with tempfile.TemporaryDirectory() as path:
        root=pathlib.Path(path);data=root/'input';data.write_bytes(b'measurement')
        archive=root/'archive.gz';write_archive(archive,[('dar',data,0o755),('LICENSE',data,0o644),('NOTICE',data,0o644)])
        with tarfile.open(archive,'r:gz') as source:
            assert source.getnames()==['dar','LICENSE','NOTICE']
            assert [(m.mode,m.uid,m.gid,m.mtime) for m in source.getmembers()]==[(0o755,0,0,0),(0o644,0,0,0),(0o644,0,0,0)]
            assert source.extractfile('dar').read()==b'measurement'
        package=dict(id='known',name='genawaiter',version='0.99.1',license='MIT',source='registry+https://github.com/rust-lang/crates.io-index',manifest_path=str(root/'Cargo.toml'),license_file=None)
        gap=dict(name='genawaiter',version='0.99.1',declared_license='MIT',registry_checksum='archive',source_vcs_commit='commit')
        locked=[dict(name='genawaiter',version='0.99.1',source=package['source'],checksum='archive')]
        validate_attribution_gap(package,gap,locked,dict(git=dict(sha1='commit')))
        assert read_package_original_notices(package,{},dict(known=gap))[1]==gap
        for invalid in [dict(package,version='0.99.2'),dict(package,source='path+file:///other')]:
            try:
                validate_attribution_gap(invalid,gap,locked,dict(git=dict(sha1='commit')))
            except AssertionError:
                pass
            else:
                raise AssertionError('changed pinned identity accepted')
        try:
            read_package_original_notices(dict(package,id='unknown'),{},dict(known=gap))
        except AssertionError:
            pass
        else:
            raise AssertionError('unrecorded missing original accepted')


if __name__=='__main__':
    self_check()
    if sys.argv[1:]==['--check']:
        print('archive member/header checks passed')
    elif len(sys.argv)==3 and sys.argv[1]=='install':
        install_tool(pathlib.Path(sys.argv[2]).resolve())
    elif len(sys.argv)==4 and sys.argv[1]=='collect':
        collect_notices(pathlib.Path(sys.argv[2]).resolve(),pathlib.Path(sys.argv[3]).resolve())
    elif len(sys.argv)==5 and sys.argv[1]=='archive':
        package_archives(*map(lambda p:pathlib.Path(p).resolve(),sys.argv[2:]))
    else:
        raise SystemExit('usage: licenses.py --check | install NEW_DIRECTORY | collect GRAPHS NEW_DIRECTORY | archive NOTICES BUILDS_JSON NEW_DIRECTORY')
