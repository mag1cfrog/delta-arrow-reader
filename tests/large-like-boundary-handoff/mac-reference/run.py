"""Fixed three-case LIKE helper capture under the approved inherited host lease."""
from pathlib import Path
import hashlib
import json
import os
import platform
import pwd
import re
import shutil
import signal
import subprocess
import sys
import time
import tomllib
import uuid

ROOT = Path('/Users/hanbowang/Documents/codex-evidence/c06-2026-11-02')
PACKET = ROOT / '771-large-helper-immutable-r2'
OUT = ROOT / '771-large-helper-immutable-r2-actual'
VALIDATOR = Path('/Users/hanbowang/Documents/repos/delta-arrow-reader-mac-validator')
CRATE = VALIDATOR / 'crates/delta-arrow-reader-spark-sql'
ADAPTER = ROOT / 'mac-shared-engine-adapter'
PINNED_CARGO_HOME = Path('/Users/hanbowang/.cargo')
PINNED_RUSTUP_HOME = Path('/Users/hanbowang/.rustup')
CASES = ('aggregate_large_utf8', 'dynamic_view_expansion', 'scalar_view_expansion')
BUDGETS = dict(zip(CASES, (8 * 1024**3, 7 * 1024**3, 11 * 1024**3)))
RESERVE = 2 * 1024**3
BOUNDARY = 2**31
COMMAND = ['cargo', '+1.96.0', 'test', '--locked', '--workspace', '--lib', '--no-run', '--message-format=json']
BUILD_TIMEOUT = 3600
CASE_TIMEOUT = 1800
BEGIN = 'c06 large boundary begin: '
COMPLETE = 'c06 large boundary complete: '


def require(value, message):
    if not value:
        raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def sha_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def validate_cargo_environment(env):
    approved = {'CARGO_BUILD_JOBS': '6', 'CARGO_TARGET_DIR': '/Users/hanbowang/.cache/dar-c06-target'}
    sensitive = {'RUSTFLAGS', 'RUSTDOCFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_ENCODED_RUSTDOCFLAGS',
                 'RUSTC', 'RUSTDOC', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'RUSTC_BOOTSTRAP',
                 'CARGO_INCREMENTAL', 'CARGO_MAKEFLAGS', 'MAKEFLAGS', 'CARGO'}
    for key in env:
        if key in sensitive or key.startswith(('CARGO_BUILD_', 'CARGO_PROFILE_', 'CARGO_TARGET_')):
            require(key in approved and env[key] == approved[key], 'unagreed build override: ' + key)
    require(all(env.get(key) == value for key, value in approved.items()), 'fixed Cargo jobs/target missing')
    if 'RUSTUP_HOME' in env:
        require(bool(env['RUSTUP_HOME']), 'empty RUSTUP_HOME reset is not agreed')
        rustup = Path(env['RUSTUP_HOME'])
    else:
        rustup = Path(env['HOME'] if 'HOME' in env else pwd.getpwuid(os.getuid()).pw_dir) / '.rustup'
    if not rustup.is_absolute():
        rustup = Path.cwd() / rustup
    require(rustup.resolve(strict=True) == PINNED_RUSTUP_HOME, 'Rustup home differs from pinned installed toolchain home')
    return {**{key: env.get(key) for key in ('HOME', 'CARGO_HOME', 'RUSTUP_HOME', 'RUSTUP_TOOLCHAIN', *approved)},
            'rustup_home_resolved': str(rustup.resolve(strict=True))}


def cargo_configuration(crate, repo, env):
    crate, repo = Path(crate).resolve(), Path(repo).resolve()
    home = env.get('CARGO_HOME')
    if home:
        home = Path(home)
    else:
        home = Path(env['HOME'] if 'HOME' in env else pwd.getpwuid(os.getuid()).pw_dir) / '.cargo'
    if not home.is_absolute():
        home = crate / home
    roots = [crate, *crate.parents, home]
    paths = {root / '.cargo' / name for root in roots[:-1] for name in ('config', 'config.toml')}
    paths |= {roots[-1] / name for name in ('config', 'config.toml')}
    result = {}
    for path in sorted(paths):
        require(not path.is_symlink() or path.exists(), 'dangling Cargo configuration symlink: ' + str(path))
        if not path.exists():
            continue
        resolved = path.resolve(strict=True)
        raw = path.read_bytes()
        data = tomllib.loads(raw.decode('utf-8'))
        if not resolved.is_relative_to(repo):
            require(not (set(data) - {'registries', 'registry', 'net', 'http', 'credential-alias'}), 'unagreed external Cargo configuration: ' + str(path))
        result[str(path)] = {'sha256': sha(raw), 'top_level_keys': sorted(data), 'resolved_path': str(resolved),
                             'effective': path.name == 'config' or not path.with_name('config').exists()}
    return {'cargo_home': str(home), 'cargo_home_resolved': str(home.resolve()), 'files': result}


def archive_executable(source, destination):
    digest = sha_file(source)
    with source.open('rb') as src, destination.open('xb') as dst:
        shutil.copyfileobj(src, dst)
    require(sha_file(source) == sha_file(destination) == digest, 'executable changed during archival')
    destination.chmod(0o555)
    return {'path': str(destination), 'sha256': digest, 'size': destination.stat().st_size,
            'cargo_path': str(source), 'cargo_sha256_at_archive': digest, 'mode': oct(destination.stat().st_mode & 0o777)}


def parse_memory(vm, memsize):
    values = [re.findall(pattern, vm, re.M) for pattern in (
        r'page size of (\d+) bytes', r'^Pages free:\s+(\d+)\.',
        r'^Pages speculative:\s+(\d+)\.')]
    require(all(len(value) == 1 for value in values), 'ambiguous vm_stat output')
    page, free, speculative = (int(value[0]) for value in values)
    physical = int(memsize.strip())
    available = page * (free + speculative)
    require(page > 0 and 0 <= available <= physical, 'invalid memory snapshot')
    return {'physical_bytes': physical, 'page_size': page, 'free_pages': free,
            'speculative_pages': speculative, 'free_plus_speculative_bytes': available}


def admission(snapshot):
    return [{'case': case, 'process_budget_bytes': BUDGETS[case], 'system_reserve_bytes': RESERVE,
             'threshold_bytes': BUDGETS[case] + RESERVE,
             'resource_eligible': snapshot['free_plus_speculative_bytes'] >= BUDGETS[case] + RESERVE}
            for case in CASES]


def validate_observation(observed, case, token):
    """Check the bounded summary of exhaustive native Rust assertions, not invented payloads."""
    require(observed.get('case') == case and observed.get('run_token') == token, 'observation identity mismatch')
    require(observed.get('complete') is True, 'observer did not finish')
    require(observed.get('proof_kind') == 'real-rewrite-and-representation-only', 'wrong proof scope')
    require(observed.get('regex_matching') is False and observed.get('apache_spark_observation') is False,
            'unexpected regex or Spark claim')
    expected_source = 'Utf8View scalar' if case == CASES[2] else 'LargeUtf8 scalar'
    require(observed.get('source_type') == expected_source and observed.get('source_value') == 'z', 'source scalar changed')
    before, after = observed['input'], observed['output']

    def large(value, rows, width):
        expected = {'type': 'LargeUtf8', 'rows': rows, 'null_count': 0,
                    'actual_payload_bytes': rows * width, 'final_offset': rows * width,
                    'all_offsets_checked': rows + 1, 'all_validity_rows_checked': rows,
                    'complete_bytes_checked': rows * width, 'arrow_validate_full': True}
        require(all(value.get(key) == actual for key, actual in expected.items()), 'incomplete LargeUtf8 evidence')

    def views(value, rows, width):
        expected = {'type': 'Utf8View', 'rows': rows, 'null_count': 0, 'actual_logical_bytes': rows * width,
                    'all_views_checked': rows, 'all_validity_rows_checked': rows,
                    'complete_logical_bytes_checked': rows * width, 'arrow_validate_full': True}
        require(all(value.get(key) == actual for key, actual in expected.items()), 'incomplete Utf8View evidence')
        require(value['actual_backing_bytes'] >= width, 'missing real view backing')

    if case == CASES[0]:
        large(before, 2048, 1048576)
        views(before['construction_views'], 2048, 1048576)
        large(after, 2048, 1048576)
    elif case == CASES[1]:
        views(before, 262144, 4096)
        large(after, 262144, 8192)
    elif case == CASES[2]:
        require(before['type'] == 'Utf8View scalar' and before['null'] is False, 'wrong scalar input')
        require(before['actual_bytes'] == before['complete_bytes_checked'] == BOUNDARY // 2, 'incomplete scalar input')
        require(before['actual_capacity'] >= BOUNDARY // 2, 'invalid scalar capacity')
        views(after, 1, BOUNDARY)
        expected = {'returned_scalar_type': 'Utf8View', 'returned_scalar_bytes_checked': BOUNDARY,
                    'actual_view_length': BOUNDARY, 'actual_view_buffer_index': 0, 'actual_view_offset': 0,
                    'view_uses_returned_owned_bytes': True}
        require(all(after.get(key) == actual for key, actual in expected.items()), 'incomplete returned scalar evidence')
        require(after['actual_backing_bytes'] == BOUNDARY and after['returned_scalar_capacity'] >= BOUNDARY,
                'returned scalar storage changed')
    else:
        raise RuntimeError('unknown fixed case')


def observations(raw, case, token):
    begins, ends = [], []
    for line in raw.decode('utf-8').splitlines():
        if BEGIN in line:
            begins.append(json.loads(line.split(BEGIN, 1)[1]))
        if COMPLETE in line:
            ends.append(json.loads(line.split(COMPLETE, 1)[1]))
    require(len(begins) == len(ends) == 1, 'expected exactly one begin and complete record')
    require(begins[0].get('case') == case and begins[0].get('run_token') == token, 'begin identity mismatch')
    require(begins[0].get('boundary_bytes') == BOUNDARY, 'begin boundary mismatch')
    validate_observation(ends[0], case, token)
    return {'begin': begins[0], 'complete': ends[0]}


def main():
    require(len(sys.argv) == 1 and Path(__file__).resolve() == PACKET / 'run.py', 'fixed runner accepts no arguments or relocation')
    require(not sys.flags.optimize and platform.system() == 'Darwin', 'requires unoptimized Python on macOS')
    lineage = json.loads((PACKET / 'LINEAGE.json').read_bytes())
    seal = json.loads((PACKET / 'SEAL.json').read_bytes())
    for name, expected in seal['files'].items():
        require(sha_file(PACKET / name) == expected, 'sealed packet changed: ' + name)
    require(sha_file(ADAPTER / 'run_locked.py') == lineage['adapter_sha256'], 'wrong approved adapter')
    require(sha_file(lineage['coordination_path']) == lineage['coordination_sha256'], 'coordination changed')
    policy = json.loads((PACKET / 'input-and-resource-policy.json').read_bytes())
    require(sha_file(PACKET / 'input-and-resource-policy.json') == lineage['fixed_policy_sha256'], 'fixed v2 policy changed')
    require(set(policy['cases']) == set(CASES) and policy['boundary_bytes'] == BOUNDARY and
            policy['system_reserve_bytes'] == RESERVE, 'policy inventory changed')
    require(all(policy['cases'][case]['process_budget_bytes'] == BUDGETS[case] for case in CASES), 'process budgets changed')
    for name, pin in policy['registry_source_pins'].items():
        require(sha_file(pin['path']) == pin['sha256'], 'locked registry source changed: ' + name)
    cargo_environment = validate_cargo_environment(os.environ)
    sys.path.insert(0, str(ADAPTER))
    from inherited_lock import borrow_controller_lock, inherited_fds, install_signal_cleanup, verify_owned_fd
    lease, identity = borrow_controller_lock(VALIDATOR)
    install_signal_cleanup()
    active = None
    manifest = None
    failure = None
    source_before = None
    raw_sequence = 0

    def git(*args):
        return subprocess.check_output(['git', '--no-optional-locks', '-C', str(VALIDATOR), *args], text=True).strip()

    def source_identity():
        require(git('status', '--porcelain', '--untracked-files=all') == '', 'validator source is dirty')
        pins = json.loads((PACKET / 'source-pins.json').read_bytes())
        require(git('rev-parse', 'HEAD') == pins['commit'] == lineage['observer_commit'], 'observer commit changed')
        require(git('rev-parse', 'HEAD^{tree}') == pins['tree'] == lineage['observer_tree'], 'observer tree changed')
        for record in pins['files']:
            require(sha_file(VALIDATOR / record['path']) == record['sha256'], 'actual source changed: ' + record['path'])
        index = Path(git('rev-parse', '--path-format=absolute', '--git-path', 'index'))
        return {'head': pins['commit'], 'tree': pins['tree'], 'status_porcelain': '',
                'source_pins_sha256': sha_file(PACKET / 'source-pins.json'), 'files_checked': len(pins['files']),
                'index_sha256': sha_file(index)}

    def configuration_snapshot():
        config = cargo_configuration(CRATE, VALIDATOR, os.environ)
        require(Path(config['cargo_home_resolved']) == PINNED_CARGO_HOME, 'Cargo home differs from pinned registry source home')
        reviewed = {record['path']: record['sha256'] for record in
                    json.loads((PACKET / 'source-pins.json').read_bytes())['files']}
        for record in config['files'].values():
            resolved = Path(record['resolved_path'])
            if resolved.is_relative_to(VALIDATOR.resolve()):
                relative = str(resolved.relative_to(VALIDATOR.resolve()))
                require(reviewed.get(relative) == record['sha256'], 'unreviewed in-repository Cargo configuration: ' + relative)
        return config

    def save():
        temp = OUT / 'manifest.tmp'
        temp.write_text(json.dumps(manifest, indent=2) + '\n')
        os.replace(temp, OUT / 'manifest.json')

    def raw_command(label, argv):
        nonlocal raw_sequence
        raw_sequence += 1
        label = str(raw_sequence).zfill(5) + '-' + label
        try:
            result = subprocess.run(argv, capture_output=True, timeout=20, check=False, pass_fds=inherited_fds())
            code, stdout, stderr = result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired as error:
            code, stdout, stderr = None, error.stdout or b'', error.stderr or b''
        for stream in ('stdout', 'stderr'):
            with (OUT / (label + '.' + stream)).open('xb') as saved:
                saved.write(stdout if stream == 'stdout' else stderr)
        record = {'label': label, 'argv': argv, 'exit': code, 'timeout_seconds': 20, 'timed_out': code is None,
                  'stdout_sha256': sha(stdout), 'stderr_sha256': sha(stderr)}
        manifest['raw_commands'].append(record)
        require(code == 0, 'captured command failed or timed out: ' + label)
        return stdout.decode('ascii')

    def memory_snapshot():
        vm = raw_command('vm_stat', ['/usr/bin/vm_stat'])
        physical = raw_command('memsize', ['/usr/sbin/sysctl', '-n', 'hw.memsize'])
        return parse_memory(vm, physical)

    def group_rss():
        raw = raw_command('ps', ['/bin/ps', '-ax', '-o', 'pgid=,rss='])
        return sum(int(parts[1]) * 1024 for line in raw.splitlines()
                   if len(parts := line.split()) == 2 and int(parts[0]) == os.getpgrp())

    def reap():
        nonlocal active
        if active is None:
            return
        if active.poll() is None:
            active.terminate()
            try:
                active.wait(timeout=5)
            except subprocess.TimeoutExpired:
                active.kill()
                active.wait()
        else:
            active.wait()
        active = None

    try:
        source_before = source_identity()
        require(Path.cwd().resolve() == CRATE, 'wrong controller cwd')
        require(os.getpgrp() == os.getpid(), 'runner must lead the controller child group')
        OUT.mkdir(exist_ok=False)
        manifest = {'role': 'actual native helper observations only; root must audit outer closeout',
                    'lineage_sha256': sha_file(PACKET / 'LINEAGE.json'), 'packet_seal_sha256': sha_file(PACKET / 'SEAL.json'),
                    'source_before': source_before, 'lock': identity, 'case_inventory': list(CASES),
                    'policy_sha256': sha_file(PACKET / 'input-and-resource-policy.json'),
                    'command': COMMAND, 'build_jobs': os.environ.get('CARGO_BUILD_JOBS'),
                    'shared_target': os.environ.get('CARGO_TARGET_DIR'), 'child_process_group': os.getpgrp(),
                    'cargo_environment': cargo_environment,
                    'raw_commands': [], 'case_results': [{'case': case, 'status': 'not-executed', 'executed': False,
                                                        'passed': False, 'acceptance_credit': False} for case in CASES],
                    'execution_state': 'resource-admission', 'local_postchecks_passed': False,
                    'probes_executed': False, 'acceptance_credit': False,
                    'sql_fields_available': False, 'ansi_exercised': False, 'final_boolean_exercised': False}
        require(manifest['build_jobs'] == '6' and manifest['shared_target'] == '/Users/hanbowang/.cache/dar-c06-target', 'wrong shared build settings')
        cache = Path(manifest['shared_target'])
        manifest['cache_identity'] = {'resolved_path': str(cache.resolve()), 'device': cache.stat().st_dev, 'inode': cache.stat().st_ino}
        save()
        manifest['cargo_configuration_before'] = configuration_snapshot()
        save()
        initial = memory_snapshot()
        manifest['initial_snapshot'] = initial
        manifest['initial_admission'] = admission(initial)
        save()
        if not any(record['resource_eligible'] for record in manifest['initial_admission']):
            manifest['execution_state'] = 'unavailable-no-case-eligible'
            manifest['case_results'] = [{'case': case, 'status': 'unavailable-resource-preflight', 'executed': False,
                                         'passed': False, 'acceptance_credit': False} for case in CASES]
            return 2
        manifest['rustc_verbose'] = raw_command('rustc-version', ['rustc', '+1.96.0', '--version', '--verbose'])
        manifest['cargo_version'] = raw_command('cargo-version', ['cargo', '+1.96.0', '--version'])
        require(manifest['rustc_verbose'].startswith('rustc 1.96.0 ') and
                manifest['cargo_version'].startswith('cargo 1.96.0 '), 'wrong Rust/Cargo version')
        manifest['tool_executables'] = {}
        for tool in ('rustc', 'cargo'):
            path = Path(raw_command(tool + '-path', ['rustup', 'which', '--toolchain', '1.96.0', tool]).strip())
            require(path.is_absolute(), 'toolchain executable must be absolute')
            manifest['tool_executables'][tool] = {'path': str(path), 'sha256': sha_file(path)}
        manifest['cargo_configuration_prebuild'] = configuration_snapshot()
        require(manifest['cargo_configuration_prebuild'] == manifest['cargo_configuration_before'], 'effective Cargo configuration changed before build')
        require(validate_cargo_environment(os.environ) == cargo_environment, 'Cargo environment changed before build')
        manifest['execution_state'] = 'building-immutable-observer'
        save()
        start = time.monotonic()
        with (OUT / 'build.stdout').open('xb') as stdout, (OUT / 'build.stderr').open('xb') as stderr:
            active = subprocess.Popen(COMMAND, cwd=CRATE, stdout=stdout, stderr=stderr, pass_fds=inherited_fds())
            active.wait(timeout=BUILD_TIMEOUT)
            code = active.returncode
            active = None
        manifest['build'] = {'exit': code, 'seconds': time.monotonic() - start,
                             'stdout_sha256': sha_file(OUT / 'build.stdout'), 'stderr_sha256': sha_file(OUT / 'build.stderr')}
        manifest['cargo_configuration_postbuild'] = configuration_snapshot()
        require(manifest['cargo_configuration_postbuild'] == manifest['cargo_configuration_before'], 'effective Cargo configuration changed during build')
        save()
        require(code == 0, 'observer build failed')
        artifacts = []
        for line in (OUT / 'build.stdout').read_text().splitlines():
            message = json.loads(line)
            if (message.get('reason') == 'compiler-artifact' and message.get('executable') and
                    Path(message.get('manifest_path', '')).resolve() == CRATE / 'vendor/sail/crates/sail-function/Cargo.toml' and
                    message['profile']['test'] and message['target']['kind'] == ['lib']):
                artifacts.append((Path(message['executable']).resolve(), message))
        require(len(artifacts) == 1 and artifacts[0][0].is_relative_to(cache.resolve()), 'expected one sail-function unit artifact in shared target')
        manifest['unit_executable'] = archive_executable(artifacts[0][0], OUT / 'sail-function-observer')
        manifest['unit_executable']['cargo_artifact'] = artifacts[0][1]
        executable = Path(manifest['unit_executable']['path'])
        for case in CASES:
            check = admission(memory_snapshot())[CASES.index(case)]
            record = manifest['case_results'][CASES.index(case)]
            record['preflight'] = check
            if not check['resource_eligible']:
                record['status'] = 'unavailable-resource-preflight'
                save()
                continue
            token = str(uuid.uuid4())
            require(sha_file(executable) == manifest['unit_executable']['sha256'], 'retained observer executable changed')
            env = dict(os.environ, C06_ENABLE_REAL_LARGE_BOUNDARY='1', C06_LARGE_BOUNDARY_CASE=case,
                       C06_LARGE_BOUNDARY_RUN_TOKEN=token)
            name = 'scalar::predicate::spark_like::c06_large_boundary_observer::' + case
            argv = [str(executable), name, '--exact', '--ignored', '--nocapture', '--test-threads=1']
            record.update(argv=argv, run_token=token, timeout_seconds=CASE_TIMEOUT, memory_samples=[])
            manifest['execution_state'] = 'observing-' + case
            save()
            start = time.monotonic()
            reason = None
            killed = None
            next_sample = 0.0
            with (OUT / (case + '.log')).open('xb') as stream:
                active = subprocess.Popen(argv, cwd=CRATE, env=env, stdout=stream, stderr=subprocess.STDOUT, pass_fds=inherited_fds())
                record['executed'] = manifest['probes_executed'] = True
                while True:
                    pid, status, usage = os.wait4(active.pid, os.WNOHANG)
                    if pid:
                        active.returncode = os.waitstatus_to_exitcode(status)
                        break
                    elapsed = time.monotonic() - start
                    if elapsed >= next_sample:
                        rss, snapshot = group_rss(), memory_snapshot()
                        record['memory_samples'].append({'seconds': elapsed, 'group_rss_bytes': rss, 'snapshot': snapshot})
                        next_sample = elapsed + 1.0
                        if rss > BUDGETS[case]:
                            reason = reason or 'unavailable-process-memory-budget'
                        if snapshot['free_plus_speculative_bytes'] < RESERVE:
                            reason = reason or 'unavailable-system-memory-reserve'
                        save()
                    if elapsed > CASE_TIMEOUT:
                        reason = reason or 'unavailable-timeout'
                    if reason and killed is None:
                        active.terminate()
                        killed = time.monotonic()
                    elif killed is not None and time.monotonic() - killed > 5:
                        active.kill()
                    time.sleep(0.25)
                record.update(exit=active.returncode, seconds=time.monotonic() - start,
                              rusage_maxrss_bytes=usage.ru_maxrss, rusage_maxrss_units='bytes on macOS',
                              user_seconds=usage.ru_utime, system_seconds=usage.ru_stime)
                active = None
            record['log_sha256'] = sha_file(OUT / (case + '.log'))
            if record['rusage_maxrss_bytes'] > BUDGETS[case]:
                reason = reason or 'unavailable-measured-memory-budget'
            if reason:
                record['status'] = reason
            elif record['exit'] < 0:
                record['status'] = 'unavailable-signal-' + str(-record['exit'])
            elif record['exit'] != 0:
                record['status'] = 'failed-boundary-observer'
            else:
                record['observation'] = observations((OUT / (case + '.log')).read_bytes(), case, token)
                record.update(status='passed-real-rewrite-and-representation-only', passed=True)
            save()
        manifest['execution_state'] = 'observations-finished'
        return 0 if all(record['passed'] for record in manifest['case_results']) else 2
    except BaseException as error:
        failure = repr(error)
        if manifest is not None:
            manifest['execution_error'] = failure
        raise
    finally:
        try:
            reap()
            if manifest is not None:
                try:
                    manifest['cargo_configuration_after'] = configuration_snapshot()
                    require(manifest['cargo_configuration_after'] == manifest.get('cargo_configuration_before'), 'effective Cargo configuration changed at closeout')
                    require(validate_cargo_environment(os.environ) == cargo_environment, 'Cargo environment changed at closeout')
                    if 'unit_executable' in manifest:
                        manifest['unit_executable_after_sha256'] = sha_file(Path(manifest['unit_executable']['path']))
                        require(manifest['unit_executable_after_sha256'] == manifest['unit_executable']['sha256'], 'retained observer executable changed at closeout')
                    manifest['source_after'] = source_identity()
                    manifest['source_unchanged'] = manifest['source_after'] == source_before
                    require(manifest['source_unchanged'], 'source/index identity changed; no acceptance credit')
                    manifest['lock_after'] = verify_owned_fd(identity['inherited_fd'], identity['path'])
                    require(sha_file(lineage['coordination_path']) == lineage['coordination_sha256'], 'coordination changed')
                    manifest['local_postchecks_passed'] = failure is None
                except BaseException as error:
                    manifest['postcheck_error'] = repr(error)
                    raise
                finally:
                    save()
        finally:
            # Children share this runner's group. The approved outer controller
            # waits for the whole group after this process exits, before releasing its lease.
            lease.close()


if __name__ == '__main__':
    raise SystemExit(main())
