#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Run the opt-in real-pipeline soak and publish its actual progress and exit verdict."""
import argparse
import datetime
import json
import os
import pathlib
import platform
import signal
import shutil
import subprocess
import time

def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def publish(folder, status):
    encoded = json.dumps(status, indent=2)
    pending = folder / 'status.json.tmp'
    pending.write_text(encoded, encoding='utf-8')
    pending.replace(folder / 'status.json')
    (folder / 'SOAK_STATUS.md').write_text(
        '# Watch Files soak\n\n'
        f"Estado: **{status['status']}**\n\n"
        f"Inicio UTC: {status['startedAt']}\n\n"
        f"Actualización UTC: {status['updatedAt']}\n\n"
        f"Duración solicitada: {status['requestedSeconds']} segundos.\n\n"
        f"Tiempo transcurrido real del supervisor: {status['elapsedSeconds']} segundos.\n\n"
        f"Último ciclo: `{json.dumps(status.get('lastCycle', {}), ensure_ascii=False)}`\n\n"
        f"Ejecutor: {status.get('executor', 'Maven')}. Log: `{status.get('log', 'maven.log')}`.\n\n"
        'El resultado solo pasa cuando el ejecutor termina correctamente y la prueba registra '
        'la duración completa, ambas estrategias, cero duplicados y cero filas inesperadas.\n', encoding='utf-8')

def resources(folder, parent_pid):
    """Read only this run's process tree, plus host counters; never record environment secrets."""
    if platform.system() != 'Linux':
        return []
    processes = {}
    for proc in pathlib.Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
            processes[int(proc.name)] = (int(fields[1]), proc)
        except (OSError, IndexError, ValueError):
            pass
    owned = {parent_pid}
    while True:
        descendants = {pid for pid, (parent, _) in processes.items() if parent in owned}
        if descendants <= owned:
            break
        owned.update(descendants)
    rows = []
    java = []
    for pid in sorted(owned):
        if pid not in processes:
            continue
        proc = processes[pid][1]
        try:
            executable = (proc / 'exe').resolve().name
            counters = {}
            for line in (proc / 'status').read_text().splitlines():
                key, _, value = line.partition(':')
                if key in ('VmRSS', 'VmHWM', 'VmSwap', 'Threads'):
                    counters[key] = value.strip()
            rows.append(dict(pid=pid, executable=executable, counters=counters))
            if executable == 'java':
                java.append(pid)
        except OSError:
            pass
    memory = {}
    for line in pathlib.Path('/proc/meminfo').read_text().splitlines():
        key, _, value = line.partition(':')
        if key in ('MemAvailable', 'MemTotal', 'SwapFree', 'SwapTotal', 'Dirty'):
            memory[key] = value.strip()
    snapshot = dict(at=utc(), memory=memory, loadAverage=pathlib.Path('/proc/loadavg').read_text().strip(),
                    diskFreeBytes=shutil.disk_usage(folder).free, processes=rows)
    with (folder / 'host-resources.jsonl').open('a', encoding='utf-8') as stream:
        stream.write(json.dumps(snapshot) + '\n')
    return java

def diagnose(folder, pids, reason):
    tool = shutil.which('jcmd')
    if not tool:
        return
    prefix = str(time.time_ns()) + '-' + reason
    for pid in pids:
        for command in ('Thread.print', 'GC.heap_info'):
            with (folder / f'{prefix}-{pid}-{command}.log').open('wb') as stream:
                try:
                    subprocess.run([tool, str(pid), command], stdout=stream, stderr=subprocess.STDOUT, timeout=15)
                except (OSError, subprocess.TimeoutExpired) as error:
                    stream.write(str(error).encode('utf-8'))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[6])
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--seconds', type=int, default=86400)
    parser.add_argument('--baseline', type=int, default=5000)
    parser.add_argument('--delivery-timeout', type=int, default=60, help='Bounded seconds per batch; tune for the test host throughput')
    parser.add_argument('--maven', default='mvn')
    parser.add_argument('--classpath', help='Exported Hop/test/JUnit classpath; run JUnit directly to avoid a resident Maven JVM')
    parser.add_argument('--java', default='java')
    parser.add_argument('--temp-directory', type=pathlib.Path, help='Test data volume; defaults to the evidence directory')
    args = parser.parse_args()
    if args.seconds < 1 or args.seconds > 259200 or args.baseline < 1 or not 1 <= args.delivery_timeout <= 900:
        parser.error('Use 1..259200 seconds, positive baseline count and 1..900 seconds delivery timeout')
    folder = args.output.resolve()
    folder.mkdir(parents=True, exist_ok=True)
    if (folder / 'status.json').exists():
        parser.error('Choose a new output directory; previous evidence is never overwritten')
    report = folder / 'samples.jsonl'
    env = os.environ.copy()
    env['JAVA_TOOL_OPTIONS'] = env.get('JAVA_TOOL_OPTIONS', '') + ' -Xmx512m'
    if args.classpath:
        temporary = (args.temp_directory or folder / 'temp').resolve()
        temporary.mkdir(parents=True, exist_ok=True)
        command = [args.java, '-Xmx512m', '-XX:+HeapDumpOnOutOfMemoryError',
                   f'-XX:HeapDumpPath={folder}', f'-XX:ErrorFile={folder / "hs_err_pid%p.log"}',
                   f'-Djava.io.tmpdir={temporary}', '-Djunit.jupiter.tempdir.cleanup.mode.default=ON_SUCCESS',
                   f'-Dwatchfiles.load.seconds={args.seconds}', f'-Dwatchfiles.load.baseline={args.baseline}',
                   f'-Dwatchfiles.load.delivery.timeout.seconds={args.delivery_timeout}',
                   f'-Dwatchfiles.load.report={report}']
        if platform.system() == 'Linux':
            command.append(f'-Xlog:gc*,safepoint:file={folder / "gc-%p.log"}:time,uptime,level,tags:filecount=4,filesize=5m')
        command += ['-cp', args.classpath, 'org.apache.hop.pipeline.transforms.watchfiles.WatchFilesTestLauncher',
                    str(folder / 'junit'), 'load']
    else:
        command = [args.maven, '-B', '-pl', 'plugins/transforms/watchfiles', '-Pskip-uitest',
               '-Dtest=WatchFilesLoadTest', f'-Dwatchfiles.load.seconds={args.seconds}',
               f'-Dwatchfiles.load.delivery.timeout.seconds={args.delivery_timeout}',
               f'-Dwatchfiles.load.baseline={args.baseline}', f'-Dwatchfiles.load.report={report}', 'test']
    if platform.system() == 'Linux':
        command = ['bash', 'tools/with-isolated-display.sh'] + command
    start = time.monotonic()
    status = dict(status='running', startedAt=utc(), requestedSeconds=args.seconds,
                  baseline=args.baseline, supervisorPid=os.getpid(), command=command,
                  deliveryTimeoutSeconds=args.delivery_timeout,
                  os=platform.platform())
    status['executor'] = 'JUnit' if args.classpath else 'Maven'
    status['log'] = 'junit.log' if args.classpath else 'maven.log'
    last_progress = time.monotonic()
    previous_progress = None
    captured_stall = False
    offset = 0
    last_cycle = None
    completed = None
    strategies = set()
    parse_errors = 0
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    with (folder / status['log']).open('wb') as log:
        child = subprocess.Popen(command, cwd=args.repo.resolve(), env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=(platform.system() == 'Linux'))
        status['buildPid'] = child.pid
        try:
            while True:
                # Observe exit before reading telemetry so a finished run includes its final row.
                result = child.poll()
                if report.exists():
                    # Keep constant memory even after a full day of telemetry.
                    with report.open(encoding='utf-8') as stream:
                        stream.seek(offset)
                        while True:
                            line = stream.readline()
                            if not line.endswith('\n'):
                                break
                            offset = stream.tell()
                            try:
                                row = json.loads(line)
                            except json.JSONDecodeError:
                                parse_errors += 1
                                continue
                            status['lastTelemetry'] = row
                            if row.get('event') in ('cycle', 'progress'):
                                progress = (row['consumed'], row['strategy'])
                                if progress != previous_progress:
                                    last_progress = time.monotonic()
                                    previous_progress = progress
                                    captured_stall = False
                            elif row.get('event') == 'ready':
                                last_progress = time.monotonic()
                                captured_stall = False
                            if row.get('event') == 'cycle':
                                last_cycle = row
                                strategies.add(row['strategy'])
                            elif row.get('event') == 'completed':
                                completed = row
                if last_cycle:
                    status['lastCycle'] = last_cycle
                pids = resources(folder, child.pid)
                if time.monotonic() - last_progress > 180 and not captured_stall:
                    diagnose(folder, pids, 'no-progress')
                    captured_stall = True
                status['updatedAt'] = utc()
                status['elapsedSeconds'] = int(time.monotonic() - start)
                if result is not None:
                    status['exitCode'] = result
                    status['telemetryParseErrors'] = parse_errors
                    passed = (result == 0 and completed and completed['elapsedSeconds'] >= args.seconds
                              and completed['duplicates'] == 0
                              and completed.get('unexpected', -1) == 0
                              and {'NATIVE', 'POLLING'} <= strategies and parse_errors == 0)
                    status['status'] = 'passed' if passed else 'failed'
                    publish(folder, status)
                    return 0 if passed else 1
                publish(folder, status)
                time.sleep(30)
        except BaseException:
            if child.poll() is None:
                if platform.system() == 'Linux':
                    try:
                        os.killpg(child.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                else:
                    child.terminate()
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                if platform.system() == 'Linux':
                    os.killpg(child.pid, signal.SIGKILL)
                else:
                    child.kill()
                child.wait(timeout=10)
            status.update(status='interrupted', updatedAt=utc(), elapsedSeconds=int(time.monotonic() - start))
            publish(folder, status)
            raise

if __name__ == '__main__':
    raise SystemExit(main())
