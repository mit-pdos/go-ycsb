#!/usr/bin/env python3
from os import path
import argparse
import subprocess
import re
import json
import os
import resource
import itertools
import time
import atexit
import signal

import peak_config

parser = argparse.ArgumentParser(
description="Find peak throughput of KV service for a varying number of shard servers running remotely"
)
parser.add_argument(
    "-n",
    "--dry-run",
    help="print commands without running them",
    action="store_true",
)
parser.add_argument(
    "-v",
    "--verbose",
    help="print commands in addition to running them",
    action="store_true",
)
parser.add_argument(
    "--outdir",
    help="output directory for benchmark results",
    required=True,
    default=None,
)
parser.add_argument(
    "-e",
    "--errors",
    help="print stderr from commands being run",
    action="store_true",
)
global_args = parser.parse_args()
gokvdir = ''
goycsbdir = ''

procs = []

def run_command(args, cwd=None):
    if global_args.dry_run or global_args.verbose:
        print("[RUNNING] " + " ".join(args))
    if not global_args.dry_run:
        return subprocess.run(args, capture_output=True, text=True, cwd=cwd)

def run_remote_command(host:str, cmd:str, cwd=None):
    run_command(["ssh", host, '"' + cmd + '"'])

def start_command(args, cwd=None):
    if global_args.dry_run or global_args.verbose:
        print("[STARTING] " + " ".join(args))
    if not global_args.dry_run:
        e = subprocess.PIPE
        if global_args.errors:
            e = None
        p = subprocess.Popen(args, text=True, stdout=subprocess.PIPE, stderr=e, cwd=cwd, preexec_fn=os.setsid)
        global procs
        procs.append(p)
        return p

def cleanup_procs():
    global procs
    for p in procs:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except Exception:
            continue
    procs = []

def many_cores(args, c):
    return ["numactl", "-C", c] + args

def one_core(args, c):
    return ["numactl", "-C", str(c)] + args

# Starts server on port 12345
def start_memkv_coord(initsrv):
    start_command(["go", "run",
                   "./cmd/memkvcoord", "-init", initsrv,
                   "-port", "12200"], cwd=gokvdir)
    print("[INFO] Started kv coordinator")

def install_shard_remote(host:str):
    # XXX: make sure it's as up-to-date as possible
    run_remote_command(host, "go install github.com/mit-pdos/gokv/cmd/memkvshard@latest")

def start_remote_shard_server(host:str, port:int, corelist:list[int], init:bool):
    c = ",".join([str(j) for j in corelist])
    run_remote_command(host, "nohup numactl" + c + "~/go/bin/memkvshard -port " + str(port) + (init * " -init"))
    # XXX: add check to see if it's running
    print("[INFO] Started a remote shard server with {0} cores at {1}:{2}".format(len(corelist), host, port))

def stop_remote_shard_server(host:str):
    run_remote_command(host, "killall memkvshard")

def parse_ycsb_output(output):
    # look for 'Run finished, takes...', then parse the lines for each of the operations
    # output = output[re.search("Run finished, takes .*\n", output).end():] # strip off beginning of output

    # NOTE: sample output from go-ycsb:
    # UPDATE - Takes(s): 12.6, Count: 999999, OPS: 79654.6, Avg(us): 12434, Min(us): 28, Max(us): 54145, 99th(us): 29000, 99.9th(us): 41000, 99.99th(us): 49000
    patrn = '(?P<opname>.*) - Takes\(s\): (?P<time>.*), Count: (?P<count>.*), OPS: (?P<ops>.*), Avg\(us\): (?P<avg_latency>.*), Min\(us\):.*\n' # Min(us): 28, Max(us): 54145, 99th(us): 29000, 99.9th(us): 41000, 99.99th(us): 49000'
    ms = re.finditer(patrn, output, flags=re.MULTILINE)
    a = dict()
    for m in ms:
        a[m.group('opname').strip()] = {'thruput': float(m.group('ops')), 'avg_latency': float(m.group('avg_latency')), 'raw': output}
    return a


def goycsb_bench(coord:str, threads:int, runtime:int, valuesize:int, readprop:float, updateprop:float, bench_cores:list[int]):
    """
    Returns a dictionary of the form
    { 'UPDATE': {'thruput': 1000, 'avg_latency': 12345', 'raw': 'blah'},...}
    """

    c = ",".join([str(j) for j in bench_cores])
    p = start_command(many_cores(['go', 'run',
                                  path.join(goycsbdir, './cmd/go-ycsb'),
                                  'run', 'memkv',
                                  '-P', path.join('../gokv/bench/memkv_workload'),
                                  '--threads', str(threads),
                                  '--target', '-1',
                                  '--interval', '1',
                                  '-p', 'operationcount=' + str(2**32 - 1),
                                  '-p', 'fieldlength=' + str(valuesize),
                                  '-p', 'requestdistribution=uniform',
                                  '-p', 'readproportion=' + str(readprop),
                                  '-p', 'updateproportion=' + str(updateprop),
                                  '-p', 'memkv.coord=' + coord,
                                  '-p', 'warmup=20', # TODO: increase warmup
                                  ], c), cwd=goycsbdir)

    if p is None:
        return ''

    ret = ''
    for stdout_line in iter(p.stdout.readline, ""):
        if stdout_line.find('Takes(s): {0}.'.format(runtime)) != -1:
            ret = stdout_line
            break
    p.stdout.close()
    p.terminate()
    return parse_ycsb_output(ret)

def find_peak_thruput(valuesize, outfilename, readprop, updateprop, clnt_cores):
    peak_thruput = 0
    low = 1
    cur = 1
    high = -1

    while True:
        threads = 2*low
        if high > 0:
            if (high - low) < 4:
                return threads, peak_thruput
            threads = int((low + high)/2)

        # FIXME: increase time
        a = goycsb_bench('127.0.0.1:12200', threads, 10, 128, readprop, updateprop, clnt_cores)
        p = {'service': 'memkv', 'num_threads': threads, 'ratelimit': -1, 'lts': a}

        with open(path.join(global_args.outdir, outfilename), 'a+') as outfile:
            outfile.write(json.dumps(p) + '\n')

        thput = sum([ a[op]['thruput'] for op in a ])
        if thput > peak_thruput:
            low = threads
            peak_thruput = thput
        else: # XXX: the thput might be barely smalle than peak_thruput, in which case maybe we should keep increasing # of threads
            high = threads
    return -1

def main():
    atexit.register(cleanup_procs)
    global gokvdir
    global goycsbdir
    goycsbdir = os.path.dirname(os.path.abspath(__file__))
    gokvdir = os.path.join(os.path.dirname(goycsbdir), "gokv")
    os.makedirs(global_args.outdir, exist_ok=True)
    resource.setrlimit(resource.RLIMIT_NOFILE, (100000, 100000))

    rh = 'pd7.csail.mit.edu'
    r = '18.26.5.7' # pd7
    install_shard_remote(rh)
    start_memkv_coord(r + ':12300')
    stop_remote_shard_server(rh)
    start_remote_shard_server(rh, 12300, range(1), True)

    threads, peak = find_peak_thruput(128, 'memkv_peak_raw.jsons', 0.95, 0.05, range(1,4))
    with open(path.join(global_args.outdir, 'memkv_peaks.jsons'), 'a+') as outfile:
        outfile.write(json.dumps({'name': config['name'], 'thruput':peak, 'clntthreads':threads }) + '\n')

    cleanup_procs()

if __name__=='__main__':
    main()
