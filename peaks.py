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

from peak_config import *

parser = argparse.ArgumentParser(
description="Find peak throughput of KV service for a varying number of shard servers"
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
def start_memkv_multiserver(config:list[list[int]]):
    """
    Given a list of lists of cores for each shard server, this brings up the kv
    system
    """
    start_command(["go", "run",
                   "./cmd/memkvcoord", "-init",
                   "127.0.0.1:12300", "-port", "12200"], cwd=gokvdir)

    for i, corelist in enumerate(config):
        start_shard_multicore(12300 + i, corelist, i == 0)
        time.sleep(1.0)
        if i > 0:
            run_command(["go", "run", "./cmd/memkvctl", "-coord", "127.0.0.1:12200", "add", "127.0.0.1:" + str(12300 + i)], cwd=gokvdir)
    print("[INFO] Started kv service with {0} server(s)".format(len(config)))

def start_shard_multicore(port:int, corelist:list[int], init:bool):
    c = ",".join([str(j) for j in corelist])
    if init:
        start_command(many_cores(["go", "run", "./cmd/memkvshard", "-init", "-port", str(port)], c), cwd=gokvdir)
    else:
        start_command(many_cores(["go", "run", "./cmd/memkvshard", "-port", str(port)], c), cwd=gokvdir)
    print("[INFO] Started a shard server with {0} cores on port {1}".format(len(corelist), port))

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


def goycsb_bench(threads:int, runtime:int, valuesize:int, readprop:float, updateprop:float, bench_cores:list[int]):
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
                                  '-p', 'memkv.coord=127.0.0.1:12200',
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

def find_peak_thruput2(kvname, valuesize, outfilename, readprop, updateprop, clnt_cores):
    peak_thruput = 0
    low = 1
    cur = 1
    high = -1

    # Find range of size b^n for optimal # of threads
    # Then, within the range, find the subrange of size b^(n-1)
    # Keep going until some desired accuracy (e.g. 10 threads).
    n = 3
    b = 10
    low = 1
    high = -1
    threads = 1
    while True:
        # FIXME: increase time
        a = goycsb_bench(threads, 60, 128, readprop, updateprop, clnt_cores)

        p = {'service': kvname, 'num_threads': threads, 'lts': a}
        with open(path.join(global_args.outdir, outfilename), 'a+') as outfile:
            outfile.write(json.dumps(p) + '\n')

        thput = sum([ a[op]['thruput'] for op in a ])
        if thput > peak_thruput:
            low = threads
            peak_thruput = thput
        elif thput < peak_thruput * 0.95: # XXX: 0.95 is the margin for error in being certain that perf is going down
            high = threads

        threads += (b**n)
    return -1

def find_peak_thruput(kvname, valuesize, outfilename, readprop, updateprop, clnt_cores):
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
        a = goycsb_bench(threads, 10, 128, readprop, updateprop, clnt_cores)
        p = {'service': kvname, 'num_threads': threads, 'ratelimit': -1, 'lts': a}

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

    for config in peak_config.configs:
        time.sleep(0.5)
        ps = start_memkv_multiserver(config['srvs'])
        time.sleep(0.5)
        threads, peak = find_peak_thruput('memkv', 128, 'memkv_peak_raw.jsons', 0.95, 0.05, config['clnts'])
        with open(path.join(global_args.outdir, 'memkv_peaks.jsons'), 'a+') as outfile:
            outfile.write(json.dumps({'name': config['name'], 'thruput':peak, 'clntthreads':threads }) + '\n')

        cleanup_procs()

if __name__=='__main__':
    main()
