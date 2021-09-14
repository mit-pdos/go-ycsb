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
        time.sleep(0.3)
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


def profile_goycsb_bench(threads:int, runtime:int, valuesize:int, readprop:float, updateprop:float, bench_cores:list[int]):
    """
    Returns a dictionary of the form
    { 'UPDATE': {'thruput': 1000, 'avg_latency': 12345', 'raw': 'blah'},...}
    """

    warmup_time = 10
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
                                  '-p', 'warmup=' + str(warmup_time), # TODO: increase warmup
                                  ], c), cwd=goycsbdir)
    if p is None:
        return ''

    time.sleep(warmup_time + 3)
    run_command(["wget", "-O", "prof.out", "http://localhost:6060/debug/pprof/trace?seconds=60"])
    p.stdout.close()
    p.terminate()

def main():
    atexit.register(cleanup_procs)
    global gokvdir
    global goycsbdir
    goycsbdir = os.path.dirname(os.path.abspath(__file__))
    gokvdir = os.path.join(os.path.dirname(goycsbdir), "gokv")
    os.makedirs(global_args.outdir, exist_ok=True)

    # Profile for 1 core
    start_memkv_multiserver([range(1)])
    time.sleep(1.0)
    profile_goycsb_bench(50, 10, 128, 0.95, 0.05, range(40,80))
    cleanup_procs()

    # Profile for 10 cores
    start_memkv_multiserver([range(10)])
    time.sleep(1.0)
    profile_goycsb_bench(500, 10, 128, 0.95, 0.05, range(40,80))
    cleanup_procs()

if __name__=='__main__':
    main()
