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
import threading

from shard_config import *

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
        # if i > 0:
            # run_command(["go", "run", "./cmd/memkvctl", "-coord", "127.0.0.1:12200", "add", "127.0.0.1:" + str(12300 + i)], cwd=gokvdir)
    print("[INFO] Started kv service with {0} server(s)".format(len(config)))

def start_shard_multicore(port:int, corelist:list[int], init:bool):
    c = ",".join([str(j) for j in corelist])
    if init:
        start_command(many_cores(["go", "run", "./cmd/memkvshard", "-init", "-port", str(port)], c), cwd=gokvdir)
    else:
        start_command(many_cores(["go", "run", "./cmd/memkvshard", "-port", str(port)], c), cwd=gokvdir)
    print("[INFO] Started a shard server with {0} cores on port {1}".format(len(corelist), port))

def parse_ycsb_output_totalops(output):
    # look for 'Run finished, takes...', then parse the lines for each of the operations
    # output = output[re.search("Run finished, takes .*\n", output).end():] # strip off beginning of output

    # NOTE: sample output from go-ycsb:
    # UPDATE - Takes(s): 12.6, Count: 999999, OPS: 79654.6, Avg(us): 12434, Min(us): 28, Max(us): 54145, 99th(us): 29000, 99.9th(us): 41000, 99.99th(us): 49000
    patrn = '(?P<opname>.*) - Takes\(s\): (?P<time>.*), Count: (?P<count>.*), OPS: (?P<ops>.*), Avg\(us\): (?P<avg_latency>.*), Min\(us\):.*\n' # Min(us): 28, Max(us): 54145, 99th(us): 29000, 99.9th(us): 41000, 99.99th(us): 49000'
    ms = re.finditer(patrn, output, flags=re.MULTILINE)
    a = 0
    time = None
    for m in ms:
        a += int(m.group('count'))
        time = float(m.group('time'))
    return (time, a)

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
                                  '--interval', '500',
                                  '-p', 'operationcount=' + str(2**32 - 1),
                                  '-p', 'fieldlength=' + str(valuesize),
                                  '-p', 'requestdistribution=uniform',
                                  '-p', 'readproportion=' + str(readprop),
                                  '-p', 'updateproportion=' + str(updateprop),
                                  '-p', 'memkv.coord=127.0.0.1:12200',
                                  '-p', 'warmup=10',
                                  '-p', 'recordcount=100000',
                                  ], c), cwd=goycsbdir)
    if p is None:
        return ''

    totalopss = []
    for stdout_line in iter(p.stdout.readline, ""):
        t,a = (parse_ycsb_output_totalops(stdout_line))
        if t:
            totalopss.append((t,a))
        if stdout_line.find('Takes(s): {0}.'.format(runtime)) != -1:
            ret = stdout_line
            break
    p.stdout.close()
    p.terminate()
    return totalopss

def add_servers():

    time.sleep(10) # warmup

    time.sleep(30)
    run_command(["go", "run", "./cmd/memkvctl", "-coord", "127.0.0.1:12200", "add", "127.0.0.1:12301"], cwd=gokvdir)

    time.sleep(30)
    run_command(["go", "run", "./cmd/memkvctl", "-coord", "127.0.0.1:12200", "add", "127.0.0.1:12302"], cwd=gokvdir)

    time.sleep(30)
    run_command(["go", "run", "./cmd/memkvctl", "-coord", "127.0.0.1:12200", "add", "127.0.0.1:12303"], cwd=gokvdir)

    time.sleep(30)
    return

def main():
    atexit.register(cleanup_procs)
    global gokvdir
    global goycsbdir
    goycsbdir = os.path.dirname(os.path.abspath(__file__))
    gokvdir = os.path.join(os.path.dirname(goycsbdir), "gokv")
    os.makedirs(global_args.outdir, exist_ok=True)
    resource.setrlimit(resource.RLIMIT_NOFILE, (100000, 100000))

    start_memkv_multiserver([[0], [10], [20], [30]])

    threading.Thread(target=add_servers).start()

    a = goycsb_bench(config['clntthreads'], 120, 128, 1.0, 0.0, config['clntcores'])
    with open(path.join(global_args.outdir, 'shard_migration.dat'), 'a+') as outfile:
        ops_so_far = 0
        for e in a:
            outfile.write('{0},{1}\n'.format(e[0], 2*(e[1] - ops_so_far)))
            ops_so_far = e[1]

if __name__=='__main__':
    main()
