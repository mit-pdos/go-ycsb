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

parser = argparse.ArgumentParser(
description="Do two single-core rpc servers give better perf than a single two-core rpc server?"
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
        p = subprocess.Popen(args, text=True, stdout=subprocess.PIPE, cwd=cwd)
        global procs
        procs.append(p)
        return p

def cleanup_procs():
    global procs
    for p in procs:
        p.kill()
    procs = []

def start_memkv():
    coord = start_command(["go", "run",
                           "cmd/memkvcoord", "-init",
                           "127.0.0.1:12300", "-port", "12200"])
    shard = start_command(["go", "run",
                           "github.com/mit-pdos/gokv/cmd/memkvshard", "-init",
                           "-port", "12300"])

def one_core(args, i):
    return ["numactl", "-C", str(i)] + args

def two_cores(args, i, j):
    return ["numactl", "-C", str(i) + "," + str(j)] + args

# Starts server on port 12345
def start_rpcscale(servers):
    ps = []
    coord = start_command(["go", "run",
                           "./cmd/memkvcoord", "-init",
                           "127.0.0.1:12300", "-port", "12200"], cwd=gokvdir)
    ps.append(coord)
    ps.append(start_command(one_core(["go", "run", "./cmd/memkvshard", "-init", "-port", str(12300)], 0), cwd=gokvdir))
    for i in range(1, servers):
        rpcscale = start_command(one_core(["go", "run", "./cmd/memkvshard", "-port", str(12300 + i)], i), cwd=gokvdir)
        ps.append(rpcscale)
        time.sleep(1)
        run_command(["go", "run", "./cmd/memkvctl", "-coord", "127.0.0.1:12200", "add", "127.0.0.1:" + str(12300 + i)], cwd=gokvdir)
    print("[INFO] Started rpcscale (and coord) server")
    return ps

def goycsb_bench():
    p = start_command(["go", "run", "./cmd/go-ycsb",
                       "run", "memkv", "-P", "../gokv/bench/memkv_workload",
                       "--threads", "64", "--target", "-1",
                       "--interval","1", "-p", "operationcount=4294967295", "-p",
                       "fieldlength=128", "-p", "requestdistribution=uniform", "-p",
                       "readproportion=1.0", "-p", "updateproportion=0.0", "-p",
                       "memkv.coord=127.0.0.1:12200",], cwd=goycsbdir)

    print("Throughput of goycsb rpcscale benchmark")
    seconds = 0
    for stdout_line in iter(p.stdout.readline, ""):
        patrn = "OPS: (?P<ops>.*), Avg"
        r = re.compile(patrn)
        m = r.search(stdout_line)
        if m:
            print('\r' + m.group('ops'), end='', flush=True)
            seconds += 1
        if seconds > 5:
            print()
            return


def main():
    atexit.register(cleanup_procs)
    global gokvdir
    global goycsbdir
    goycsbdir = os.path.dirname(os.path.abspath(__file__))
    gokvdir = os.path.join(os.path.dirname(goycsbdir), "gokv")

    ps = start_rpcscale(1)
    time.sleep(1)
    goycsb_bench()
    cleanup_procs()

    time.sleep(3)
    ps = start_rpcscale(2)
    time.sleep(0.5)
    goycsb_bench()
    for p in ps:
        p.kill()

if __name__=='__main__':
    main()
