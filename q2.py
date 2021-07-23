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
        p = subprocess.Popen(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, preexec_fn=os.setsid)
        global procs
        procs.append(p)
        return p

def cleanup_procs():
    global procs
    for p in procs:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
    procs = []

def one_core(args, i):
    return ["numactl", "-C", str(i)] + args

def many_cores(args, c):
    return ["numactl", "-C", c] + args

# Starts server on port 12345
def start_memkv_multiserver(servers):
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
    print("[INFO] Started single core memkv servers with {0} servers".format(servers))
    return ps

def start_memkv_multicore(cores):
    ps = []
    c = "0"
    for i in range(1,cores):
        c += "," + str(i)

    coord = start_command(["go", "run",
                           "./cmd/memkvcoord", "-init",
                           "127.0.0.1:12300", "-port", "12200"], cwd=gokvdir)
    ps.append(coord)
    ps.append(start_command(many_cores(["go", "run", "./cmd/memkvshard", "-init", "-port", str(12300)], c), cwd=gokvdir))
    print("[INFO] Started a memkv server with {0} cores".format(cores))
    return ps


def goycsb_bench():
    p = start_command(["go", "run", "./cmd/go-ycsb",
                       "run", "memkv", "-P", "../gokv/bench/memkv_workload",
                       "--threads", "64", "--target", "-1",
                       "--interval","1", "-p", "operationcount=4294967295", "-p",
                       "fieldlength=128", "-p", "requestdistribution=uniform", "-p",
                       "readproportion=1.0", "-p", "updateproportion=0.0", "-p",
                       "memkv.coord=127.0.0.1:12200",], cwd=goycsbdir)

    print("Throughput of goycsb against memkv")
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

    # ps = start_memkv(1)
    # time.sleep(1)
    # goycsb_bench()
    # cleanup_procs()

    time.sleep(0.5)
    ps = start_memkv_multiserver(2)
    time.sleep(0.5)
    goycsb_bench()
    cleanup_procs()

    time.sleep(0.5)
    ps = start_memkv_multiserver(3)
    time.sleep(0.5)
    goycsb_bench()
    cleanup_procs()

    time.sleep(0.5)
    ps = start_memkv_multicore(2)
    time.sleep(0.5)
    goycsb_bench()

    time.sleep(0.5)
    ps = start_memkv_multicore(3)
    time.sleep(0.5)
    goycsb_bench()

if __name__=='__main__':
    main()
