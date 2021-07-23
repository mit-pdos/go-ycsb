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
description="(Why) is go-ycsb faster than the custom client program for rpcscale?"
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

def run_command(args):
    if global_args.dry_run or global_args.verbose:
        print("[RUNNING] " + " ".join(args))
    if not global_args.dry_run:
        return subprocess.run(args, capture_output=True, text=True)

def start_command(args, cwd=None):
    if global_args.dry_run or global_args.verbose:
        print("[STARTING] " + " ".join(args))
    if not global_args.dry_run:
        p = subprocess.Popen(args, text=True, stdout=subprocess.PIPE, cwd=cwd)
        global procs
        procs.append(p)
        return p

def cleanup_procs():
    for p in procs:
        p.kill()

def start_memkv():
    coord = start_command(["go", "run",
                           "cmd/memkvcoord", "-init",
                           "127.0.0.1:12300", "-port", "12200"])
    shard = start_command(["go", "run",
                           "github.com/mit-pdos/gokv/cmd/memkvshard", "-init",
                           "-port", "12300"])

def one_core(args):
    return ["numactl", "-C", "0"] + args

# Starts server on port 12345
def start_rpcscale():
    coord = start_command(["go", "run",
                           "./cmd/memkvcoord", "-init",
                           "127.0.0.1:12345", "-port", "12200"], cwd=gokvdir)
    rpcscale = start_command(one_core(["go", "run", "./cmd/rpcscale", "-port", "12345"]), cwd=gokvdir)
    print("[INFO] Started rpcscale (and coord) server")

def custom_rpcscale():
    p = start_command(["go", "test", "-v", "./cmd/rpcscale"], cwd=gokvdir)
    print("Throughput of custom rpcscale benchmark")
    ops = 0
    seconds = 0
    for stdout_line in iter(p.stdout.readline, ""):
        patrn = "(?P<ops>.*) ops/sec"
        r = re.compile(patrn)
        m = r.match(stdout_line)
        if m:
            ops += int(m.group('ops').replace(',', ''))
            seconds += 1
            print('\r' + str(int(ops/seconds)), end='', flush=True)

def goycsb_rpcscale():
    p = start_command(["go", "run", "./cmd/go-ycsb",
                       "run", "memkv", "-P", "../gokv/bench/memkv_workload",
                       "--threads", "64", "--target", "-1",
                       "--interval","1", "-p", "operationcount=4294967295", "-p",
                       "fieldlength=128", "-p", "requestdistribution=uniform", "-p",
                       "readproportion=1.0", "-p", "updateproportion=0.0", "-p",
                       "memkv.coord=127.0.0.1:12200",], cwd=goycsbdir)

    print("Throughput of goycsb rpcscale benchmark")
    for stdout_line in iter(p.stdout.readline, ""):
        patrn = "OPS: (?P<ops>.*), Avg"
        r = re.compile(patrn)
        m = r.search(stdout_line)
        if m:
            print('\r' + m.group('ops'), end='', flush=True)

def main():
    atexit.register(cleanup_procs)
    global gokvdir
    global goycsbdir
    goycsbdir = os.path.dirname(os.path.abspath(__file__))
    gokvdir = os.path.join(os.path.dirname(goycsbdir), "gokv")

    start_rpcscale()
    time.sleep(0.5)
    goycsb_rpcscale()
    custom_rpcscale()

if __name__=='__main__':
    main()
