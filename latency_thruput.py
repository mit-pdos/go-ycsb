#!/usr/bin/env python3
from os import path
import argparse
import subprocess
import re
import json
import os
import resource
import itertools

parser = argparse.ArgumentParser(
description="Run benchmarks on kv services and generate latency-throughput graphs"
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
# subparsers = parser.add_subparsers(dest="command")
# run_parser = subparsers.add_parser('run')
parser.add_argument(
    "system",
    help="memkv|redis",
)

parser.add_argument(
    "nshard",
    help="Number of shards in system",
)

parser.add_argument(
    "workload",
    help="update|read|both",
)

global_args = parser.parse_args()

def run_command(args):
    if global_args.dry_run or global_args.verbose:
        print("[RUNNING] " + " ".join(args))
    if not global_args.dry_run:
        return subprocess.run(args, capture_output=True, text=True)

def start_command(args):
    if global_args.dry_run or global_args.verbose:
        print("[STARTING] " + " ".join(args))
    if not global_args.dry_run:
        return subprocess.Popen(args, text=True, stdout=subprocess.PIPE)

backgrounds = []
def background_run_command(args, cwd=None):
    global backgrounds
    if global_args.dry_run or global_args.verbose:
        print("[BACKGROUND] " + " ".join(args))
    if not global_args.dry_run:
        backgrounds = backgrounds + [subprocess.Popen(args, cwd=cwd, stdout=subprocess.DEVNULL)]

def cleanup_background():
    for b in backgrounds:
        b.kill()

ycsb_dir = "."

# kvname = redis|gokv; workload file just has configuration info, not workload info.
def ycsb_one(kvname:str, runtime:int, target_rps:int, threads:int, valuesize:int, readprop:float, updateprop):
    # want it to take 10 seconds; want to give (target_time * target_rps) operations
    p = start_command(['go', 'run',
                       path.join(ycsb_dir, 'cmd/go-ycsb'),
                       'run', kvname,
                       '-P', path.join('bench', kvname + '_workload'),
                       '--threads', str(threads),
                       '--target', str(target_rps),
                       '--interval', '1',
                       '-p', 'operationcount=' + str(2**32 - 1),
                       '-p', 'fieldlength=' + str(valuesize),
                       '-p', 'requestdistribution=uniform',
                       '-p', 'readproportion=' + str(readprop),
                       '-p', 'updateproportion=' + str(updateprop),
                       ])

    if p is None:
        return ''

    ret = ''
    for stdout_line in iter(p.stdout.readline, ""):
        if stdout_line.find('Takes(s): 10.') != -1:
            ret = stdout_line
            break
    p.stdout.close()
    p.terminate()

    # if p and p.returncode != 0: print(p.stderr)
    return ret

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

def find_peak_throughput(kvname, valuesize):
    # keep doubling number of threads until throughput gets saturated.
    pass

def num_threads(nshards):
    def temp(i):
        if i < 5:
            return i + 1
        else:
            return nshards * (i - 4) * 10
    return temp

# TODO: add a ycsb_many to run a small benchmark many times
# TODO: ycsb_one should take a real-time parameter, and just kill the benchmark after that much time (post-warmup) has elapsed.
def closed_lt(kvname, valuesize, outfilename, readprop, updateprop, thread_fn):
    data = []
    i = 0
    last_good_index = 0
    peak_thruput = 0
    # last_thruput = 10000
    # last_threads = 10

    while True:
        if i > last_good_index + 5:
            break
        threads = thread_fn(i)

        # make a guess about the thruput this round;
        # another (probably better) option is to have no bound on the number of ops, and just kill the benchmark early after enough ops/time
        # pred_thruput = (last_thruput/last_threads) * threads
        # num_ops = int(pred_thruput * 5) # estimate enough operations for 10 seconds
        a = parse_ycsb_output(ycsb_one(kvname, 10, -1, threads, valuesize, readprop, updateprop))
        p = {'service': kvname, 'num_threads': threads, 'ratelimit': -1, 'lts': a}

        data = data + [ p ]
        with open(outfilename, 'a+') as outfile:
            outfile.write(json.dumps(p) + '\n')

        thput = sum([ a[op]['thruput'] for op in a ])

        if thput > peak_thruput:
            last_good_index = i
        if thput > peak_thruput:
            peak_thruput = thput

        # last_thruput = int(thput + 1)
        last_threads = threads

        i = i + 1

    return data

def generic_bench(s, readRatio, writeRatio, nshard):
    closed_lt(s, 128, path.join(global_args.outdir, s + '_update_closed_lt.jsons'), readRatio, writeRatio, num_threads(nshard))
    cleanup_background()

def read_lt_data(infilename):
    with open(infilename, 'r') as f:
        data = []
        for line in f:
            data.append(json.loads(line))
    return data

def main():
    resource.setrlimit(resource.RLIMIT_NOFILE, (100000, 100000))
    os.makedirs(global_args.outdir, exist_ok=True)
    if global_args.workload == 'update':
        generic_bench(global_args.system, 0.0, 1.0, int(global_args.nshard))
    cleanup_background()


if __name__=='__main__':
    main()
