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

def start_command(args, gomaxprocs=0):
    if global_args.dry_run or global_args.verbose:
        print("[STARTING] " + " ".join(args))
    if not global_args.dry_run:
        e = os.environ.copy()
        e['MAXGOPROCS'] = str(gomaxprocs)
        return subprocess.Popen(args, text=True, stdout=subprocess.PIPE, env=e)

ycsb_dir = "."

# kvname = redis|gokv; workload file just has configuration info, not workload info.
def ycsb_one(kvname:str, runtime:int, target_rps:int, threads:int, valuesize:int, readprop:float, updateprop):
    # want it to take N seconds; want to give (target_time * target_rps) operations
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
                       ], gomaxprocs=25)

    if p is None:
        return ''

    ret = ''
    for stdout_line in iter(p.stdout.readline, ""):
        if stdout_line.find('Takes(s): {0}.'.format(runtime)) != -1:
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

def find_peak_thruput(kvname, valuesize, outfilename, readprop, updateprop):
    peak_thruput = 0
    low = 1
    high = -1

    while True:
        threads = 2*low
        if high > 0:
            if (high - low) < 10:
                return peak_thruput
            threads = int((low + high)/2)

        a = parse_ycsb_output(ycsb_one(kvname, 60, -1, threads, valuesize, readprop, updateprop))
        p = {'service': kvname, 'num_threads': threads, 'ratelimit': -1, 'lts': a}

        with open(outfilename, 'a+') as outfile:
            outfile.write(json.dumps(p) + '\n')

        thput = sum([ a[op]['thruput'] for op in a ])
        if thput > peak_thruput:
            low = threads
            peak_thruput = thput
        else:
            high = threads
    return -1

def generic_bench(s, readRatio, writeRatio, nshard):
    closed_lt(s, 128, path.join(global_args.outdir, s + '_update_closed_lt.jsons'), readRatio, writeRatio, num_threads(nshard))

def generic_peak(s, readRatio, writeRatio, nshard, outname):
    return find_peak_thruput(s, 128, path.join(global_args.outdir, outname), readRatio, writeRatio)

def get_memkv_peaks_all(outfilename):
    # This will manage the shard servers on its own;
    # It will start one shard server and one coordinator with 4 GOMAXPROCS each.
    # It runs benchmarks until it finds the peak.
    # Then, it adds a new shard server by via memkvctl, and continues.
    max_srvs = 5
    procs_per_shard = 8
    ps = []
    ps.append(start_command(['memkvshard', '-init', '-port', '12300'], gomaxprocs=procs_per_shard))
    ps.append(start_command(['memkvcoord', '-init', '127.0.0.1:12300', '-port', '12200'], gomaxprocs=procs_per_shard))
    for i in range(1, max_srvs):
        ps.append(start_command(['memkvshard', '-port', str(12300 + i)], gomaxprocs=procs_per_shard))

    for i in range(0, max_srvs): # max num of shard
        if i > 0:
            run_command(['memkvctl', '-coord', '127.0.0.1:12200', 'add', '127.0.0.1:' + str(12300 + i)])
        p = generic_peak('memkv', 0.0, 1.0, i+1, 'memkv_peak_raw.jsons')
        with open(outfilename, 'a+') as outfile:
            outfile.write(json.dumps({'srvs':i+1, 'peak': p}) + '\n')

def main():
    resource.setrlimit(resource.RLIMIT_NOFILE, (100000, 100000))
    os.makedirs(global_args.outdir, exist_ok=True)
    if global_args.workload == 'update':
        generic_bench(global_args.system, 0.0, 1.0, int(global_args.nshard))
    elif global_args.workload == 'peak':
        p = path.join(global_args.outdir, 'memkv_peaks.json')
        get_memkv_peaks_all(p)

if __name__=='__main__':
    main()
