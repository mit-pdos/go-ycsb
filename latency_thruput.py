#!/usr/bin/env python
from os import path
import argparse
import subprocess
import re

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
    "--redis",
    help="location of redis root directory (must have built redis already by running `make`)",
    required=True,
    default=None,
)
parser.add_argument(
    "--gokv",
    help="location of gokv root directory",
    required=True,
    default=None,
)

global_args = parser.parse_args()

def run_command(args):
    if global_args.dry_run or global_args.verbose:
        print("[RUNNING] " + " ".join(args))
    if not global_args.dry_run:
        return subprocess.run(args, capture_output=True, text=True)

backgrounds = []
def background_run_command(args, cwd=None):
    global backgrounds
    if global_args.dry_run or global_args.verbose:
        print("[STARTING] " + " ".join(args))
    if not global_args.dry_run:
        backgrounds = backgrounds + [subprocess.Popen(args, cwd=cwd, stdout=subprocess.DEVNULL)]

def cleanup_background():
    for b in backgrounds:
        b.kill()

gokv_dir = global_args.gokv # root gokv directory
redis_dir = global_args.redis # root redis directory
ycsb_dir = '.' # root go-ycsb directory

# if redis is already running, this will fail, but that's ok with us.
def start_redis():
    background_run_command([path.join(redis_dir, 'src/redis-server'),
                            path.join(gokv_dir, 'bench/redis-pers.conf'),
                            ], cwd=gokv_dir)

# kvname = redis|gokv; looks for
def ycsb_one(kvname:str, target_time:float, target_rps:int, threads:int):
    # want it to take 10 seconds; want to give (target_time * target_rps) operations
    opcount = int(target_time * target_rps)
    p = run_command(['go', 'run',
                     path.join(ycsb_dir, 'cmd/go-ycsb'),
                     'run', kvname,
                     '-P', path.join(gokv_dir, 'bench', kvname + '_workload'),
                     '--threads', str(threads),
                     '--target', str(target_rps),
                     '-p', 'operationcount=' + str(opcount)
                     ])
    if p and p.returncode != 0: print(p.stderr)
    if p: return p.stdout
    return ""

def parse_ycsb_output(output):
    # look for 'Run finished', then parse the lines for each of the operations

    output = output[re.search("Run finished, takes .*\n", output).end():] # strip off beginning of output
    # NOTE: sample output from go-ycsb
    # UPDATE - Takes(s): 12.6, Count: 999999, OPS: 79654.6, Avg(us): 12434, Min(us): 28, Max(us): 54145, 99th(us): 29000, 99.9th(us): 41000, 99.99th(us): 49000
    patrn = '(?P<opname>.*) - Takes\(s\): (?P<time>.*), Count: (?P<count>.*), OPS: (?P<ops>.*), Avg\(us\): (?P<avg_latency>.*), Min\(us\):.*\n' # Min(us): 28, Max(us): 54145, 99th(us): 29000, 99.9th(us): 41000, 99.99th(us): 49000'
    ms = re.finditer(patrn, output, flags=re.MULTILINE)
    a = dict()
    for m in ms:
        a[m.group('opname')] = (float(m.group('ops')), float(m.group('avg_latency')))
    return a

def main():
    start_redis()
    print(parse_ycsb_output(ycsb_one('rediskv', 10.0, -1, 50)))

    for i in range(10):
        a = parse_ycsb_output(ycsb_one('rediskv', 10.0, 100 * 2**i, 50))
        print(a)

    cleanup_background()

if __name__=='__main__':
    main()
