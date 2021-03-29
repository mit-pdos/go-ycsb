#!/usr/bin/env python
from os import path
import argparse
import subprocess
import re
import matplotlib
import matplotlib.pyplot as plt

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
def ycsb_one(kvname:str, num_ops:int, target_rps:int, threads:int):
    # want it to take 10 seconds; want to give (target_time * target_rps) operations
    p = run_command(['go', 'run',
                     path.join(ycsb_dir, 'cmd/go-ycsb'),
                     'run', kvname,
                     '-P', path.join(gokv_dir, 'bench', kvname + '_workload'),
                     '--threads', str(threads),
                     '--target', str(target_rps),
                     '-p', 'operationcount=' + str(num_ops)
                     ])
    if p and p.returncode != 0: print(p.stderr)
    if p: return p.stdout
    return ""

def parse_ycsb_output(output):
    # look for 'Run finished, takes...', then parse the lines for each of the operations
    output = output[re.search("Run finished, takes .*\n", output).end():] # strip off beginning of output

    # NOTE: sample output from go-ycsb:
    # UPDATE - Takes(s): 12.6, Count: 999999, OPS: 79654.6, Avg(us): 12434, Min(us): 28, Max(us): 54145, 99th(us): 29000, 99.9th(us): 41000, 99.99th(us): 49000
    patrn = '(?P<opname>.*) - Takes\(s\): (?P<time>.*), Count: (?P<count>.*), OPS: (?P<ops>.*), Avg\(us\): (?P<avg_latency>.*), Min\(us\):.*\n' # Min(us): 28, Max(us): 54145, 99th(us): 29000, 99.9th(us): 41000, 99.99th(us): 49000'
    ms = re.finditer(patrn, output, flags=re.MULTILINE)
    a = dict()
    for m in ms:
        a[m.group('opname')] = (float(m.group('ops')), float(m.group('avg_latency')))
    return a

def main():
    start_redis()
    data = []
    for i in range(5):
        for j in range(5):
            target_rps = 100 * ((j + 1)**4)
            threads = 10*(i + 1)
            num_ops = min(5 * target_rps, 50000)
            a = parse_ycsb_output(ycsb_one('rediskv', num_ops, target_rps, threads))
            data = data + [ (target_rps, threads, a) ]
            print(data)

    print("FINAL")
    print(data)
    cleanup_background()


def test_plot():
    ds = [(100, 10, {'UPDATE': (1772.6, 5650.0)}), (1600, 10, {'UPDATE': (1225.9, 8148.0)}), (8100, 10, {'UPDATE': (875.7, 11401.0)}), (25600, 10, {'UPDATE': (1087.8, 9175.0)}), (62500, 10, {'UPDATE': (1011.8, 9856.0)}), (100, 20, {'UPDATE': (1471.8, 13724.0)}), (1600, 20, {'UPDATE': (2176.9, 9164.0)}), (8100, 20, {'UPDATE': (2024.7, 9862.0)}), (25600, 20, {'UPDATE': (2160.6, 9213.0)}), (62500, 20, {'UPDATE': (2029.8, 9813.0)}), (100, 30, {'UPDATE': (2496.2, 11542.0)}), (1600, 30, {'UPDATE': (2490.5, 11925.0)}), (8100, 30, {'UPDATE': (2800.5, 10679.0)}), (25600, 30, {'UPDATE': (2474.1, 12015.0)}), (62500, 30, {'UPDATE': (2606.0, 11409.0)}), (100, 40, {'UPDATE': (2491.6, 13687.0)}), (1600, 40, {'UPDATE': (6679.8, 5922.0)}), (8100, 40, {'UPDATE': (4265.9, 9300.0)}), (25600, 40, {'UPDATE': (4016.0, 9867.0)}), (62500, 40, {'UPDATE': (3770.5, 10505.0)}), (100, 50, {'UPDATE': (1010.4, 16382.0)}), (1600, 50, {'UPDATE': (6410.2, 7649.0)}), (8100, 50, {'UPDATE': (4573.4, 10769.0)}), (25600, 50, {'UPDATE': (4884.1, 10048.0)}), (62500, 50, {'UPDATE': (4364.2, 11391.0)})]
    xs = []
    ys = []
    for d in ds:
        if d[1] == 50:
            xs = xs + [d[2]['UPDATE'][0]]
            ys = ys + [d[2]['UPDATE'][1] / 1000]
    plt.scatter(xs, ys)
    plt.show()

if __name__=='__main__':
    # main()
    test_plot()
