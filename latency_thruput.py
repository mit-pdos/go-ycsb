#!/usr/bin/env python
from os import path
import argparse
import subprocess
import re
import matplotlib
import matplotlib.pyplot as plt
import json
import os
import resource

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
parser.add_argument(
    "--outdir",
    help="output directory for benchmark results",
    required=True,
    default=None,
)
subparsers = parser.add_subparsers(dest="command")
run_parser = subparsers.add_parser('run')
run_parser.add_argument(
    "workload",
    help="update|read",
)
plot_parser = subparsers.add_parser('plot')
plot_parser.add_argument(
    "workload",
    help="update|read",
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

gokv_dir = global_args.gokv # root gokv directory
redis_dir = global_args.redis # root redis directory
ycsb_dir = '.' # root go-ycsb directory

# if redis is already running, this will fail, but that's ok with us.
def start_redis():
    background_run_command([path.join(redis_dir, 'src/redis-server'),
                            path.join(gokv_dir, 'bench/redis-pers.conf'),
                            ], cwd=gokv_dir)

# if redis is already running, this will fail, but that's ok with us.
def start_gokv():
    background_run_command(["go", "run",
                            path.join(gokv_dir, "cmd/srv"),
                            ], cwd=gokv_dir)

# kvname = redis|gokv; workload file just has configuration info, not workload info.
def ycsb_one(kvname:str, runtime:int, target_rps:int, threads:int, valuesize:int, readprop:float, updateprop):
    # want it to take 10 seconds; want to give (target_time * target_rps) operations
    p = start_command(['go', 'run',
                       path.join(ycsb_dir, 'cmd/go-ycsb'),
                       'run', kvname,
                       '-P', path.join(gokv_dir, 'bench', kvname + '_workload'),
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
        a[m.group('opname').strip()] = {'thruput': float(m.group('ops')), 'avg_latency': float(m.group('avg_latency'))}
    return a

def find_peak_throughput(kvname, valuesize):
    # keep doubling number of threads until throughput gets saturated.
    pass

# [5, 10, 20, 40, 80, 160, 320, 640, 1280, ...]?
def num_update_threads(i):
    if i < 5:
        return (i + 1)
    elif i < 10:
        return (i - 4) * 10
    elif i < 15:
        return (i - 9) * 100
    else:
        return 1000 * (i - 14) + 500

def num_read_threads(i):
    if i < 5:
        return i + 1
    else:
        return (i - 4) * 10

# TODO: add a ycsb_many to run a small benchmark many times
# TODO: ycsb_one should take a real-time parameter, and just kill the benchmark after that much time (post-warmup) has elapsed.
def closed_lt(kvname, valuesize, outfilename, readprop, updateprop, thread_fn):
    data = []
    i = 0
    last_good_index = 0
    peak_thruput = 0
    last_thruput = 10000
    last_threads = 10

    while True:
        if i > last_good_index + 5:
            break
        threads = thread_fn(i)

        # make a guess about the thruput this round;
        # another (probably better) option is to have no bound on the number of ops, and just kill the benchmark early after enough ops/time
        pred_thruput = (last_thruput/last_threads) * threads
        num_ops = int(pred_thruput * 5) # estimate enough operations for 10 seconds
        a = parse_ycsb_output(ycsb_one(kvname, num_ops, -1, threads, valuesize, readprop, updateprop))
        p = {'service': kvname, 'num_threads': threads, 'ratelimit': -1, 'lts': a}

        data = data + [ p ]
        with open(outfilename, 'a+') as outfile:
            outfile.write(json.dumps(p) + '\n')

        thput = sum([ a[op]['thruput'] for op in a ])

        if thput > peak_thruput:
            last_good_index = i
        if thput > peak_thruput:
            peak_thruput = thput

        last_thruput = int(thput + 1)
        last_threads = threads

        i = i + 1

    return data

def redis_update_bench():
    closed_lt('rediskv', 128, path.join(global_args.outdir, 'redis_update_closed_lt.jsons'), 0.0, 1.0, num_update_threads)
    cleanup_background()

def redis_read_bench():
    closed_lt('rediskv', 128, path.join(global_args.outdir, 'redis_read_closed_lt.jsons'), 1.0, 0.0, num_read_threads)
    cleanup_background()

def main():
    if global_args.command == 'run':
        os.makedirs(global_args.outdir, exist_ok=True)
        start_redis()
        resource.setrlimit(resource.RLIMIT_NOFILE, (100000, 100000))
        if global_args.workload == 'update':
            redis_update_bench()
        elif global_args.workload == 'read':
            redis_read_bench()
    elif global_args.command == 'plot':
        data = []
        if global_args.workload == 'update':
            data = read_lt_data(path.join(global_args.outdir, 'redis_update_closed_lt.jsons'))
        elif global_args.workload == 'read':
            data = read_lt_data(path.join(global_args.outdir, 'redis_read_closed_lt.jsons'))
        plot_lt(data)
    cleanup_background()

def read_lt_data(infilename):
    with open(infilename, 'r') as f:
        data = []
        for line in f:
            data.append(json.loads(line))
    return data

def plot_lt(data):
    """
    Assumes data is in format
    [ (kvname, numthreads, { 'OPERATION_TYPE': (throughput in ops/sec, latency in us), ... } ),  ... ]
    """
    #ds = [(10, {'UPDATE': (866.5, 11510.0)}), (20, {'UPDATE': (1607.5, 12412.0)}), (30, {'UPDATE': (2378.7, 12572.0)}), (40, {'UPDATE': (3449.7, 11441.0)}), (50, {'UPDATE': (4683.3, 10559.0)}), (60, {'UPDATE': (6024.8, 9888.0)}), (70, {'UPDATE': (6928.2, 9965.0)}), (80, {'UPDATE': (7745.6, 10164.0)}), (90, {'UPDATE': (9578.5, 9272.0)}), (100, {'UPDATE': (9475.2, 10494.0)}), (110, {'UPDATE': (9827.7, 10931.0)}), (120, {'UPDATE': (10165.9, 11663.0)}), (130, {'UPDATE': (13514.3, 9463.0)}), (140, {'UPDATE': (11959.8, 11639.0)}), (150, {'UPDATE': (13112.1, 11241.0)}), (160, {'UPDATE': (14825.6, 10726.0)}), (170, {'UPDATE': (17538.3, 9631.0)}), (180, {'UPDATE': (16067.8, 11065.0)}), (190, {'UPDATE': (16734.9, 11198.0)}), (200, {'UPDATE': (18395.5, 10559.0)}), (210, {'UPDATE': (18649.6, 11135.0)}), (220, {'UPDATE': (16995.9, 12826.0)}), (230, {'UPDATE': (20062.0, 11238.0)}), (240, {'UPDATE': (21908.3, 10782.0)}), (250, {'UPDATE': (19225.3, 12852.0)}), (260, {'UPDATE': (22559.7, 11328.0)}), (270, {'UPDATE': (21599.6, 12237.0)}), (280, {'UPDATE': (23328.6, 11798.0)}), (290, {'UPDATE': (22441.3, 12815.0)}), (300, {'UPDATE': (21585.1, 13698.0)}), (310, {'UPDATE': (18597.7, 16485.0)})]
    # ds =  [(40, {'UPDATE': (3671.7, 10773.0)}), (80, {'UPDATE': (6960.9, 11366.0)}), (120, {'UPDATE': (10251.0, 11642.0)}), (160, {'UPDATE': (13054.8, 12160.0)}), (200, {'UPDATE': (14917.7, 13270.0)}), (240, {'UPDATE': (17166.4, 13856.0)}), (280, {'UPDATE': (18630.4, 14889.0)}), (320, {'UPDATE': (21374.8, 14781.0)}), (360, {'UPDATE': (24216.1, 14710.0)}), (400, {'UPDATE': (24968.4, 15863.0)}), (440, {'UPDATE': (24225.0, 17944.0)}), (480, {'UPDATE': (24634.1, 19279.0)}), (520, {'UPDATE': (24461.3, 21066.0)}), (560, {'UPDATE': (30088.9, 18431.0)}), (600, {'UPDATE': (30493.1, 19568.0)}), (640, {'UPDATE': (32317.5, 19685.0)}), (680, {'UPDATE': (29337.9, 23087.0)}), (720, {'UPDATE': (33602.3, 21183.0)}), (760, {'UPDATE': (34762.7, 21750.0)}), (800, {'UPDATE': (33995.9, 23443.0)}), (840, {'UPDATE': (34785.0, 24043.0)}), (880, {'UPDATE': (35929.9, 24272.0)}), (920, {'UPDATE': (38012.5, 23960.0)}), (960, {'UPDATE': (38445.0, 24746.0)}), (1000, {'UPDATE': (39164.4, 25287.0)})]
    # ds = ds + [(1040, {'UPDATE': (39389.0, 26093.0)}), (1080, {'UPDATE': (39032.3, 27535.0)}), (1120, {'UPDATE': (37647.2, 29619.0)}), (1160, {'UPDATE': (38163.7, 30274.0)}), (1200, {'UPDATE': (39366.4, 30257.0)})]
    # ds = ds + [(1240, {'UPDATE': (39874.2, 30831.0)}), (1280, {'UPDATE': (40491.1, 31423.0)}), (1320, {'UPDATE': (41281.4, 31759.0)}), (1360, {'UPDATE': (41302.3, 32527.0)}), (1400, {'UPDATE': (42146.6, 32796.0)}), (1440, {'UPDATE': (42023.9, 33922.0)}), (1480, {'UPDATE': (41291.9, 35442.0)}), (1520, {'UPDATE': (41327.2, 36216.0)}), (1560, {'UPDATE': (43586.4, 35532.0)}), (1600, {'UPDATE': (39293.9, 40125.0)}), (1640, {'UPDATE': (43613.3, 37284.0)}), (1680, {'UPDATE': (42707.9, 39017.0)}), (1720, {'UPDATE': (43129.2, 39394.0)}), (1760, {'UPDATE': (43257.0, 40141.0)}), (1800, {'UPDATE': (43442.8, 40821.0)}), (1840, {'UPDATE': (43511.7, 41307.0)}), (1880, {'UPDATE': (43787.6, 42220.0)}), (1920, {'UPDATE': (43748.7, 43229.0)}), (1960, {'UPDATE': (44689.9, 43374.0)}), (2000, {'UPDATE': (44075.1, 44837.0)}), (2040, {'UPDATE': (44375.4, 45587.0)}), (2080, {'UPDATE': (41538.2, 49266.0)}), (2120, {'UPDATE': (44662.0, 46540.0)}), (2160, {'UPDATE': (44139.7, 48079.0)}), (2200, {'UPDATE': (45193.7, 47936.0)}), (2240, {'UPDATE': (44946.9, 48959.0)}), (2280, {'UPDATE': (45037.8, 49611.0)}), (2320, {'UPDATE': (45405.3, 50278.0)}), (2360, {'UPDATE': (44965.6, 51371.0)}), (2400, {'UPDATE': (44917.4, 52481.0)}), (2440, {'UPDATE': (44852.3, 53438.0)}), (2480, {'UPDATE': (46005.5, 53155.0)}), (2520, {'UPDATE': (45755.8, 54009.0)}), (2560, {'UPDATE': (45651.2, 55191.0)}), (2600, {'UPDATE': (44784.6, 56615.0)}), (2640, {'UPDATE': (44907.2, 57796.0)}), (2680, {'UPDATE': (46418.2, 56733.0)}), (2720, {'UPDATE': (45533.8, 57947.0)}), (2760, {'UPDATE': (47084.9, 57824.0)}), (2800, {'UPDATE': (46285.1, 59098.0)}), (2840, {'UPDATE': (46183.0, 59933.0)}), (2880, {'UPDATE': (46702.9, 60346.0)}), (2920, {'UPDATE': (47216.9, 60527.0)}), (2960, {'UPDATE': (46444.1, 62533.0)}), (3000, {'UPDATE': (46675.5, 62967.0)}), (3040, {'UPDATE': (46497.0, 63902.0)}), (3080, {'UPDATE': (47332.3, 64183.0)}), (3120, {'UPDATE': (47319.1, 64956.0)}), (3160, {'UPDATE': (46827.5, 65853.0)}), (3200, {'UPDATE': (47504.6, 66169.0)}), (3240, {'UPDATE': (46229.8, 67871.0)}), (3280, {'UPDATE': (46922.9, 68378.0)}), (3320, {'UPDATE': (47210.8, 68841.0)}), (3360, {'UPDATE': (48003.8, 68950.0)}), (3400, {'UPDATE': (47246.6, 70304.0)}), (3440, {'UPDATE': (47990.9, 70562.0)}), (3480, {'UPDATE': (47462.5, 71606.0)}), (3520, {'UPDATE': (47468.3, 72304.0)}), (3560, {'UPDATE': (47564.4, 73525.0)}), (3600, {'UPDATE': (47766.2, 74231.0)})]
    rxs = []
    rys = []

    wxs = []
    wys = []

    for d in data:
        # TODO: look for updates and reads; if any other operation is found, report an error
        for k, v in d['lts'].items():
            if k == 'READ':
                rxs = rxs + [v['thruput']]
                rys = rys + [v['avg_latency'] / 1000]
            if k == 'UPDATE':
                wxs = wxs + [v['thruput']]
                wys = wys + [v['avg_latency'] / 1000]

    if wxs != []:
        plt.plot(wxs, wys, '-o')
    if rxs != []:
        plt.plot(rxs, rys, '-o')
    plt.xlabel('Throughput (ops/sec)')
    plt.ylabel('Latency (ms)')
    plt.title(data[0]['service'])
    plt.show()

if __name__=='__main__':
    main()
