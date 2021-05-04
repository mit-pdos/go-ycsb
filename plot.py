#!/usr/bin/env python3
from os import path
import argparse
import subprocess
import re
import matplotlib
import matplotlib.pyplot as plt
import json
import os
import resource
import itertools

plt.rcParams.update({
    "text.usetex": True,
    "font.family": "serif",
    "font.size": 22
})

parser = argparse.ArgumentParser(
description="Run benchmarks on kv services and generate latency-throughput graphs"
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

def read_lt_data(infilename):
    with open(infilename, 'r') as f:
        data = []
        for line in f:
            data.append(json.loads(line))
    return data

def plot_lt(datas):
    """
    Assumes data is in format
    [ (kvname, numthreads, { 'OPERATION_TYPE': (throughput in ops/sec, latency in us), ... } ),  ... ]
    """
    marker = itertools.cycle(('+', '.', 'o', '*'))
    for data in datas:
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
            plt.plot(wxs, wys, marker = next(marker), label=data[0]['service'] + " updates")
        if rxs != []:
            plt.plot(rxs, rys, marker = next(marker), label=data[0]['service'] + " reads")
        plt.xlabel('Throughput (ops/sec)')
        plt.ylabel('Latency (ms)')
        # plt.title(data[0]['service'])
    plt.legend()
    plt.show()

def main():
    resource.setrlimit(resource.RLIMIT_NOFILE, (100000, 100000))
    if global_args.command == 'run':
        os.makedirs(global_args.outdir, exist_ok=True)
        start_redis()
        if global_args.workload == 'update':
            gokv_update_bench()
        elif global_args.workload == 'read':
            gokv_read_bench()
    elif global_args.command == 'plot':
        datas = []
        if global_args.workload == 'update' or global_args.workload == 'both':
            redis_write_data = read_lt_data(path.join(global_args.outdir, 'redis_update_closed_lt.jsons'))
            gokv_write_data = read_lt_data(path.join(global_args.outdir, 'gokv_update_closed_lt.jsons'))
            datas += [redis_write_data, gokv_write_data]
        if global_args.workload == 'read' or global_args.workload == 'both':
            gokv_unsafe_read_data = read_lt_data(path.join(global_args.outdir, 'gokv_fast_unsafe_read_closed_lt.jsons'))
            redis_read_data = read_lt_data(path.join(global_args.outdir, 'redis_read_closed_lt.jsons'))
            datas += [redis_read_data, gokv_unsafe_read_data]
        plot_lt(datas)
    cleanup_background()


if __name__=='__main__':
    main()
