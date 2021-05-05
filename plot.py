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
description="Generate latency-throughput graphs"
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

global_args = parser.parse_args()

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
    datas = []
    redis_write_data = read_lt_data(path.join(global_args.outdir, 'rediskv_update_closed_lt.jsons'))
    memkv_write_data = read_lt_data(path.join(global_args.outdir, 'memkv_update_closed_lt.jsons'))
    datas += [redis_write_data, memkv_write_data]
    plot_lt(datas)

if __name__=='__main__':
    main()
