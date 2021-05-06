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
    "font.size": 16
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
    fig = plt.figure()
    for data in datas:
        # rxs = []
        # rys = []

        # wxs = []
        # wys = []

        xys = []

        for d in data:
            # TODO: look for updates and reads; if any other operation is found, report an error
            x = 0.0
            y = 0.0
            for k, v in d['lts'].items():
                if k == 'READ':
                    throw("unimpl")
                if k == 'UPDATE':
                    x = x + v['thruput']
                    y = y + v['avg_latency'] / 1000
            xys.append((x,y))

        with open(data[0]['service'] + '.dat', 'w') as f:
            for xy in xys:
                print('{0}, {1}'.format(xy[0], xy[1]), file=f)

        # if wxs != []:
            # plt.plot(wxs, wys, marker = next(marker), label=data[0]['service'] + " puts")
        # if rxs != []:
            # plt.plot(rxs, rys, marker = next(marker), label=data[0]['service'] + " gets")
        # plt.xlabel('Throughput (ops/sec)')
        # plt.ylabel('Latency (ms)')
        # plt.title(data[0]['service'])
    # plt.legend()
    # plt.tight_layout()
    # fig.savefig('temp.pdf')

def main():
    datas = []
    redis_write_data = read_lt_data(path.join(global_args.outdir, 'rediskv_update_closed_lt.jsons'))
    memkv_write_data = read_lt_data(path.join(global_args.outdir, 'memkv_update_closed_lt.jsons'))
    datas += [redis_write_data, memkv_write_data]
    plot_lt(datas)

if __name__=='__main__':
    main()
