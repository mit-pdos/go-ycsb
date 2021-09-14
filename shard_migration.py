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
description="Run benchmark with server being added in the middle"
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
