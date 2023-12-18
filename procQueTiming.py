#!/usr/bin/env python3
"""
Simple timings on the use of a Queue to send arrays between
processes in a ProcessPool. Does it really add much overhead? No
idea, so measuring it to find out.

Turns out it only takes about 0.25 second (on Sam's VM) to send enough
arrays for a whole Sentinel-2 band (10m), which suggests it is negligible.
"""
import argparse
from concurrent import futures
from multiprocessing import Manager

import numpy

import utils


def getCmdargs():
    p = argparse.ArgumentParser()
    p.add_argument("-b", "--blocksize", type=int, default=2048,
        help="Blocksize (default=%(default)s)")
    p.add_argument("-n", "--numblocks", type=int, default=30,
        help="Number of blocks (default=%(default)s)")
    cmdargs = p.parse_args()
    return cmdargs


def main():
    cmdargs = getCmdargs()

    # Make a suitable Queue
    manager = Manager()
    que = manager.Queue()

    timestamps = utils.TimeStampSet()

    timestamps.stamp("que", utils.TS_START)
    poolClass = futures.ProcessPoolExecutor
    with poolClass(max_workers=1) as procPool:
        senderProc = procPool.submit(senderFunc, que, cmdargs.blocksize,
            cmdargs.numblocks)

    receiverFunc(que)

    timestamps.stamp("que", utils.TS_END)

    print("tot", timestamps.timeSpentByPrefix("que"))


def senderFunc(que, blocksize, numblocks):
    for blocknum in range(numblocks):
        arr = numpy.zeros((blocksize, blocksize), dtype=numpy.uint8)
        que.put(arr)
    que.put(None)


def receiverFunc(que):
    arr = que.get()
    numblocks = 0
    while arr is not None:
        numblocks += 1
        arr = que.get()

    print("numblocks", numblocks)


if __name__ == "__main__":
    main()
