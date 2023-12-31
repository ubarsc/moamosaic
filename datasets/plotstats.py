#!/usr/bin/env python
"""
Plot moamosaic timing stats
"""
import argparse
import json

import numpy
from matplotlib import pyplot


def getCmdargs():
    """
    Get comand line arguments
    """
    p = argparse.ArgumentParser()
    p.add_argument("--moatiming", help="JSON file of moamosaic timings")
    p.add_argument("--gdaltiming", help="JSON file of gdal_merge timings")
    p.add_argument("--plotfile",
        help="Name of output plot file (default plots to screen)")
    cmdargs = p.parse_args()
    return cmdargs


def main():
    """
    Main routine
    """
    cmdargs = getCmdargs()

    plotMoaTimings(cmdargs)
    plotGdalmergeTimings(cmdargs)

    pyplot.xlabel('Number of extra read threads')
    pyplot.ylabel('Elapsed time (seconds)')
    pyplot.legend(loc=0)

    if cmdargs.plotfile is not None:
        pyplot.savefig(cmdargs.plotfile)
    else:
        pyplot.show()


def plotMoaTimings(cmdargs):
    """
    Plot the timing graph for moamosaic stats.

    X axis is number of read threads, Y is seconds elapsed time.
    """
    monitorlist = json.load(open(cmdargs.moatiming))
    timesByNumthreads = {}
    for monitor in monitorlist:
        timestamps = monitor['timestamps']
        elapsed = timestamps['domosaic:end'] - timestamps['domosaic:start']
        numthreads = monitor['params']['numthreads']
        if numthreads not in timesByNumthreads:
            timesByNumthreads[numthreads] = []
        timesByNumthreads[numthreads].append(elapsed)

    allNumthreads = numpy.array(list(timesByNumthreads.keys()))
    numThreadcounts = len(allNumthreads)
    numStats = 4
    elapsedStats = numpy.zeros((numThreadcounts, numStats),
            dtype=numpy.float32)
    for numthreads in allNumthreads:
        allElapsed = numpy.array(timesByNumthreads[numthreads])
        elapsedStats[numthreads - 1, 0] = numpy.percentile(allElapsed, 50)
        elapsedStats[numthreads - 1, 1] = numpy.percentile(allElapsed, 75)
        elapsedStats[numthreads - 1, 2] = numpy.percentile(allElapsed, 25)
        elapsedStats[numthreads - 1, 3] = len(allElapsed)

    pyplot.plot(allNumthreads, elapsedStats[:, 0], linestyle='-', c='k',
        label='Moamosaic (median)')
    pyplot.plot(allNumthreads, elapsedStats[:, 1], linestyle='--', c='k',
        label='Moamosaic (quartiles)')
    pyplot.plot(allNumthreads, elapsedStats[:, 2], linestyle='--', c='k')
    pyplot.xticks(range(numThreadcounts + 1))


def plotGdalmergeTimings(cmdargs):
    """
    Plot gdal_merge timings
    """
    timeList = json.load(open(cmdargs.gdaltiming))
    median = numpy.percentile(timeList, 50)
    quartiles = [numpy.percentile(timeList, 25),
        numpy.percentile(timeList, 75)]
    pyplot.plot([0], [median], linestyle=None, marker='X', c='k',
        label="gdal_merge (median")
    pyplot.plot([0, 0], quartiles, linestyle=None, marker='0', c='k',
        label="gdal_merge (quartiles)")


if __name__ == "__main__":
    main()
