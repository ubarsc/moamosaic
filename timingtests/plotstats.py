#!/usr/bin/env python
"""
Plot moamosaic timing stats
"""
import argparse
import json

import numpy
from matplotlib import pyplot, markers


def getCmdargs():
    """
    Get comand line arguments
    """
    p = argparse.ArgumentParser()
    p.add_argument("--moatiming", action="append",
        help="JSON file of moamosaic timings")
    p.add_argument("--gdaltiming", help="JSON file of gdal_merge timings")
    p.add_argument("--plotfile",
        help="Name of output plot file (default plots to screen)")
    p.add_argument("--confidence", default=90, type=int,
        help="Confidence interval to show (in %%) (default=%(default)s)")
    cmdargs = p.parse_args()
    return cmdargs


def main():
    """
    Main routine
    """
    cmdargs = getCmdargs()

    pyplot.figure(figsize=(9, 5))
    pyplot.subplots_adjust(right=0.98, top=0.98, left=0.08, bottom=0.12)

    confidence = cmdargs.confidence
    confTail = (100 - confidence) / 2
    upper = 100 - confTail
    lower = confTail

    plotGdalmergeTimings(cmdargs, lower, upper)
    plotMoaTimings(cmdargs, lower, upper)

    pyplot.xlabel('Number of read worker threads')
    pyplot.ylabel('Elapsed time (seconds)')
    pyplot.legend(loc=0)

    if cmdargs.plotfile is not None:
        pyplot.savefig(cmdargs.plotfile)
    else:
        pyplot.show()


def plotMoaTimings(cmdargs, lower, upper):
    """
    Plot the timing graph for moamosaic stats.

    X axis is number of read threads, Y is seconds elapsed time.
    """
    maxThreadcount = 0
    colourList = ['b', 'r', 'g']
    j = 0
    for moatimingfile in cmdargs.moatiming:
        monitorlist = json.load(open(moatimingfile))
        timesByNumthreads = {}
        for monitor in monitorlist:
            timestamps = monitor['timestamps']
            elapsed = timestamps['domosaic:end'] - timestamps['domosaic:start']
            numthreads = monitor['params']['numthreads']
            if numthreads not in timesByNumthreads:
                timesByNumthreads[numthreads] = []
            timesByNumthreads[numthreads].append(elapsed)

        nCpus = monitorlist[0]['params']['cpucount']

        allNumthreads = numpy.array(list(timesByNumthreads.keys()))
        numThreadcounts = len(allNumthreads)
        numStats = 4
        elapsedStats = numpy.zeros((numThreadcounts, numStats),
                dtype=numpy.float32)
        for numthreads in allNumthreads:
            allElapsed = numpy.array(timesByNumthreads[numthreads])
            elapsedStats[numthreads - 1, 0] = numpy.percentile(allElapsed, 50)
            elapsedStats[numthreads - 1, 1] = numpy.percentile(allElapsed, upper)
            elapsedStats[numthreads - 1, 2] = numpy.percentile(allElapsed, lower)
            elapsedStats[numthreads - 1, 3] = len(allElapsed)
            maxThreadcount = max(maxThreadcount, numthreads)

        pyplot.plot(allNumthreads, elapsedStats[:, 0], linestyle='-',
            label='Moamosaic ({} CPU) (median)'.format(nCpus), linewidth=1.0,
            c=colourList[j])
        pyplot.plot(allNumthreads, elapsedStats[:, 1], linestyle='--',
            label='                                ({}% confidence)'.format(
                cmdargs.confidence),
            linewidth=0.5, c=colourList[j])
        pyplot.plot(allNumthreads, elapsedStats[:, 2], linestyle='--',
            linewidth=0.5, c=colourList[j])

        j += 1
    pyplot.xticks(range(maxThreadcount + 1))


def plotGdalmergeTimings(cmdargs, lower, upper):
    """
    Plot gdal_merge timings
    """
    timeList = json.load(open(cmdargs.gdaltiming))
    median = numpy.percentile(timeList, 50)
    quartiles = [numpy.percentile(timeList, lower),
        numpy.percentile(timeList, upper)]
    pyplot.scatter([0], [median], c='k', label="gdal_merge (median)",
        marker=markers.MarkerStyle('x', fillstyle='none'),
        linewidth=0.8)
    pyplot.scatter([0, 0], quartiles, c='k',
        label="                    ({}% confidence)".format(cmdargs.confidence),
        marker=markers.MarkerStyle('o', fillstyle='none'),
        linewidth=0.8)


if __name__ == "__main__":
    main()
