#!/usr/bin/env python3
"""
Simple test of concurrent use of ImageInfo
"""
import argparse
from concurrent import futures

from osgeo import gdal

import utils

gdal.UseExceptions()


def getCmdargs():
    """
    Get command line arguments
    """
    p = argparse.ArgumentParser()
    p.add_argument("filelist", help="Text file list of files to info")
    p.add_argument("-n", "--numproc", type=int, default=5,
        help="Number of processes for reading (default=%(default)s)")
    p.add_argument("--usethreads", default=False, action="store_true",
        help="Use threads. Default will use separate subprocesses")
    cmdargs = p.parse_args()
    return cmdargs


def main():
    """
    Main routine
    """
    cmdargs = getCmdargs()

    filelist = [line.strip() for line in open(cmdargs.filelist)]
    timestamps = utils.TimeStampSet()

    if cmdargs.usethreads:
        poolClass = futures.ThreadPoolExecutor
    else:
        poolClass = futures.ProcessPoolExecutor

    timestamps.stamp("info", utils.TS_START)
    if cmdargs.numproc > 0:
        with poolClass(max_workers=cmdargs.numproc) as procPool:
            infolist = [info for info in
                    procPool.map(utils.ImageInfo, filelist)]
    else:
        infolist = [utils.ImageInfo(fn) for fn in filelist]
    timestamps.stamp("info", utils.TS_END)

    print("Read {} info objects".format(len(infolist)))
    print("read time", timestamps.timeElapsedByPrefix("info"))


if __name__ == "__main__":
    main()
