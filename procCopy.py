#!/usr/bin/env python3
"""
Copy a single input image, using ProcessPool to read blocks
in parallel, and the main process doing the writing.
"""
import argparse
from concurrent import futures
from multiprocessing import Manager
import queue

from osgeo import gdal

import utils

gdal.UseExceptions()


def getCmdargs():
    """
    Get command line arguments
    """
    p = argparse.ArgumentParser()
    p.add_argument("infile")
    p.add_argument("outfile")
    p.add_argument("-b", "--blocksize", type=int, default=2048,
        help="Blocksize (default=%(default)s)")
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

    # Make a suitable Queue and poolClass
    if cmdargs.usethreads:
        que = queue.Queue()
        poolClass = futures.ThreadPoolExecutor
    else:
        manager = Manager()
        que = manager.Queue()
        poolClass = futures.ProcessPoolExecutor

    timestamps = utils.TimeStampSet()

    imginfo = utils.ImageInfo(cmdargs.infile)
    blockList = utils.makeBlockList(imginfo.nrows, imginfo.ncols,
        cmdargs.blocksize)

    timestamps.stamp("whole", utils.TS_START)
    numproc = cmdargs.numproc
    with poolClass(max_workers=numproc) as procPool:
        procList = []
        for i in range(numproc):
            blockSubset = blockList[i::numproc]
            proc = procPool.submit(readFunc, cmdargs.infile, que, blockSubset)
            procList.append(proc)

        writeBlocks(imginfo, cmdargs.outfile, que, blockList)
    timestamps.stamp("whole", utils.TS_END)

    print("whole", timestamps.timeSpentByPrefix("whole"))

    # Merge all the individual read timestamps objects, so we can look
    # at overlap timings
    readStampsList = [proc.result() for proc in procList]
    readStamps = readStampsList[0]
    for rds in readStampsList[1:]:
        readStamps.merge(rds)
    # Just use one big group of all readblock entries
    readGroupNames = [set([name for (name, startEnd) in readStamps.stamps
            if name.startswith("readblock")])]
    pcntOverlap = readStamps.pcntOverlapByGroup(readGroupNames)[0]
    print("pcnt overlap", round(pcntOverlap, 2))

    utils.checkOutput(cmdargs.infile, cmdargs.outfile)


def readFunc(infile, que, blockList):
    """
    Each read worker is running this function. For each block specification
    in the given list, read that block from the file, and put it into the
    queue, along with its block specification.
    """
    ds = gdal.Open(infile)
    band = ds.GetRasterBand(1)
    timestamps = utils.TimeStampSet()

    for block in blockList:
        tsName = utils.TS_READBLOCK.format("{}_{}".format(block.left, block.top))
        timestamps.stamp(tsName, utils.TS_START)
        arr = band.ReadAsArray(block.left, block.top, block.xsize, block.ysize)
        timestamps.stamp(tsName, utils.TS_END)
        que.put((block, arr))

    return timestamps


def writeBlocks(imginfo, outfile, que, blockList):
    """
    Write all the blocks. Read each block (and its block specification) from
    the queue (as put there by the read workers). Because there are multiple
    read workers, the blocks could arrive in the queue in any order, so
    each block is read, place it in the local block cache, keyed by its
    specification.

    The blocks in the blockList are to be written, in that order. For each
    block in the list, when it becomes available in the blockCache, write
    it to the outfile, remove it from the cache.
    """
    drvr = gdal.GetDriverByName("GTiff")
    options = ["COMPRESS=DEFLATE", "TILED=YES"]
    ds = drvr.Create(outfile, imginfo.ncols, imginfo.nrows, 1,
        imginfo.dataType, options=options)
    band = ds.GetRasterBand(1)

    # Cache of blocks available to write. Keyed by BlockSpec object.
    blockCache = {}

    numBlocks = len(blockList)
    i = 0
    maxCacheSize = 0
    maxQueueSize = 0
    while i < numBlocks:
        # Get another block from the que (if available), and cache it
        if not que.empty():
            (blockSpec, arr) = que.get_nowait()
            key = makeBlockKey(blockSpec)
            blockCache[key] = arr
        else:
            blockSpec = None
            arr = None

        maxCacheSize = max(maxCacheSize, len(blockCache))
        maxQueueSize = max(maxQueueSize, que.qsize())

        # If the i-th block is ready in the cache, write it out,
        # remove it from cache, and increment i
        block = blockList[i]
        key = makeBlockKey(block)
        if key in blockCache:
            arr = blockCache[key]
            band.WriteArray(arr, block.left, block.top)
            blockCache.pop(key)
            i += 1

    print("Max que size", maxQueueSize)
    print("Max cache size", maxCacheSize)


def makeBlockKey(block):
    """
    Make a key value for the given BlockSpec object.

    I should have been able to do this using a __hash__ on the class
    itself, but for some reason identical hashes appear distinct to Python,
    so it was useless. No idea why.

    """
    return "{}_{}".format(block.top, block.left)


if __name__ == "__main__":
    main()
