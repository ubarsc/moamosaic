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
    p = argparse.ArgumentParser()
    p.add_argument("infile")
    p.add_argument("outfile")
    p.add_argument("-b", "--blocksize", type=int, default=2048,
        help="Blocksize (default=%(default)s)")
    p.add_argument("-n", "--numproc", type=int, default=5,
        help="Number of processes for reading (default=%(default)s)")
    cmdargs = p.parse_args()
    return cmdargs


def main():
    cmdargs = getCmdargs()

    # Make a suitable Queue
    manager = Manager()
    que = manager.Queue()

    timestamps = utils.TimeStampSet()

    imginfo = ImageInfo(cmdargs.infile)
    blockList = utils.makeBlockList(imginfo.nrows, imginfo.ncols,
        cmdargs.blocksize)

    timestamps.stamp("whole", utils.TS_START)
    timestamps.stamp("read", utils.TS_START)
    numproc = cmdargs.numproc
    with futures.ProcessPoolExecutor(max_workers=numproc) as procPool:
        procList = []
        for i in range(numproc):
            blockSubset = blockList[i::numproc]
            proc = procPool.submit(readFunc, cmdargs.infile, que, blockSubset)
            procList.append(proc)
    timestamps.stamp("read", utils.TS_END)

    timestamps.stamp("write", utils.TS_START)
    writeBlocks(imginfo, cmdargs.outfile, que, blockList)
    timestamps.stamp("write", utils.TS_END)
    timestamps.stamp("whole", utils.TS_END)

    print("whole", timestamps.timeSpentByPrefix("whole"))
    print("read", timestamps.timeSpentByPrefix("read"))
    print("write", timestamps.timeSpentByPrefix("write"))


def readFunc(infile, que, blockList):
    ds = gdal.Open(infile)
    band = ds.GetRasterBand(1)

    for block in blockList:
        arr = band.ReadAsArray(block.left, block.top, block.xsize, block.ysize)
        que.put((block, arr))


def writeBlocks(imginfo, outfile, que, blockList):
    drvr = gdal.GetDriverByName("GTiff")
    options = ["COMPRESS=DEFLATE", "TILED=YES"]
    ds = drvr.Create(outfile, imginfo.ncols, imginfo.nrows, 1,
        imginfo.dataType, options=options)
    band = ds.GetRasterBand(1)

    # Cache of blocks available to write. Keyed by BlockSpec object.
    blockCache = {}
    print("Que size at start of writeFunc", que.qsize())

    numBlocks = len(blockList)
    print("Num blocks to write", numBlocks)
    i = 0
    while i < numBlocks:
        # Get another block from the que (if available), and cache it
        try:
            (blockSpec, arr) = que.get_nowait()
            key = makeBlockKey(blockSpec)
            blockCache[key] = arr
        except queue.Empty:
            blockSpec = None
            arr = None

        print("i, Cache size", i, len(blockCache))
        # If the i-th block is ready in the cache, write it out,
        # remove it from cache, and increment i
        block = blockList[i]
        key = makeBlockKey(block)
        if key in blockCache:
            arr = blockCache[key]
            band.WriteArray(arr, block.left, block.top)
            blockCache.pop(key)
            i += 1


def makeBlockKey(block):
    """
    Make a key value for the given BlockSpec object.

    I should have been able to do this using a __hash__ on the class
    itself, but for some reason identical hashes appear distinct to Python,
    so it was useless. No idea why.

    """
    return "{}_{}".format(block.top, block.left)


class ImageInfo:
    def __init__(self, filename):
        ds = gdal.Open(str(filename), gdal.GA_ReadOnly)

        (self.ncols, self.nrows) = (ds.RasterXSize, ds.RasterYSize)
        self.transform = ds.GetGeoTransform()
        self.projection = ds.GetProjection()
        self.dataType = ds.GetRasterBand(1).DataType


if __name__ == "__main__":
    main()
