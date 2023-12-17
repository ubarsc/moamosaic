#!/usr/bin/env python3
"""
Playing around with async. No idea what I am doing.
"""
import os
import argparse
import asyncio

from osgeo import gdal

import utils


gdal.UseExceptions()
DFLT_BLOCKSIZE = 1024


def getCmdargs():
    p = argparse.ArgumentParser()
    p.add_argument("infile")
    p.add_argument("outfile")
    p.add_argument("-b", "--blocksize", type=int, default=DFLT_BLOCKSIZE,
        help="Blocksize in pixels (default=%(default)s)")
    p.add_argument("-t", "--threads", type=int, default=5,
        help="Number of threads (default=%(default)s)")
    cmdargs = p.parse_args()
    return cmdargs


def main():
    cmdargs = getCmdargs()

    timestamps = utils.TimeStampSet()

    inDs = gdal.Open(cmdargs.infile)
    inBand = inDs.GetRasterBand(1)
    (nrows, ncols) = (inDs.RasterYSize, inDs.RasterXSize)

    drvr = gdal.GetDriverByName("GTiff")
    if os.path.exists(cmdargs.outfile):
        drvr.Delete(cmdargs.outfile)
    outOptions = ["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=IF_SAFER"]
    outDs = drvr.Create(cmdargs.outfile, ncols, nrows, 1, inBand.DataType,
        outOptions)
    outBand = outDs.GetRasterBand(1)
    outDs.SetProjection(inDs.GetProjection())
    outDs.SetGeoTransform(inDs.GetGeoTransform())

    blockList = utils.makeBlockList(nrows, ncols, cmdargs.blocksize)
    asyncio.run(mainAsync(blockList, inBand, outBand, cmdargs, timestamps))

    del inBand
    del inDs
    del outBand
    del outDs

    print("Total reading", timestamps.timeSpentByPrefix("readblock"))
    print("Total elapsed reading", timestamps.timeElapsedByPrefix("readblock"))
    print("Total writing", timestamps.timeSpentByPrefix("writeblock"))
    print("Total elapsed writing", timestamps.timeElapsedByPrefix("writeblock"))

    utils.checkOutput(cmdargs.infile, cmdargs.outfile)


async def mainAsync(blockList, inBand, outBand, cmdargs, timestamps):
    """
    Main asynchronous routine
    """
    numBlocks = len(blockList)
    i = 0
    while i < numBlocks:
        subList = blockList[i:i + cmdargs.threads]

        taskList = []
        for block in subList:
            blockId = "{}_{}".format(block.left, block.top)

            task = asyncio.create_task(readBlock(inBand, block, timestamps,
                blockId))
            taskList.append(task)
        await asyncio.gather(*taskList)

        for j in range(len(taskList)):
            block = subList[j]
            arr = taskList[j].result()

            blockId = "{}_{}".format(block.left, block.top)
            tsNameWrite = utils.TS_WRITEBLOCK.format(blockId)
            timestamps.stamp(tsNameWrite, utils.TS_START)
            outBand.WriteArray(arr, block.left, block.top)
            timestamps.stamp(tsNameWrite, utils.TS_END)

        i += len(subList)


async def readBlock(inBand, block, timestamps, blockId):
    """
    Read a single block, asynchronously, and record timestamps
    """
    tsNameRead = utils.TS_READBLOCK.format(blockId)
    timestamps.stamp(tsNameRead, utils.TS_START)
    arr = inBand.ReadAsArray(block.left, block.top, block.xsize,
        block.ysize)
    timestamps.stamp(tsNameRead, utils.TS_END)
    return arr


if __name__ == "__main__":
    main()
