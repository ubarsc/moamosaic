#!/usr/bin/env python3
"""
Get some simple timings on reading/writing a single file
"""
import os
import argparse

from osgeo import gdal

import utils


gdal.UseExceptions()
DFLT_BLOCKSIZE = 2048


def getCmdargs():
    p = argparse.ArgumentParser()
    p.add_argument("infile")
    p.add_argument("outfile")
    p.add_argument("-b", "--blocksize", type=int, default=DFLT_BLOCKSIZE)
    cmdargs = p.parse_args()
    return cmdargs


def main():
    cmdargs = getCmdargs()

    timestamps = utils.TimeStampSet()

    timestamps.stamp(utils.TS_OPENINFILE, utils.TS_START)
    inDs = gdal.Open(cmdargs.infile)
    inBand = inDs.GetRasterBand(1)
    timestamps.stamp(utils.TS_OPENINFILE, utils.TS_END)
    (nrows, ncols) = (inDs.RasterYSize, inDs.RasterXSize)

    drvr = gdal.GetDriverByName("GTiff")
    if os.path.exists(cmdargs.outfile):
        drvr.Delete(cmdargs.outfile)
    outOptions = ["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=IF_SAFER"]
    timestamps.stamp(utils.TS_OPENOUTFILE, utils.TS_START)
    outDs = drvr.Create(cmdargs.outfile, ncols, nrows, 1, inBand.DataType,
        outOptions)
    outBand = outDs.GetRasterBand(1)
    outDs.SetProjection(inDs.GetProjection())
    outDs.SetGeoTransform(inDs.GetGeoTransform())
    timestamps.stamp(utils.TS_OPENOUTFILE, utils.TS_END)

    blockList = utils.makeBlockList(nrows, ncols, cmdargs.blocksize)

    for block in blockList:
        blockId = "{}_{}".format(block.left, block.top)

        tsNameRead = utils.TS_READBLOCK.format(blockId)
        timestamps.stamp(tsNameRead, utils.TS_START)
        arr = inBand.ReadAsArray(block.left, block.top, block.xsize,
            block.ysize)
        timestamps.stamp(tsNameRead, utils.TS_END)

        tsNameWrite = utils.TS_WRITEBLOCK.format(blockId)
        timestamps.stamp(tsNameWrite, utils.TS_START)
        outBand.WriteArray(arr, block.left, block.top)
        timestamps.stamp(tsNameWrite, utils.TS_END)

    timestamps.stamp(utils.TS_CLOSEINFILE, utils.TS_START)
    del inBand
    del inDs
    timestamps.stamp(utils.TS_CLOSEINFILE, utils.TS_END)
    timestamps.stamp(utils.TS_CLOSEOUTFILE, utils.TS_START)
    del outBand
    del outDs
    timestamps.stamp(utils.TS_CLOSEOUTFILE, utils.TS_END)

    print("Total reading", timestamps.timeSpentByPrefix("readblock"))
    print("Total elapsed reading", timestamps.timeElapsedByPrefix("readblock"))
    print("Total writing", timestamps.timeSpentByPrefix("writeblock"))
    print("Total elapsed writing", timestamps.timeElapsedByPrefix("writeblock"))
    print("Total opening", timestamps.timeSpentByPrefix("open"))
    print("Total closing", timestamps.timeSpentByPrefix("close"))

    utils.checkOutput(cmdargs.infile, cmdargs.outfile)


if __name__ == "__main__":
    main()
