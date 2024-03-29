#!/usr/bin/env python3
"""
Run timings for gdal_merge.py, using similar mosaics to those
in runmoatiming.py. This makes a comparable set of timings.

"""
import os
import argparse
import json
import time
import subprocess

from osgeo import gdal


def getCmdargs():
    """
    Get command line arguments
    """
    p = argparse.ArgumentParser()
    p.add_argument("--stacresults", help=("JSON file of pre-computed " +
        "STAC search results"))
    p.add_argument("--outjson", help=("Name of JSON file to save " +
        "monitoring info"))
    cmdargs = p.parse_args()
    return cmdargs


def main():
    cmdargs = getCmdargs()
    gdal.UseExceptions()

    tilesByDate = json.load(open(cmdargs.stacresults))
    mosaicJobList = genJoblist(tilesByDate)
    # Limit to 40 jobs
    mosaicJobList = mosaicJobList[:40]

    outfile = 'testimg.tif'

    timeList = []
    for infileList in mosaicJobList:
        if os.path.exists(outfile):
            os.remove(outfile)

        cmdList = ['gdal_merge.py', '-o', outfile, '-of', 'GTiff',
                     '-co', 'COMPRESS=DEFLATE', '-co', 'TILED=YES',
                     '-co', 'BIGTIFF=IF_SAFER', '-co', 'INTERLEAVE=BAND',
                     '-n', '0', '-a_nodata', '0'
                   ] + infileList

        t0 = time.time()
        subprocess.run(cmdList)
        t1 = time.time()
        elapsed = t1 - t0
        timeList.append(elapsed)

    json.dump(timeList, open(cmdargs.outjson, 'w'))


def genFilelist(tileList, band):
    filelist = []
    for (tilename, path, nullPcnt) in tileList:
        vsiPath = path.replace("s3:/", "/vsis3")
        fn = "{}/{}.tif".format(vsiPath, band)
        filelist.append(fn)
    return filelist


def genJoblist(tilesByDate):
    """
    Generate a list of mosaic jobs to do. Each job is a list of
    nine adjacent tiles for a given date and band. Return a list
    of these lists.
    """
    datelist = sorted(tilesByDate.keys())
    mosaicJobList = []
    for date in datelist:
        if len(tilesByDate[date]) == 9:
            filelist = genFilelist(tilesByDate[date], "B02")
            mosaicJobList.append(filelist)
    return mosaicJobList


if __name__ == "__main__":
    main()
