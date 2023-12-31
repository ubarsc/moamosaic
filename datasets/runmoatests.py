#!/usr/bin/env python3
"""
This script is designed to generate sets of test data for the mosaic
script. The aim is to have many different but equivalent sets of input
images which would all result in similar mosaics, but allowing us to
run many different mosaics with distinct inputs, thus avoiding any
caching of inputs which would invalidate timings of test runs.

Make a series of mosaic input files. Starting point is the name and
bounds of a central Sentinel-2 tile. This bounding box is queried
via STAC to find the images forming a 3x3 mosaic around it. The central
tile is one chosen to be in the centre of a Sentinel-2 swath, so that
there is a sensible set of tiles surrounding it.

The query is for a single year, and all the dates for those sets of tiles
make up a series of mosaics, for band B02. The other 10m bands would
make similar sets of mosaics, I might use them for extra sets later on.

"""
import sys
import argparse
import json

from pystac_client import Client

from moa import moamosaic


# This holds the known central tile bounds, used for searching
# with STAC. Bounds are in order given to pystac_client.
centralTileBounds = {
    '56JPQ': (153.98, -28.15, 155.1, -27.18)
}
bandList = ['B02', 'B03', 'B04', 'B08']
stacServer = "https://earth-search.aws.element84.com/v1/"
collection = "sentinel-2-l2a"


def getCmdargs():
    """
    Get command line arguments
    """
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--centraltile", default="56JPQ",
        help="Nominated central tile (default=%(default)s)")
    p.add_argument("-l", "--listknowncentraltiles", default=False,
        action="store_true", help="List known central tiles, and exit")
    p.add_argument("-y", "--year", default=2023, type=int,
        help="Year (default=%(default)s)")
    p.add_argument("--monitorjson", default="fullrun.stats.json",
        help="Name of JSON file to save monitoring info (default=%(default)s)")
    p.add_argument("--maxnumthreads", default=5, type=int,
        help=("Maximum number of threads to use in mosaic runs " +
                "(default=%(default)s)"))
    p.add_argument("--blocksize", default=1024, type=int,
        help="Blocksize (in pixels) (default=%(default)s)")

    cmdargs = p.parse_args()

    if cmdargs.listknowncentraltiles:
        for tile in centralTileBounds:
            print(tile)
        sys.exit()

    if cmdargs.centraltile not in centralTileBounds:
        msg = "Unknown central tile '{}'".format(cmdargs.centraltile)
        raise ValueError(msg)

    return cmdargs


def main():
    cmdargs = getCmdargs()

    tilesByDate = searchStac(cmdargs)
    print("Found {} dates".format(len(tilesByDate)))

    mosaicJobList = genJoblist(tilesByDate)
    print("Made {} mosaic jobs".format(len(mosaicJobList)))

    # For each value of numthreads, do this many mosaic jobs, to make a
    # population of runtimes.
    runsPerThreadcount = len(mosaicJobList) // cmdargs.maxnumthreads

    driver = "GTiff"
    outfile = "crap.tif"
    nopyramids = True
    monitorjson = None
    nullval = 0
    outf = open(cmdargs.monitorjson, 'w')

    monitorList = []
    i = 0
    for infileList in mosaicJobList:
        try:
            numthreads = i // runsPerThreadcount + 1
            monitorDict = moamosaic.doMosaic(infileList, outfile,
                numthreads, cmdargs.blocksize, driver, nullval,
                nopyramids, monitorjson)
            monitorList.append(monitorDict)
            print("Done job", i)
        except Exception as e:
            print("Exception {} for job {}".format(e, i))

        i += 1

    json.dump(monitorList, outf, indent=2)


def searchStac(cmdargs):
    """
    Search the STAC server for suitable tiles. Return a dictionary
    of tiles, keyed by date.
    """
    bbox = centralTileBounds[cmdargs.centraltile]

    client = Client.open(stacServer)
    results = client.search(collections=collection, bbox=bbox,
        datetime='{year}-01-01/{year}-12-31'.format(year=cmdargs.year))
    featureCollection = results.item_collection_as_dict()

    tilesByDate = {}
    for feature in featureCollection['features']:
        props = feature['properties']
        datestr = props['datetime'].split('T')[0]
        tilename = props['grid:code'].split('-')[1]
        path = props['earthsearch:s3_path']
        nullPcnt = props['s2:nodata_pixel_percentage']
        if datestr not in tilesByDate:
            tilesByDate[datestr] = []
        tilesByDate[datestr].append((tilename, path, tilename, nullPcnt))
    return tilesByDate


def genFilelist(tileList, band):
    filelist = []
    for (tilename, path, tilename, nullPcnt) in tileList:
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
            for band in bandList:
                filelist = genFilelist(tilesByDate[date], band)
                mosaicJobList.append(filelist)
    return mosaicJobList


if __name__ == "__main__":
    main()
