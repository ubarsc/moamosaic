#!/usr/bin/env python
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

from pystac_client import Client


# This holds the known central tile bounds, used for searching
# with STAC. Bounds are in order given to pystac_client.
centralTileBounds = {
    '56JPQ': (153.98, -28.15, 155.1, -27.18)
}
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
    cmdargs = p.parse_args()
    return cmdargs


def main():
    cmdargs = getCmdargs()
    if cmdargs.listknowncentraltiles:
        for tile in centralTileBounds:
            print(tile)
        sys.exit()

    if cmdargs.centraltile not in centralTileBounds:
        msg = "Unknown central tile '{}'".format(cmdargs.centraltile)
        raise ValueError(msg)

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
        if datestr not in tilesByDate:
            tilesByDate[datestr] = []
        tilesByDate[datestr].append((tilename, path))
    print("Found {} dates".format(len(tilesByDate)))

    datelist = sorted(tilesByDate.keys())
    for date in datelist:
        if len(tilesByDate[date]) == 9:
            writeOutfile(cmdargs.centraltile, date, tilesByDate[date])


def writeOutfile(centraltile, date, tileList):
    filename = "{}-3x3_{}.txt".format(centraltile, date)
    f = open(filename, 'w')
    for (tilename, path) in tileList:
        vsiPath = path.replace("s3:/", "/vsis3")
        f.write("{}/B02.tif\n".format(vsiPath))
    f.close()


if __name__ == "__main__":
    main()
