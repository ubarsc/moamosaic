#!/usr/bin/env python
"""
Search for some suitable Sentinel-2 tiles to do mosaics with.

Output is a JSON file, with sets of tiles, collated by date.

"""
import sys
import argparse
import json

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
    p.add_argument("--outjson",
        help="JSON file in which to save STAC search results")

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

    json.dump(tilesByDate, open(cmdargs.outjson, 'w'))


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


if __name__ == "__main__":
    main()
