import time

import numpy
from osgeo import gdal


# Names for timestamp points
TS_START = "start"
TS_END = "end"
TS_OPENINFILE = "openinfile"
TS_OPENOUTFILE = "openoutfile"
TS_READBLOCK = "readblock_{}"
TS_WRITEBLOCK = "writeblock_{}"
TS_CLOSEINFILE = "closeinfile"
TS_CLOSEOUTFILE = "closeoutfile"


def makeBlockList(nrows, ncols, blocksize):
    """
    Divide the raster into blocks
    """
    blockList = []
    top = 0
    while top < nrows:
        ysize = min(blocksize, (nrows - top))
        left = 0
        while left < ncols:
            xsize = min(blocksize, (ncols - left))
            block = BlockSpec(top, left, xsize, ysize)
            blockList.append(block)
            left += xsize
        top += ysize
    return blockList


def checkOutput(infile, outfile):
    """
    Do a brainless but effective check of the output file, by comparing
    it against the input file
    """
    ds1 = gdal.Open(infile)
    b1 = ds1.GetRasterBand(1)
    arr1 = b1.ReadAsArray()
    ds2 = gdal.Open(outfile)
    b2 = ds2.GetRasterBand(1)
    arr2 = b2.ReadAsArray()

    neq = (arr1 != arr2)
    countDiff = numpy.count_nonzero(neq)
    if countDiff > 0:
        raise ComparisonError("Output image differs in {} of {} pixels".format(
            countDiff, arr1.size))


class ComparisonError(Exception):
    pass


class BlockSpec:
    def __init__(self, top, left, xsize, ysize):
        self.top = top
        self.left = left
        self.xsize = xsize
        self.ysize = ysize

    def __str__(self):
        s = "{} {} {} {}".format(self.top, self.left, self.xsize, self.ysize)
        return s


class TimeStampSet():
    def __init__(self):
        self.stamps = {}

    def stamp(self, name, startEnd):
        key = (name, startEnd)

        self.stamps[key] = time.time()

    def timeSpentByPrefix(self, prefix):
        """
        Add up the total time spent, for the given
        prefix
        """
        tot = 0
        nameList = list(set([name for (name, startEnd) in self.stamps
            if name.startswith(prefix)]))
        for name in nameList:
            startTime = self.stamps[(name, TS_START)]
            endTime = self.stamps[(name, TS_END)]
            diff = endTime - startTime
            tot += diff
        return tot

    def timeElapsedByPrefix(self, prefix):
        """
        Find the elapsed wall time spent doing anything matching
        prefix. This is the time difference between the earliest
        and latest time stamps.
        """
        nameList = list(set([name for (name, startEnd) in self.stamps
            if name.startswith(prefix)]))
        stampList = []
        for name in nameList:
            stamp = self.stamps[(name, TS_START)]
            stampList.append((stamp, TS_START))
            stamp = self.stamps[(name, TS_END)]
            stampList.append((stamp, TS_END))
        # Sort the events into chronological order
        stampList = sorted(stampList)
        # Count when we were in at least one action
        tot = 0
        count = 0
        prevStamp = stampList[0][0]
        for (stamp, flag) in stampList:
            if count > 0:
                # There is at least one 'prefix' action still going, so
                # this time period counts, add it to the total
                tot += (stamp - prevStamp)
            prevStamp = stamp

            if flag == TS_START:
                count += 1
            else:
                count -= 1
        return tot
