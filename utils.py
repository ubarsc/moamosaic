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
TS_WHOLEPROGRAM = "program"


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
        return round(tot, 2)

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
        return round(tot, 2)

    def avgTimeByPrefix(self, prefix):
        """
        Over all start/end pairs matching the given prefix, find the
        average time taken from start to end.
        """
        nameList = list(set([name for (name, startEnd) in self.stamps
            if name.startswith(prefix)]))
        durationList = []
        for name in nameList:
            startTime = self.stamps[(name, TS_START)]
            endTime = self.stamps[(name, TS_END)]
            duration = endTime - startTime
            durationList.append(duration)
        avgTime = sum(durationList) / len(durationList)
        return round(avgTime, 3)

    def pcntOverlapByGroup(self, groupList):
        """
        Percentage overlapping calculations within each group of
        the list. Return array of percentages, one for each group.
        """
        pcntList = []
        for group in groupList:
            stampList = []
            for name in group:
                startStamp = self.stamps[(name, TS_START)]
                stampList.append((startStamp, TS_START))
                endStamp = self.stamps[(name, TS_END)]
                stampList.append((endStamp, TS_END))
            # Sort the events into chronological order
            stampList = sorted(stampList)

            # Count when we were in more than one action, i.e. there was some
            # overlap going on.
            totInOverlap = 0
            count = 0
            prevStamp = stampList[0][0]
            for (stamp, flag) in stampList:
                if count > 1:
                    # There is more than one action still going, so
                    # this time period counts, add it to the total
                    totInOverlap += (stamp - prevStamp)
                prevStamp = stamp

                if flag == TS_START:
                    count += 1
                else:
                    count -= 1

            totElapsed = stampList[-1][0] - stampList[0][0]
            pcnt = 100 * totInOverlap / totElapsed
            pcntList.append(pcnt)
        return numpy.array(pcntList)
