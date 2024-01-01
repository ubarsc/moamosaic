"""
Classes for monitoring various things in MoaMosaic
"""
import time

import numpy


class Monitoring:
    def __init__(self):
        self.minMaxBlockCacheSize = MinMax()
        self.minMaxBlockQueueSize = MinMax()
        self.timestamps = TimeStampSet()
        self.params = {}

    def setParam(self, name, value):
        self.params[name] = value

    def reportAsDict(self):
        """
        Return a dictionary of important information, suitable for
        making into JSON.
        """
        d = {}
        d['minMaxBlockCacheSize'] = self.minMaxBlockCacheSize.minMax()
        d['minMaxBlockQueueSize'] = self.minMaxBlockQueueSize.minMax()
        d['timestamps'] = self.timestamps.stamps
        d['params'] = self.params
        return d


class MinMax:
    def __init__(self):
        self.maxval = None
        self.minval = None

    def update(self, val):
        """
        Update max/min vals for given val
        """
        if self.maxval is None or val > self.maxval:
            self.maxval = val
        if self.minval is None or val < self.minval:
            self.minval = val

    def minMax(self):
        "Return list of [minval, maxval]"
        return [self.minval, self.maxval]


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


class TimeStampSet():
    def __init__(self, keySep=':'):
        self.stamps = {}
        self.keySep = keySep

    def __makekey(self, name, startEnd):
        if self.keySep in name:
            raise ValueError("Time stamp name cannot include '{}'".format(
                self.keySep))
        return "{}{}{}".format(name, self.keySep, startEnd)

    def stamp(self, name, startEnd):
        key = self.__makekey(name, startEnd)

        self.stamps[key] = time.time()

    def getStamp(self, name, startEnd):
        """
        Return the stamp for the given name/startEnd
        """
        key = self.__makekey(name, startEnd)
        return self.stamps[key]

    def namesByPrefix(self, prefix):
        """
        Return a list of all names beginning with given prefix
        """
        nameList = list(set([key.split(self.keySep)[0]
                for key in self.stamps]))
        nameList = [name for name in nameList if name.startswith(prefix)]
        return nameList

    def timeSpentByPrefix(self, prefix):
        """
        Add up the total time spent, for the given
        prefix
        """
        tot = 0
        nameList = self.namesByPrefix(prefix)
        for name in nameList:
            startTime = self.getStamp(name, TS_START)
            endTime = self.getStamp(name, TS_END)
            diff = endTime - startTime
            tot += diff
        return round(tot, 2)

    def timeElapsedByPrefix(self, prefix):
        """
        Find the elapsed wall time spent doing anything matching
        prefix. This is the time difference between the earliest
        and latest time stamps.
        """
        nameList = self.namesByPrefix(prefix)
        stampList = []
        for name in nameList:
            stamp = self.getStamp(name, TS_START)
            stampList.append((stamp, TS_START))
            stamp = self.getStamp(name, TS_END)
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
        nameList = self.namesByPrefix(prefix)
        durationList = []
        for name in nameList:
            startTime = self.getStamp(name, TS_START)
            endTime = self.getStamp(name, TS_END)
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
                startStamp = self.getStamp(name, TS_START)
                stampList.append((startStamp, TS_START))
                endStamp = self.getStamp(name, TS_END)
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

    def merge(self, other):
        """
        Merge another timestamps instance into this one
        """
        self.stamps.update(other.stamps)
