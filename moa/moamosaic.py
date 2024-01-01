#!/usr/bin/env python3
"""
MoaMosaic is a tool for mosaicing larger numbers of input raster
image files into a single output raster. It uses threading to overlap
the reading of inputs from slower storage, such as S3 buckets. In many
other situations there is little advantage in this, but the latencies
involved in reading from remote files mean there is significant benefit
to reading blocks of data in parallel.

The software is named for the Moa, a group of large flightless birds
native to New Zealand (now extinct).
See https://en.wikipedia.org/wiki/Moa

MoaMosaic relies on GDAL to read and write raster files, so any format
supported by GDAL may be used. This includes all of its "/vsi" virtual
file systems, so support for files on S3 is available via /vsis3/.

"""
import os
import argparse
from concurrent import futures
import queue
import json
from multiprocessing import cpu_count

import numpy
from osgeo import gdal
from osgeo.gdal_array import GDALTypeCodeToNumericTypeCode

from rios import pixelgrid

from moa import monitoring


gdal.UseExceptions()


# Some default values
DFLT_NUMTHREADS = 4
DFLT_BLOCKSIZE = 1024
DFLT_DRIVER = "GTiff"
defaultCreationOptions = {
    'GTiff': ['COMPRESS=DEFLATE', 'TILED=YES', 'BIGTIFF=IF_SAFER',
        'INTERLEAVE=BAND'],
    'KEA': [],
    'HFA': ['COMPRESS=YES', 'IGNORE_UTM=TRUE']
}


def getCmdargs():
    """
    Get command line arguments
    """
    knownDrivers = ','.join(defaultCreationOptions.keys())

    p = argparse.ArgumentParser()
    p.add_argument("-i", "--infilelist", help="Text file list of input images")
    p.add_argument("-n", "--numthreads", type=int, default=4,
        help="Number of read threads to use (default=%(default)s)")
    p.add_argument("-b", "--blocksize", type=int, default=1024,
        help="Blocksize in pixels (default=%(default)s)")
    p.add_argument("-d", "--driver", default="GTiff",
        help="GDAL driver to use for output file (default=%(default)s)")
    p.add_argument("-o", "--outfile", help="Name of output raster")
    p.add_argument("--creationoption", action="append",
        help=("Specify a GDAL creation option (as 'NAME=VALUE'). Can be " +
              "given multiple times. There are sensible default creation " +
              "options for some drivers ({}), but if this option is used, " +
              "those are ignored.").format(knownDrivers))
    p.add_argument("--nullval", type=int,
        help="Null value to use (default comes from input files)")
    p.add_argument("--nopyramids", default=False, action="store_true",
        help="Omit the pyramid layers (i.e. overviews)")
    p.add_argument("--monitorjson", help="Output JSON file of monitoring info")
    cmdargs = p.parse_args()
    return cmdargs


def mainCmd():
    """
    Main command line stub, referenced from pyproject.toml
    """
    cmdargs = getCmdargs()
    filelist = makeFilelist(cmdargs.infilelist)
    monitorDict = doMosaic(filelist, cmdargs.outfile,
            numthreads=cmdargs.numthreads,
            blocksize=cmdargs.blocksize,
            driver=cmdargs.driver,
            nullval=cmdargs.nullval,
            nopyramids=cmdargs.nopyramids,
            creationoptions=cmdargs.creationoption)

    if cmdargs.monitorjson is not None:
        with open(cmdargs.monitorjson, 'w') as f:
            json.dump(monitorDict, f, indent=2)


def doMosaic(filelist, outfile, *, numthreads=DFLT_NUMTHREADS,
        blocksize=DFLT_BLOCKSIZE, driver=DFLT_DRIVER, nullval=None,
        nopyramids=False, creationoptions=None):
    """
    Main routine, callable from non-commandline context
    """
    monitors = monitoring.Monitoring()
    monitors.setParam('numthreads', numthreads)
    monitors.setParam('blocksize', blocksize)
    monitors.setParam('cpucount', cpu_count())

    # Work out what we are going to do
    monitors.setParam('numinfiles', len(filelist))
    monitors.timestamps.stamp("imginfodict", monitoring.TS_START)
    imgInfoDict = makeImgInfoDict(filelist, numthreads)
    monitors.timestamps.stamp("imginfodict", monitoring.TS_END)

    if nullval is None:
        nullval = imgInfoDict[filelist[0]].nullVal

    monitors.timestamps.stamp("analysis", monitoring.TS_START)
    outgrid = makeOutputGrid(filelist, imgInfoDict)
    outGeoTransform = outgrid.makeGeoTransform()
    blockList = makeOutputBlockList(outgrid, blocksize)

    (blockListWithInputs, filesForBlock) = (
        findInputsPerBlock(blockList, outGeoTransform, filelist, imgInfoDict))
    blockReadingList = makeBlockReadingList(blockListWithInputs)
    blocksPerThread = divideBlocksByThread(blockReadingList, numthreads)
    monitors.timestamps.stamp("analysis", monitoring.TS_END)

    blockQ = queue.Queue()
    poolClass = futures.ThreadPoolExecutor
    numBands = imgInfoDict[filelist[0]].numBands

    # Now do it all, using concurrent threads to read blocks into a queue
    outImgInfo = makeOutImgInfo(imgInfoDict[filelist[0]], outgrid, nullval)
    outDs = openOutfile(outfile, driver, outgrid, outImgInfo, creationoptions)
    monitors.timestamps.stamp("domosaic", monitoring.TS_START)
    for bandNum in range(1, numBands + 1):
        with poolClass(max_workers=numthreads) as threadPool:
            workerList = []
            for i in range(numthreads):
                blocksToRead = blocksPerThread[i]
                worker = threadPool.submit(readFunc, blocksToRead, blockQ,
                        bandNum, outImgInfo.nullVal)
                workerList.append(worker)

            writeFunc(outgrid, blockQ, outDs, outImgInfo, bandNum,
                    blockList, filesForBlock, workerList, monitors)
    monitors.timestamps.stamp("domosaic", monitoring.TS_END)

    outDs.SetGeoTransform(outGeoTransform)
    outDs.SetProjection(outImgInfo.projection)
    if not nopyramids:
        outDs.BuildOverviews(overviewlist=[4, 8, 16, 32, 64, 128, 256, 512])

    return monitors.reportAsDict()


def readFunc(blocksToRead, blockQ, bandNum, outNullVal):
    """
    This function is run by all the read workers, each with its own list
    of blocks to read.
    """
    blocksPerInfile = BlocksByInfile()
    for blockInfo in blocksToRead:
        blocksPerInfile.blockToDo(blockInfo.filename, blockInfo.outblock)
    gdalObjCache = GdalObjCache()

    i = 0
    for blockInfo in blocksToRead:
        filename = blockInfo.filename
        (ds, band) = gdalObjCache.openBand(filename, bandNum)
        inblock = blockInfo.inblock
        (left, top, xsize, ysize) = (inblock.left, inblock.top,
                inblock.xsize, inblock.ysize)
        # Don't try to read outside the extent of the infile
        left1 = max(left, 0)
        top1 = max(top, 0)
        right1 = min(left + xsize, ds.RasterXSize)
        xsize1 = right1 - left1
        bottom1 = min(top + ysize, ds.RasterYSize)
        ysize1 = bottom1 - top1
        arr = band.ReadAsArray(left1, top1, xsize1, ysize1)

        # Now slot this possibly smaller array back into a full array,
        # with null padding.
        outArr = numpy.zeros((ysize, xsize), dtype=arr.dtype)
        outArr.fill(outNullVal)
        coloffset = max(0, -left)
        rowoffset = max(0, -top)
        outArr[rowoffset:rowoffset+ysize1, coloffset:coloffset+xsize1] = arr

        # Put the full bloc into the blockQ, along with the associated
        # block information
        blockQ.put((blockInfo, outArr))

        # If this input file is now done, we can close it.
        blocksPerInfile.blockDone(filename, blockInfo.outblock)
        if blocksPerInfile.countRemaining(filename) == 0:
            gdalObjCache.closeBand(filename, bandNum)
        i += 1


def writeFunc(outgrid, blockQ, outDs, outImgInfo, bandNum,
                    blockList, filesForBlock, workerList, monitors):
    """
    Loop over all blocks of the output grid, and write them.

    Input blocks are retrieved from the blockQ, and placed in a block
    cache. When all inputs for a given output block are available,
    that block is assembled for output, merging the inputs appropriately.
    The resulting block is written to the output file.

    The input blocks are deleted from the cache. All worker processes
    are then checked for exceptions. Then move to the next output block.

    This function runs continuously for a single band of the output file,
    after which it returns. It will then be called again for the next band.

    """
    band = outDs.GetRasterBand(bandNum)

    # Cache of blocks available to write
    blockCache = BlockCache()

    numOutBlocks = len(blockList)
    i = 0
    while i < numOutBlocks:
        # Get another block from the blockQ (if available), and cache it
        if not blockQ.empty():
            (blockInfo, arr) = blockQ.get_nowait()
            filename = blockInfo.filename
            outblock = blockInfo.outblock
            blockCache.add(filename, outblock, arr)
        else:
            blockInfo = None
            arr = None

        outblock = blockList[i]

        if outblock not in filesForBlock:
            # This block does not intersect any input files, so
            # just write nulls
            numpyDtype = GDALTypeCodeToNumericTypeCode(outImgInfo.dataType)
            outArr = numpy.zeros((outblock.ysize, outblock.xsize),
                    dtype=numpyDtype)
            outArr.fill(outImgInfo.nullVal)
            band.WriteArray(outArr, outblock.left, outblock.top)
            i += 1
        elif blockInfo is not None or len(blockCache) > 0:
            # If we actually got something from the blockQ, then we might
            # be ready to write the current block

            allInputBlocks = getInputsForBlock(blockCache, outblock,
                    filesForBlock)
            if allInputBlocks is not None:
                outArr = mergeInputs(allInputBlocks, outImgInfo.nullVal)
                band.WriteArray(outArr, outblock.left, outblock.top)

                # Remove all inputs from cache
                for filename in filesForBlock[outblock]:
                    blockCache.remove(filename, outblock)

                # Proceed to the next output block
                i += 1

        checkReaderExceptions(workerList)

        monitors.minMaxBlockCacheSize.update(len(blockCache))
        monitors.minMaxBlockQueueSize.update(blockQ.qsize())

    band.SetNoDataValue(outImgInfo.nullVal)


def checkReaderExceptions(workerList):
    """
    Check the read workers, in case one has raised an exception. The
    elements of workerList are futures.Future objects.
    """
    for worker in workerList:
        if worker.done():
            e = worker.exception(timeout=0)
            if e is not None:
                raise e


def allWorkersDone(workerList):
    """
    Return True if all owrkers are done
    """
    allDone = True
    for worker in workerList:
        if not worker.done():
            allDone = False
    return allDone


def makeFilelist(infilelist):
    """
    Read the list of input files, and return a list of the filenames
    """
    filelist = [line.strip() for line in open(infilelist)]
    return filelist


def makeOutputGrid(filelist, imgInfoDict):
    """
    Work out the extent of the whole mosaic. Return a pixel grid
    of the whole thing.
    """
    # For now, we will assume that all inputs are in the same projection,
    # pixel size and grid alignment, but later we may intercede at this
    # point to generate some VRT files to reproject on the fly.

    # Use RIOS's pixelgrid module to work out the union of all input rasters.
    # Probably should re-write this bit to be independent of RIOS, eventually.
    infoList = [imgInfoDict[fn] for fn in filelist]
    pixgridList = [pixelgrid.PixelGridDefn(geotransform=info.transform,
            nrows=info.nrows, ncols=info.ncols) for info in infoList]
    unionGrid = pixgridList[0]
    for pixgrid in pixgridList[1:]:
        unionGrid = unionGrid.union(pixgrid)
    return unionGrid


def makeOutputBlockList(outgrid, blocksize):
    """
    Given a pixel grid of the whole extent, divide it up into blocks.
    Return a list of BlockSpec objects.
    """
    # Divide this up into blocks
    # Should do something to avoid tiny blocks on the right and bottom edges...
    (nrows, ncols) = outgrid.getDimensions()
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


def makeImgInfoDict(filelist, numthreads):
    """
    Create ImageInfo objects for all the given input files.
    Store these in a dictionary, keyed by their filenames.
    """
    # I originally had the following elegant multi-threaded implementation
    # of this. However, for reasons which are not at all clear to me,
    # this does not play well with the HDF5 library. So, if the input
    # files are KEA format files, this code throws up lots of scary
    # error messages from deep within the HDF5 library. So, I have disabled
    # this, in favour of a simple sequential version. Hopefully it does not
    # make much difference. It worked fine with GTiff inputs.
    #
    # poolClass = futures.ThreadPoolExecutor
    # with poolClass(max_workers=numthreads) as threadPool:
    #     imgInfoList = threadPool.map(ImageInfo, filelist)
    #
    # imgInfoDict = {fn: info for (fn, info) in zip(filelist, imgInfoList)}

    imgInfoDict = {}
    for filename in filelist:
        imgInfoDict[filename] = ImageInfo(filename)
    return imgInfoDict


def findInputsPerBlock(blockList, outGeoTransform, filelist, imgInfoDict):
    """
    For every block, work out which input files intersect with it,
    and the bounds of that block, in each file's pixel coordinate system.
    """
    blockListWithInputs = []
    filesForBlock = {}
    for block in blockList:
        blockWithInputs = BlockSpecWithInputs(block)

        for filename in filelist:
            imginfo = imgInfoDict[filename]
            (fileLeft, fileTop, fileRight, fileBottom) = (
                block.transformToFilePixelCoords(outGeoTransform, imginfo))
            intersects = ((fileRight + 1) >= 0 and (fileBottom + 1) >= 0 and
                fileLeft <= imginfo.ncols and fileTop <= imginfo.nrows)

            if intersects:
                xs = fileRight - fileLeft
                ys = fileBottom - fileTop
                inblock = BlockSpec(fileTop, fileLeft, xs, ys)
                blockWithInputs.add(filename, inblock)

                if block not in filesForBlock:
                    filesForBlock[block] = []
                filesForBlock[block].append(filename)

        if len(blockWithInputs.infilelist) > 0:
            blockListWithInputs.append(blockWithInputs)

    return (blockListWithInputs, filesForBlock)


def makeBlockReadingList(blockListWithInputs):
    """
    Make a single list of all the blocks to be read. This is returned as
    a list of BlockReadingSpec objects
    """
    blockReadingList = []
    for blockWithInputs in blockListWithInputs:
        outblock = blockWithInputs.outblock
        n = len(blockWithInputs.infilelist)
        for i in range(n):
            filename = blockWithInputs.infilelist[i]
            inblock = blockWithInputs.inblocklist[i]
            blockInfo = BlockReadingSpec(outblock, filename, inblock)
            blockReadingList.append(blockInfo)
    return blockReadingList


def divideBlocksByThread(blockReadingList, numthreads):
    """
    Divide up the given blockReadingList into several such lists, one
    per thread. Return a list of these sub-lists.
    """
    blocksPerThread = []
    for i in range(numthreads):
        sublist = blockReadingList[i::numthreads]
        blocksPerThread.append(sublist)
    return blocksPerThread


def getInputsForBlock(blockCache, outblock, filesForBlock):
    """
    Search the block cache for all the expected inputs for the current
    block. If all are found, return a list of them. If any are still
    missing from the cache, then just return None.
    """
    allInputsForBlock = []
    i = 0
    shp = None
    missing = False
    filelist = filesForBlock[outblock]
    numFiles = len(filelist)
    while i < numFiles and not missing:
        filename = filelist[i]
        k = blockCache.makeKey(filename, outblock)
        if k in blockCache.cache:
            (blockSpec, arr) = blockCache.cache[k]
            # Check on array shape. They must all be the same shape
            if shp is None:
                shp = arr.shape
            if arr.shape != shp:
                msg = ("Block array mismatch at block {}\n".format(
                       blockSpec) +
                       "{}!={}\n{}".format(arr.shape, shp, filelist)
                       )
                raise ValueError(msg)

            allInputsForBlock.append(arr)
            i += 1
        else:
            missing = True
            allInputsForBlock = None

    return allInputsForBlock


def openOutfile(outfile, driver, outgrid, outImgInfo, creationoptions):
    """
    Open the output file
    """
    (nrows, ncols) = outgrid.getDimensions()
    numBands = outImgInfo.numBands
    datatype = outImgInfo.dataType
    if creationoptions is None:
        creationoptions = defaultCreationOptions[driver]
    drvr = gdal.GetDriverByName(driver)
    if drvr is None:
        msg = "Driver {} not supported in this version of GDAL".format(driver)
        raise ValueError(msg)

    if os.path.exists(outfile):
        drvr.Delete(outfile)
    ds = drvr.Create(outfile, ncols, nrows, numBands, datatype,
        creationoptions)
    return ds


def mergeInputs(allInputsForBlock, outNullVal):
    """
    Given a list of input arrays, merge to produce the final
    output array. Ordering is important, the last non-null
    value is the one used.
    """
    numInputs = len(allInputsForBlock)
    outArr = allInputsForBlock[0]
    for i in range(1, numInputs):
        arr = allInputsForBlock[i]
        nonNull = (arr != outNullVal)
        outArr[nonNull] = arr[nonNull]
    return outArr


def makeOutImgInfo(inImgInfo, outgrid, nullval):
    """
    Create an ImageInfo for the output file, based on one of the
    input files, and information from the outgrid and the nullval.
    """
    outImgInfo = ImageInfo(None)
    (outImgInfo.nrows, outImgInfo.ncols) = outgrid.getDimensions()
    outImgInfo.numBands = inImgInfo.numBands
    outImgInfo.transform = inImgInfo.transform
    outImgInfo.projection = inImgInfo.projection
    outImgInfo.dataType = inImgInfo.dataType
    outImgInfo.nullVal = inImgInfo.nullVal
    if nullval is not None:
        outImgInfo.nullVal = nullval
    return outImgInfo


class ImageInfo:
    """
    Just the critical information about the given GDAL raster file.
    """
    def __init__(self, filename):
        if filename is not None:
            ds = gdal.Open(str(filename), gdal.GA_ReadOnly)

            (self.ncols, self.nrows) = (ds.RasterXSize, ds.RasterYSize)
            self.transform = ds.GetGeoTransform()
            self.projection = ds.GetProjection()
            band1 = ds.GetRasterBand(1)
            self.dataType = band1.DataType
            self.numBands = ds.RasterCount
            self.nullVal = band1.GetNoDataValue()
        else:
            self.nrows = None
            self.ncols = None
            self.transform = None
            self.projection = None
            self.dataType = None
            self.numBands = None
            self.nullVal = None


class BlockSpec:
    """
    The basic pixel coordinates of a single raster block. The values are
    those required by ReadAsArray and WriteArray.
    """
    def __init__(self, top, left, xsize, ysize):
        self.top = top
        self.left = left
        self.xsize = xsize
        self.ysize = ysize

    def transformToFilePixelCoords(self, geotransform, imginfo):
        """
        Transform the block's own pixel coordinates into the pixel
        coordinates for the given ImageInfo object.

        The given geotransform (GDAL conventions) maps the block's
        pixel coordinates into world coordinates, assumed to be the same
        as those of the imginfo object.

        Return a tuple (left, top, right, bottom). These values are floats,
        but can be truncated to be integer pixel coords.

        """
        # World coords of block outer bounds
        (xLeft, yTop) = gdal.ApplyGeoTransform(geotransform, self.left,
            self.top)
        (right, bottom) = (self.left + self.xsize,
            self.top + self.ysize)
        (xRight, yBottom) = gdal.ApplyGeoTransform(geotransform, right, bottom)

        # Block bounds in image's pixel coords
        imgInvGT = gdal.InvGeoTransform(imginfo.transform)
        (fileLeft, fileTop) = gdal.ApplyGeoTransform(imgInvGT, xLeft, yTop)
        (fileRight, fileBottom) = gdal.ApplyGeoTransform(imgInvGT, xRight,
            yBottom)

        return (int(round(fileLeft)), int(round(fileTop)),
                int(round(fileRight)), int(round(fileBottom)))

    def __str__(self):
        s = "{} {} {} {}".format(self.top, self.left, self.xsize, self.ysize)
        return s

    # Define __hash__ and __eq__ so we can use these objects as
    # dictionary keys.

    def __eq__(self, other):
        eq = (self.top == other.top and self.left == other.left and
            self.xsize == other.xsize and self.ysize == other.ysize)
        return eq

    def __hash__(self):
        return hash((self.top, self.left, self.xsize, self.ysize))


class BlockSpecWithInputs:
    """
    To hold an output block, plus the input blocks that go into it
    """
    def __init__(self, outblock):
        self.outblock = outblock
        self.infilelist = []
        self.inblocklist = []

    def add(self, filename, inblock):
        self.infilelist.append(filename)
        self.inblocklist.append(inblock)


class BlockReadingSpec:
    """
    All required to read a single block from infile, and align it for output
    """
    def __init__(self, outblock, filename, inblock):
        self.outblock = outblock
        self.filename = filename
        self.inblock = inblock


class BlocksByInfile:
    """
    Keep track of which blocks have been read from which files. When created,
    it is filled with all of the blocks to read, and then as they are done,
    they are removed. This allows us to know when all blocks have been read
    for a particular input file, and it can be closed.

    The BlockSpec objects passed in to each method are always in terms
    of the output grid.
    """
    def __init__(self):
        # Keyed by filename, each element is a set of __blockId() values
        self.blockSets = {}

    def __blockId(self, blockSpec):
        "Make the block ID value used in each set of blocks"
        return (blockSpec.top, blockSpec.left)

    def blockDone(self, filename, block):
        "This block has been read for the given file"
        self.blockSets[filename].remove(self.__blockId(block))

    def blockToDo(self, filename, block):
        "Add a block to read for the given file"
        if filename not in self.blockSets:
            self.blockSets[filename] = set()
        self.blockSets[filename].add(self.__blockId(block))

    def countRemaining(self, filename):
        return len(self.blockSets[filename])


class BlockCache:
    """
    Cache of blocks which have been read in, but not yet processed
    """
    def __init__(self):
        self.cache = {}

    def makeKey(self, filename, block):
        """
        Return a key for the given block/filename
        """
        s = "{}_{}_{}".format(filename, block.top, block.left)
        return s

    def add(self, filename, block, arr):
        """
        Add the given array to the cache, as per the blockInfo
        """
        key = self.makeKey(filename, block)
        self.cache[key] = (block, arr)

    def remove(self, filename, outblock):
        """
        Remove the given block from cache
        """
        key = self.makeKey(filename, outblock)
        self.cache.pop(key)

    def keys(self):
        return list(self.cache.keys())

    def __len__(self):
        return len(self.cache)


class GdalObjCache:
    """
    Cache the open gdal.Band and gdal.Dataset objects.
    """
    def __init__(self):
        self.cache = {}

    def openBand(self, filename, bandNum):
        """
        Open the given band and return the gdal.Band
        object. If it is already open, and in the cache,
        just return that Band object.
        """
        key = (filename, bandNum)
        if key not in self.cache:
            ds = gdal.Open(filename)
            band = ds.GetRasterBand(bandNum)
            self.cache[key] = (ds, band)
        else:
            (ds, band) = self.cache[key]
        return (ds, band)

    def closeBand(self, filename, bandNum):
        """
        Close the band and dataset and remove from cache
        """
        key = (filename, bandNum)
        if key in self.cache:
            (ds, band) = self.cache[key]
            del band
            del ds
            self.cache.pop(key)

    def __len__(self):
        return len(self.cache)


if __name__ == "__main__":
    mainCmd()
