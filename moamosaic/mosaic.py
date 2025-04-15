"""
Core module of the moamosaic package.

The main function to call in this module is the `doMosaic` function.

"""
import os
import argparse
from concurrent import futures
import queue
import json
import shutil
from multiprocessing import cpu_count

import numpy
from osgeo import gdal
from osgeo.gdal_array import GDALTypeCodeToNumericTypeCode

from . import monitoring
from . import structures
from . import reproj


# Some default values
DFLT_NUMTHREADS = 4
DFLT_BLOCKSIZE = 1024
DFLT_DRIVER = "GTiff"
DFLT_RESAMPLEMETHOD = "near"
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
    p.add_argument("--co", action="append",
        help=("Specify a GDAL creation option (as 'NAME=VALUE'). Can be " +
              "given multiple times. There are sensible default creation " +
              "options for some drivers ({}), but if this option is used, " +
              "those are ignored.").format(knownDrivers))
    p.add_argument("--nullval", type=int,
        help="Null value to use (default comes from input files)")
    p.add_argument("--monitorjson",
        help="Output JSON file of monitoring info (optional)")

    outprojGroup = p.add_argument_group("Output Projection Options",
        description=("Default projection matches the input files. The " +
            "following options are used to specify something different. " +
            "Use only one of --outprojepsg or --outprojwktfile"))
    outprojGroup.add_argument("--outprojepsg", type=int,
        help="EPSG number of desired output projection")
    outprojGroup.add_argument("--outprojwktfile",
        help="Name of text file containing WKT of desired output projection")
    outprojGroup.add_argument("--xres", type=float,
        help="Desired output X pixel size (default matches input)")
    outprojGroup.add_argument("--yres", type=float,
        help="Desired output Y pixel size (default matches input)")
    outprojGroup.add_argument("--resample", default=DFLT_RESAMPLEMETHOD,
        help=("GDAL name of resampling method to use for " +
            "reprojection (default='%(default)s')"))
    cmdargs = p.parse_args()
    return cmdargs


def mainCmd():
    """
    Main command line wrapper for the doMosaic function.

    This function is referenced from pyproject.toml to create command
    line script.
    """
    gdal.UseExceptions()

    cmdargs = getCmdargs()
    filelist = makeFilelist(cmdargs.infilelist)
    monitors = doMosaic(filelist, cmdargs.outfile,
        numthreads=cmdargs.numthreads, blocksize=cmdargs.blocksize,
        driver=cmdargs.driver, nullval=cmdargs.nullval,
        creationoptions=cmdargs.co,
        outprojepsg=cmdargs.outprojepsg, outprojwktfile=cmdargs.outprojwktfile,
        outXres=cmdargs.xres, outYres=cmdargs.yres,
        resamplemethod=cmdargs.resample)

    if cmdargs.monitorjson is not None:
        with open(cmdargs.monitorjson, 'w') as f:
            json.dump(monitors.reportAsDict(), f, indent=2)


def doMosaic(filelist, outfile, *, numthreads=DFLT_NUMTHREADS,
        blocksize=DFLT_BLOCKSIZE, driver=DFLT_DRIVER, nullval=None,
        creationoptions=None, outprojepsg=None,
        outprojwktfile=None, outprojwkt=None, outXres=None,
        outYres=None, resamplemethod=DFLT_RESAMPLEMETHOD):
    """
    From the given list of input raster files, create a single mosaic
    output raster.

    Parameters
    ----------
    filelist : List of str
        List of filenames of input raster files
    outfile : str
        Name of output raster file
    numthreads : int
        Number of threads to use for reading input. These are in addition
        to the main thread which manages everything else, including
        writing the output
    blocksize : int
        Number of pixels in each square block of input (blocksize x blocksize)
    driver : str
        GDAL short name of format driver to use for output
    creationoptions : List of str
        List of 'NAME=VALUE' strings, giving GDAL creation options to go
        with the selected format driver. If this is None, then some
        sensible defaults are supplied for some known drivers.
    nullval : int
        Value to use as "no data" in input and output rasters. Default will
        be taken from the input files, but this can be used to over-ride
        that.
    outprojepsg : int
        EPSG number of projection for output file. Default projection
        matches the input files
    outprojwktfile : str
        Name of text file containing WKT string for output projection
    outprojwkt : str
        WKT string for output projection
    outXres, outYres : float
        Desired output pxel size (X and Y directions). Default pixel
        size matches the input files
    resamplemethod : str
        GDAL name of resampling method to use, if any resampling is required


    Returns
    -------
    monitorInfo : Monitoring
        An object of various bits of monitoring information, mainly
        useful in development and testing. Most importantly, timing
        information of various steps in the mosaicing process.

    """
    monitors = monitoring.Monitoring()
    monitors.setParam('numthreads', numthreads)
    monitors.setParam('blocksize', blocksize)
    monitors.setParam('cpucount', cpu_count())

    # Work out what we are going to do
    monitors.setParam('numinfiles', len(filelist))
    with monitors.timestamps.ctx("imginfodict"):
        imgInfoDict = makeImgInfoDict(filelist)

    with monitors.timestamps.ctx("projection"):
        (filelist, tmpdir) = reproj.handleProjections(filelist,
            imgInfoDict, outprojepsg, outprojwktfile, outprojwkt, outXres,
            outYres, resamplemethod, nullval)

    if nullval is None:
        nullval = imgInfoDict[filelist[0]].nullVal

    with monitors.timestamps.ctx("analysis"):
        outImgInfo = makeOutputGrid(filelist, imgInfoDict, nullval)
        blockList = makeOutputBlockList(outImgInfo, blocksize)

        (blockListWithInputs, filesForBlock) = (
            findInputsPerBlock(blockList, outImgInfo.transform, filelist,
            imgInfoDict))
        blockReadingList = makeBlockReadingList(blockListWithInputs)
        blocksPerThread = divideBlocksByThread(blockReadingList, numthreads)

    blockQ = queue.Queue()
    poolClass = futures.ThreadPoolExecutor
    numBands = imgInfoDict[filelist[0]].numBands

    # Now do it all, using concurrent threads to read blocks into a queue
    (outDs, overviewLevels) = openOutfile(outfile, driver, outImgInfo,
        creationoptions)
    statsAccumList = []
    with monitors.timestamps.ctx("domosaic"):
        for bandNum in range(1, numBands + 1):
            statsAccum = StatsAccumulator(nullval)
            statsAccumList.append(statsAccum)
            with poolClass(max_workers=numthreads) as threadPool:
                workerList = []
                for i in range(numthreads):
                    blocksToRead = blocksPerThread[i]
                    worker = threadPool.submit(readFunc, blocksToRead, blockQ,
                            bandNum, outImgInfo.nullVal)
                    workerList.append(worker)

                writeFunc(blockQ, outDs, outImgInfo, bandNum, blockList,
                    filesForBlock, workerList, overviewLevels, statsAccum,
                    monitors)

    if tmpdir is not None:
        shutil.rmtree(tmpdir)

    return monitors


def readFunc(blocksToRead, blockQ, bandNum, outNullVal):
    """
    This function is run by all the read workers, each with its own list
    of blocks to read.

    Parameters
    ----------
    blocksToRead : List of BlockSpecWithInputs objects
    blockQ : queue.Queue
        As each block is read, it is sent to the writer via this Queue
    bandNum : int
        GDAL band number of band to read (i.e. first band is 1)
    outNullVal : int
        Output null value. Used as the fill value for incomplete
        blocks (i.e. when the output block falls partly off the edge of
        the input raster)

    """
    blocksPerInfile = structures.BlocksByInfile()
    for blockInfo in blocksToRead:
        blocksPerInfile.blockToDo(blockInfo.filename, blockInfo.outblock)
    gdalObjCache = structures.GdalObjCache()

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
        outArr[rowoffset:(rowoffset + ysize1),
               coloffset:(coloffset + xsize1)] = arr

        # Put the full bloc into the blockQ, along with the associated
        # block information
        blockQ.put((blockInfo, outArr))

        # If this input file is now done, we can close it.
        blocksPerInfile.blockDone(filename, blockInfo.outblock)
        if blocksPerInfile.countRemaining(filename) == 0:
            gdalObjCache.closeBand(filename, bandNum)
        i += 1


def writeFunc(blockQ, outDs, outImgInfo, bandNum, blockList, filesForBlock,
        workerList, overviewLevels, statsAccum, monitors):
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

    Parameters
    ----------
    blockQ : queue.Queue
        Blocks of data are taken from this Queue
    outDs : gdal.Dataset
        Open Dataset of output file
    outImgInfo : ImageInfo
        All info about the output file
    bandNum : int
        GDAL band number (i.e. starts at 1) for band to write to outDs
    blockList : List of BlockSpec
        List of the blocks in the output grid. Blocks of data are written
        in this order.
    filesForBlock : dict
        Key is a BlockSpec, value is a list of filenames which contribute
        data to that block (i.e. intersect with it)
    workerList : List of futures.Future
        List of worker threads, so we can continually check them for
        exceptions
    overviewLevels: List of int
        List of overview levels (as given to BuildOverviews()
    statsAccum: StatsAccumulator
        Object to manage incremental accumulators for single-pass statistics
    monitors : Monitoring
        A Monitoring object, mainly used to accumulate timing info

    """
    band = outDs.GetRasterBand(bandNum)
    band.SetNoDataValue(outImgInfo.nullVal)

    # Cache of blocks available to write
    blockCache = structures.BlockCache()

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
        outArr = None

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

        if outArr is not None:
            # We actually wrote this block, so do pyramids and stats
            writeBlockPyramids(band, outArr, overviewLevels, outblock.left,
                outblock.top)
            statsAccum.doStatsAccum(outArr)

        checkReaderExceptions(workerList)

        monitors.minMaxBlockCacheSize.update(len(blockCache))
        monitors.minMaxBlockQueueSize.update(blockQ.qsize())

    (minval, maxval, meanval, stddev, count) = statsAccum.finalStats()
    if count > 0:
        band.SetMetadataItem("STATISTICS_MINIMUM", str(minval))
        band.SetMetadataItem("STATISTICS_MAXIMUM", str(maxval))
        band.SetMetadataItem("STATISTICS_MEAN", str(meanval))
        band.SetMetadataItem("STATISTICS_STDDEV", str(stddev))


def checkReaderExceptions(workerList):
    """
    Check the read workers, in case one has raised an exception. The
    elements of workerList are futures.Future objects. If a worker
    has ended by raising an exception, then re-raise it.
    """
    for worker in workerList:
        if worker.done():
            e = worker.exception(timeout=0)
            if e is not None:
                raise e


def allWorkersDone(workerList):
    """
    Return True if all workers are done
    """
    allDone = True
    for worker in workerList:
        if not worker.done():
            allDone = False
    return allDone


def makeFilelist(infilelist):
    """
    Read the text file listing all input files, and return a
    list of the filenames
    """
    filelist = [line.strip() for line in open(infilelist)]
    return filelist


def makeOutputGrid(filelist, imgInfoDict, nullval):
    """
    Work out the extent of the whole mosaic. Return an ImageInfo
    object of the output grid.
    """
    infoList = [imgInfoDict[fn] for fn in filelist]
    boundsArray = numpy.array([(i.xMin, i.xMax, i.yMin, i.yMax)
        for i in infoList])
    xMin = boundsArray[:, 0].min()
    xMax = boundsArray[:, 1].max()
    yMin = boundsArray[:, 2].min()
    yMax = boundsArray[:, 3].max()

    firstImgInfo = imgInfoDict[filelist[0]]
    outImgInfo = structures.ImageInfo(None)
    outImgInfo.projection = firstImgInfo.projection
    (xRes, yRes) = (firstImgInfo.xRes, firstImgInfo.yRes)
    outImgInfo.ncols = int(round(((xMax - xMin) / xRes)))
    outImgInfo.nrows = int(round(((yMax - yMin) / yRes)))
    outImgInfo.transform = (xMin, xRes, 0.0, yMax, 0.0, -yRes)
    outImgInfo.dataType = firstImgInfo.dataType
    outImgInfo.numBands = firstImgInfo.numBands
    outImgInfo.nullVal = firstImgInfo.nullVal
    # Get thematic/athematic from first input
    outImgInfo.layerType = [lyrType for lyrType in firstImgInfo.layerType]
    if nullval is not None:
        outImgInfo.nullVal = nullval
    return outImgInfo


def makeOutputBlockList(outImgInfo, blocksize):
    """
    Given a pixel grid of the whole extent, divide it up into blocks.
    Return a list of BlockSpec objects.
    """
    # Divide this up into blocks
    (nrows, ncols) = (outImgInfo.nrows, outImgInfo.ncols)
    blockList = []
    top = 0
    while top < nrows:
        ysize = min(blocksize, (nrows - top))
        # If what remains after this block is less than 25% of blocksize,
        # then expand this block to the edge, to avoid very small blocks
        if (nrows - (top + ysize)) < (blocksize // 4):
            ysize = nrows - top

        left = 0
        while left < ncols:
            xsize = min(blocksize, (ncols - left))
            # Similarly avoid 'too narrow' blocks at the right-hand edge
            if (ncols - (left + xsize)) < (blocksize // 4):
                xsize = ncols - left

            block = structures.BlockSpec(top, left, xsize, ysize)
            blockList.append(block)
            left += xsize

        top += ysize
    return blockList


def makeImgInfoDict(filelist):
    """
    Create ImageInfo objects for all the given input files.
    Store these in a dictionary, keyed by their filenames.
    """
    imgInfoDict = {}
    for filename in filelist:
        imgInfoDict[filename] = structures.ImageInfo(filename)
    return imgInfoDict


def findInputsPerBlock(blockList, outGeoTransform, filelist, imgInfoDict):
    """
    For every block, work out which input files intersect with it,
    and the bounds of that block, in each file's pixel coordinate system.
    """
    blockListWithInputs = []
    filesForBlock = {}
    for block in blockList:
        blockWithInputs = structures.BlockSpecWithInputs(block)

        for filename in filelist:
            imginfo = imgInfoDict[filename]
            (fileLeft, fileTop, fileRight, fileBottom) = (
                block.transformToFilePixelCoords(outGeoTransform, imginfo))
            intersects = ((fileRight + 1) >= 0 and (fileBottom + 1) >= 0 and
                fileLeft <= imginfo.ncols and fileTop <= imginfo.nrows)

            if intersects:
                xs = fileRight - fileLeft
                ys = fileBottom - fileTop
                inblock = structures.BlockSpec(fileTop, fileLeft, xs, ys)
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
            blockInfo = structures.BlockReadingSpec(outblock, filename,
                    inblock)
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


def openOutfile(outfile, driver, outImgInfo, creationoptions):
    """
    Open the output file.

    Parameters
    ----------
    outfile : str
    driver : str
    outImgInfo : ImageInfo
    creationoptions : List of str

    Returns
    -------
    outDs : gdal.Dataset

    """
    if outfile is None:
        raise ValueError("Must specify output file")

    (nrows, ncols) = (outImgInfo.nrows, outImgInfo.ncols)
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
    ds.SetGeoTransform(outImgInfo.transform)
    ds.SetProjection(outImgInfo.projection)
    for i in range(numBands):
        layerType = outImgInfo.layerType[i]
        if layerType is not None:
            band = ds.GetRasterBand(i + 1)
            band.SetMetadataItem('LAYER_TYPE', layerType)

    # Work out a list of overview levels, starting with 4, until the raster
    # size (in largest direction) is smaller then finalOutSize.
    outSize = max(ds.RasterXSize, ds.RasterYSize)
    finalOutSize = 1024
    overviewLevels = []
    i = 2
    while ((outSize // (2 ** i)) >= finalOutSize):
        overviewLevels.append(2 ** i)
        i += 1

    # Create the empty pyramid layers on the dataset. Currently only
    # support NEAREST
    aggType = "NEAREST"
    ds.BuildOverviews(aggType, overviewLevels)

    return (ds, overviewLevels)


def mergeInputs(allInputsForBlock, outNullVal):
    """
    Given a list of input arrays, merge to produce the final
    output array. Ordering is important, the last non-null
    value is the one used.

    Parameters
    ----------
    allInputsForBlock : List of numpy.ndarray (nrows, ncols)
        List of blocks of raster data from input files.
    outNullVal : int
        Pixels with this value will be be excluded from contributing
        to the output array

    Returns
    -------
    outArr : numpy.ndarray (nrows, ncols)
        Final output block of pixels

    """
    numInputs = len(allInputsForBlock)
    outArr = allInputsForBlock[0]
    for i in range(1, numInputs):
        arr = allInputsForBlock[i]
        nonNull = (arr != outNullVal)
        outArr[nonNull] = arr[nonNull]
    return outArr


def doStats(outDs):
    """
    Calculate basic statistics on all bands of the output file
    """
    allNullErrorMsg = ("Failed to compute statistics, " +
        "no valid pixels found in sampling.")

    usingExceptions = gdal.GetUseExceptions()
    gdal.UseExceptions()

    approx_ok = True
    try:
        for i in range(1, outDs.RasterCount + 1):
            band = outDs.GetRasterBand(i)
            (minval, maxval, meanval, stddevval) = band.ComputeStatistics(
                approx_ok)
            band.SetMetadataItem("STATISTICS_MINIMUM", repr(minval))
            band.SetMetadataItem("STATISTICS_MAXIMUM", repr(maxval))
            band.SetMetadataItem("STATISTICS_MEAN", repr(meanval))
            band.SetMetadataItem("STATISTICS_STDDEV", repr(stddevval))
    except RuntimeError as e:
        if not str(e).endswith(allNullErrorMsg):
            raise e
    finally:
        if not usingExceptions:
            gdal.DontUseExceptions()


def writeBlockPyramids(band, arr, overviewLevels, xOff, yOff):
    """
    Calculate and write out the pyramid layers for one band of the block
    given as arr. Uses nearest neighbour sampling to sub-sample the array.

    """
    nOverviews = len(overviewLevels)

    for j in range(nOverviews):
        band_ov = band.GetOverview(j)
        lvl = overviewLevels[j]
        # Offset from top-left edge
        o = lvl // 2
        # Sub-sample by taking every lvl-th pixel in each direction
        arr_sub = arr[o::lvl, o::lvl]
        # The xOff/yOff of the block within the sub-sampled raster
        xOff_sub = xOff // lvl
        yOff_sub = yOff // lvl
        # The actual number of rows and cols to write, ensuring we
        # do not go off the edges
        nc = band_ov.XSize - xOff_sub
        nr = band_ov.YSize - yOff_sub
        arr_sub = arr_sub[:nr, :nc]
        band_ov.WriteArray(arr_sub, xOff_sub, yOff_sub)


class StatsAccumulator:
    """
    Accumulator for statistics for a single band.
    """
    def __init__(self, nullval):
        self.nullval = nullval
        self.minval = None
        self.maxval = None
        self.sum = 0
        self.ssq = 0
        self.count = 0

    def doStatsAccum(self, arr):
        """
        Accumulate basic stats for the given array
        """
        if self.nullval is None:
            values = arr.flatten()
        elif numpy.isnan(self.nullval):
            values = arr[~numpy.isnan(arr)]
        else:
            values = arr[arr != self.nullval]
        if len(values) > 0:
            self.sum += values.astype(numpy.float64).sum()
            self.ssq += (values.astype(numpy.float64)**2).sum()
            self.count += values.size
            minval = values.min()
            if self.minval is None or minval < self.minval:
                self.minval = minval
            maxval = values.max()
            if self.maxval is None or maxval > self.maxval:
                self.maxval = maxval

    def finalStats(self):
        """
        Return the final values of the four basic statistics
        (minval, maxval, mean, stddev)
        """
        meanval = None
        stddev = None
        if self.count > 0:
            meanval = self.sum / self.count
            variance = self.ssq / self.count - meanval ** 2
            stddev = 0.0
            # In case some rounding error made variance negative
            if variance >= 0:
                stddev = numpy.sqrt(variance)

        return (self.minval, self.maxval, meanval, stddev, self.count)
