"""
Routines for dealing with reprojection of inputs
"""
import os
import tempfile
import math

from osgeo import gdal
from osgeo import osr

from . import structures


def handleProjections(filelist, imgInfoDict, outprojepsg, outprojwktfile,
        outXres, outYres):
    """
    Main routine for handling issues around projections, etc.

    Returns
    """
    tmpdir = None
    if reprojectionRequested(outprojepsg, outprojwktfile):
        returnTuple = makeReprojVRTs(filelist, imgInfoDict,
                outprojepsg, outprojwktfile)
    else:
        # Raise an exception if input projections don't match
        checkInputProjections(imgInfoDict)
        returnTuple = (filelist, tmpdir)

    return returnTuple


def reprojectionRequested(outprojepsg, outprojwktfile):
    """
    Check whether an output projection has been requested,
    in any of the possible forms.
    """
    reprojReq = (outprojepsg is not None or outprojwktfile is not None)
    return reprojReq


def checkInputProjections(imgInfoDict):
    """
    Check the projections of all the input files. If they are not
    the same, then raise an exception.

    Also check pixel sizes and grid alignments.
    """
    firstSRS = firstFilename = firstTransform = None
    for (filename, imginfo) in imgInfoDict.items():
        if firstSRS is None:
            firstSRS = osr.SpatialReference(wkt=imginfo.projection)
            firstFilename = filename
            firstTransform = imginfo.transform

        srs = osr.SpatialReference(wkt=imginfo.projection)
        if not srs.IsSame(firstSRS):
            msg = ("Projection mis-match for files {} and {}. " +
                "Specify the output projection").format(
                firstFilename, filename)
            raise MoaProjectionError(msg)

        transform = imginfo.transform
        # Check pixel sizes
        if transform[1] != firstTransform[1]:
            msg = "X pixel size mis-match for files {} and {}".format(
                firstFilename, filename)
            raise MoaProjectionError(msg)
        if transform[5] != firstTransform[5]:
            msg = "Y pixel size mis-match for files {} and {}".format(
                firstFilename, filename)
            raise MoaProjectionError(msg)

        # Check pixel grid alignment
        if not isAligned(transform[0], firstTransform[0], transform[1]):
            msg = "X grid mis-alignment for files {} and {}".format(
                firstFilename, filename)
            raise MoaProjectionError(msg)
        if not isAligned(transform[3], firstTransform[3], transform[5]):
            msg = "Y grid mis-alignment for files {} and {}".format(
                firstFilename, filename)
            raise MoaProjectionError(msg)


def isAligned(x1, x2, res):
    """
    Check if the two coordinate values x1 and x2 are aligned, i.e.
    they differ by an integer multiple of the resolution.
    """
    diff = abs(x1 - x2)
    factor = diff / res
    intFactor = round(factor)
    remainder = abs(intFactor - factor)
    factorIsInt = (remainder < 0.0001)
    return factorIsInt


def makeReprojVRTs(filelist, imgInfoDict, outprojepsg, outprojwktfile,
        outprojwkt, outXres, outYres, resampleMethod, nullval):
    """
    Reproject the input files to the requested output projection, using
    temporary VRT files.

    Parameters
    ----------
      filelist : List of str
        The input filenames to be reprojected
      imgInfoDict : dict
        Keyed by filename, value is ImageInfo object. This dictionary
        is updated to included entries for all the new VRT files
      outptojepsg : int
        EPSG number of requested output projection. None if not required.
      outprojwktfile : str
        Name of file containing WKT of requested projection. None if not
        required
      outprojwkt : str
        WKT string of requested projection, or None.
      outXres, outYres : float
        Pixel size for desired output pixels
      resampleMethod : str
        GDAL resample method string (nearest, cubic, etc.)
      nullval : float
        Null value to use for input and output images, or None.

    Returns
    -------
      newFilelist : List of str
        Names of VRT files
      tmpdir : str
        Name of temporary directory containing all VRTs

    """
    tmpdir = tempfile.mkdtemp()

    if outprojwktfile is not None:
        outprojwkt = open(outprojwktfile).read()

    outSrs = osr.SpatialReference()
    if outprojepsg is not None:
        outSrs.ImportFromEPSG(outprojepsg)
    elif outprojwkt is not None:
        outSrs.ImportFromWkt(outprojwkt)

    firstSrs = osr.SpatialReference()
    firstImgInfo = imgInfoDict[filelist[0]]
    firstSrs.ImportFromWkt(firstImgInfo.projection)

    # Work out default resolution
    if outXres is None or outYres is None:
        if not outSrs.IsSameGeogGC(firstSrs):
            msg = ("Cannot deduce default pixel size, because output " +
                    "coordinate system is different to input")
            raise MoaProjectionError(msg)

        outXres = firstImgInfo.transform[1]
        outYres = firstImgInfo.transform[5]

    newFilelist = []
    for filename in filelist:
        vrtfilename = os.path.basename(filename) + ".vrt"
        vrtfilename = os.path.join(tmpdir, vrtfilename)

        # Work out the output bounds
        inSrs = osr.SpatialReference()
        inInfo = imgInfoDict[filename]
        inSrs.ImportFromWkt(inInfo.projection)
        tr = osr.CoordinateTransformation(inSrs, outSrs)
        (xMin, xMax, yMin, yMax) = reprojCorners(tr, inInfo)
        (xMin, xMax, yMin, yMax) = alignGrid(xMin, xMax, yMin, yMax,
                outXres, outYres)

        outBounds = (xMin, yMin, xMax, yMax)
        vrtOptions = gdal.BuildVRTOptions(xRes=outXres, yRes=outYres,
                srcNodata=nullval, VRTNodata=nullval, outputSRS=outSrs,
                outputBounds=outBounds)
        gdal.BuildVRT(vrtfilename, filename, vrtOptions)

        newFilelist.append(vrtfilename)
        imgInfoDict[vrtfilename] = structures.ImageInfo(vrtfilename)

    return (newFilelist, tmpdir)


def reprojCorners(tr, inInfo):
    """
    Reproject all corners of the given ImageInfo, using the given
    transformation.
    """
    (tlX, tlY, z) = tr.TransformPoint(inInfo.xMin, inInfo.yMax)
    (trX, trY, z) = tr.TransformPoint(inInfo.xMax, inInfo.yMax)
    (blX, blY, z) = tr.TransformPoint(inInfo.xMin, inInfo.yMin)
    (brX, brY, z) = tr.TransformPoint(inInfo.xMax, inInfo.yMin)
    xMin = min(tlX, trX, blX, brX)
    yMin = min(tlY, trY, blY, brY)
    xMax = max(tlX, trX, blX, brX)
    yMax = max(tlY, trY, blY, brY)
    return (xMin, xMax, yMin, yMax)


def alignGrid(xMin, xMax, yMin, yMax, outXres, outYres):
    """
    Align the grid extent so that the bounds are all multiples of
    the pixel size.
    """
    xMin = snapValue(xMin, outXres, False)
    xMax = snapValue(xMax, outXres, True)
    yMin = snapValue(yMin, outYres, False)
    yMax = snapValue(yMax, outYres, True)
    return (xMin, xMax, yMin, yMax)


def snapValue(val, res, ceil):
    """
    Snap the given value to a multiple of the resolution. If ceil
    is True, then snap to next highest value, otherwise snap to
    next lowest value.
    """
    n = val / res
    if ceil:
        n = math.ceil(n)
    else:
        n = math.floor(n)
    snappedVal = res * n
    return snappedVal


class MoaProjectionError(Exception):
    pass
