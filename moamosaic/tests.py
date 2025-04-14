"""
Routine tests of the package
"""
import os
import unittest

import numpy
from osgeo import gdal, gdal_array, osr

from moamosaic import mosaic

gdal.UseExceptions()


DFLT_DRIVER = 'KEA'


def makeRaster(filename, imgArr, transform, projection, nullval):
    """
    Create a raster file from the given numpy array.

    The transform is a GDAL-style GeoTransform tuple, and the projection
    is a WKT string.
    """
    drvr = gdal.GetDriverByName(DFLT_DRIVER)
    (nrows, ncols) = imgArr.shape
    gdalType = gdal_array.NumericTypeCodeToGDALTypeCode(imgArr.dtype)

    ds = drvr.Create(filename, ncols, nrows, 1, gdalType)
    ds.SetGeoTransform(transform)
    ds.SetProjection(projection)
    band = ds.GetRasterBand(1)
    band.WriteArray(imgArr)
    band.SetNoDataValue(nullval)
    del band
    del ds


def readRaster(filename):
    """
    Read the given raster and return a numpy array
    """
    ds = gdal.Open(filename)
    band = ds.GetRasterBand(1)
    a = band.ReadAsArray()
    return a


def readStats(filename):
    """
    Return (minval, maxval, meanval, stddev) from the stored statistics
    on band 1 of the given file
    """
    ds = gdal.Open(filename)
    band = ds.GetRasterBand(1)
    minval = eval(band.GetMetadataItem("STATISTICS_MINIMUM"))
    maxval = eval(band.GetMetadataItem("STATISTICS_MAXIMUM"))
    meanval = eval(band.GetMetadataItem("STATISTICS_MEAN"))
    stddev = eval(band.GetMetadataItem("STATISTICS_STDDEV"))
    return (minval, maxval, meanval, stddev)


class Fulltest(unittest.TestCase):
    """
    Run a basic test of the whole mosaic operation. Generates
    two input files, mosaics them, and checks that the output is
    what we expect.
    """
    def test_sameProjection(self):
        # Set up a pair of side-by-side rasters
        transform1 = [300000.0, 10.0, 0.0, 7000000.0, 0.0, -10.0]
        sr = osr.SpatialReference()
        sr.ImportFromEPSG(32756)
        projection = sr.ExportToWkt()

        (nrows, ncols) = (5000, 5000)
        row = numpy.arange(ncols, dtype=numpy.uint16)
        imgArr = numpy.tile(row, (nrows, 1))
        nullval = 0

        file1 = 'file1.kea'
        file2 = 'file2.kea'
        makeRaster(file1, imgArr, transform1, projection, nullval)
        transform2 = transform1.copy()
        # Shift to the right, leaving a 2-pixel overlap
        transform2[0] += (ncols - 2) * transform1[1]
        makeRaster(file2, imgArr, transform2, projection, nullval)

        # Set up what the true mosaiced array should be
        trueMosaicImg = numpy.zeros((nrows, 2 * ncols - 2), dtype=imgArr.dtype)
        trueMosaicImg[:, :ncols] = imgArr
        # Note that the first column is null, and so should not over-write
        # the second-last column of the first array
        trueMosaicImg[:, ncols - 1:] = imgArr[:, 1:]

        outfile = 'outfile.kea'
        mosaic.doMosaic([file1, file2], outfile, driver=DFLT_DRIVER)

        mosaicImg = readRaster(outfile)

        self.assertTrue((mosaicImg == trueMosaicImg).all())

        self.checkBasicStats(outfile, trueMosaicImg, nullval)

        for fn in [file1, file2, outfile]:
            if os.path.exists(fn):
                os.remove(fn)

    def checkBasicStats(self, outfile, trueMosaicImg, nullval):
        """
        Check that the basic statistics are calculated correctly
        """
        nonnullMask = (trueMosaicImg != nullval)
        nonnullVals = trueMosaicImg[nonnullMask]
        trueMinval = int(nonnullVals.min())
        trueMaxval = int(nonnullVals.max())
        trueMeanval = float(nonnullVals.mean())
        trueStddev = float(nonnullVals.std())
        (minval, maxval, meanval, stddev) = readStats(outfile)

        self.assertAlmostEqual(trueMinval, minval, msg="Minval mis-match")
        self.assertAlmostEqual(trueMaxval, maxval, msg="Maxval mis-match")
        self.assertAlmostEqual(trueMeanval, meanval, msg="Meanval mis-match")
        self.assertAlmostEqual(trueStddev, stddev, msg="Stddev mis-match")


def mainCmd():
    unittest.main(module='moamosaic.tests', exit=False)


if __name__ == "__main__":
    mainCmd()
