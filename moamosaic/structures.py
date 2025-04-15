"""
All major data structures for moamosaic
"""
from osgeo import gdal


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

    @property
    def xMin(self):
        return self.transform[0]

    @property
    def xMax(self):
        return self.transform[0] + self.ncols * self.transform[1]

    @property
    def yMax(self):
        return self.transform[3]

    @property
    def yMin(self):
        return self.transform[3] + self.nrows * self.transform[5]

    @property
    def xRes(self):
        return self.transform[1]

    @property
    def yRes(self):
        return abs(self.transform[5])


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
        as those of the imginfo object (i.e. in the same projection).

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
