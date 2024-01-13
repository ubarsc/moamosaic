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

The main module in this package is `moamosaic.mosaic`. 

"""

__version__ = "1.0.0"
