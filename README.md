# MoaMosaic
## Introduction
A tool for using GDAL to make a mosaic of multiple input images. Its main advantage is
that it reads the input files block-by-block with multiple threads. On systems where there is
significant latency on reading, this allows the process to be significantly faster.

It should be emphasized that on a system with all inputs on local disk, and a sensible
operating system, this approach is unlikely to provide much benefit. It is difficult to
perform better than the operating system's own caching and disk management. The benefits are mainly
seen when the input data is stored somewhere like an S3 bucket.

This tool is similar in spirit to GDAL's own `gdal_merge.py`. The main differences
are:

  * Smaller memory usage, as reading is done in fixed-size blocks, rather than whole raster files
  * Automatic reprojection of inputs to a desired output projection
  * Multi-threaded reading of input files, so there is some reading ahead going on. Writing of output is always single-threaded, as most GDAL format drivers do not support multi-threaded writing
  * It has no equivalent of `gdal_merge.py`'s `-separate` option. This is not a stacking tool.

The package name is in honour of the Moa, a large flightless bird from 
New Zealand (sadly, now extinct).

## Full Documentation
Moamosaic can be used either from the command line, or via a Python API.

Full documentation of both is available on Github Pages 
https://ubarsc.github.io/moamosaic/
