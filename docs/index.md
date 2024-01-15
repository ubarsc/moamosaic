# MoaMosaic
## Description
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

## Command Line
Moamosaic can be run from the command line. The command usage is described
[here](cmdline.md)

## Python API
The moamosaic package is written in Python, and can be called directly 
from Python. The API documentation is available [here](api)

## Performance
An analysis of performance timings is available [here](performance.md)

## Source Code
The source for moamosaic is available at 
[https://github.com/ubarsc/moamosaic](https://github.com/ubarsc/moamosaic)

## Installation
Moamosaic can be downloaded as a tar.gz file from 
[https://github.com/ubarsc/moamosaic/releases](https://github.com/ubarsc/moamosaic/releases)

Release notes for different versions are available [here](releasenotes.md)

The tar.gz file can then be installed using pip, for example

```bash
pip install moamosaic-1.0.0.tar.gz
```

It does require a sufficiently modern version of setuptools to install. Version
69 works fine, but version 59 does not. I am unsure of exactly the minimum
requirement.

