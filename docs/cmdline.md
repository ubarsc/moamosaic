# Command line script
The package is available from the command line with the `moamosaic` command. 
Its usage is described below.

```bash
usage: moamosaic [-h] [-i INFILELIST] [-n NUMTHREADS] [-b BLOCKSIZE]
                 [-d DRIVER] [-o OUTFILE] [--co CO] [--nullval NULLVAL]
                 [--monitorjson MONITORJSON] [--outprojepsg OUTPROJEPSG]
                 [--outprojwktfile OUTPROJWKTFILE] [--xres XRES] [--yres YRES]
                 [--resample RESAMPLE]

options:
  -h, --help            show this help message and exit
  -i INFILELIST, --infilelist INFILELIST
                        Text file list of input images
  -n NUMTHREADS, --numthreads NUMTHREADS
                        Number of read threads to use (default=4)
  -b BLOCKSIZE, --blocksize BLOCKSIZE
                        Blocksize in pixels (default=1024)
  -d DRIVER, --driver DRIVER
                        GDAL driver to use for output file (default=GTiff)
  -o OUTFILE, --outfile OUTFILE
                        Name of output raster
  --co CO               Specify a GDAL creation option (as 'NAME=VALUE'). Can
                        be given multiple times. There are sensible default
                        creation options for some drivers (GTiff,KEA,HFA), but
                        if this option is used, those are ignored.
  --nullval NULLVAL     Null value to use (default comes from input files)
  --monitorjson MONITORJSON
                        Output JSON file of monitoring info (optional)

Output Projection Options:
  Default projection matches the input files. The following options are used
  to specify something different. Use only one of --outprojepsg or
  --outprojwktfile

  --outprojepsg OUTPROJEPSG
                        EPSG number of desired output projection
  --outprojwktfile OUTPROJWKTFILE
                        Name of text file containing WKT of desired output
                        projection
  --xres XRES           Desired output X pixel size (default matches input)
  --yres YRES           Desired output Y pixel size (default matches input)
  --resample RESAMPLE   GDAL name of resampling method to use for reprojection
                        (default='near')

```
