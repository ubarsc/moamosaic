# Performance
## Introduction
Outline

Input data on S3
Multi-threaded reading, with different numbers of read workers (numthreads).

## Methods
Variability quite high, so run many repeats (N=40)
Unsure of possible cache effects, at any point in the process, so
each run on new data (Sentinel-2 imagery, different dates for 3x3 tile
mosaic).
Runs with different number of read worker threads
Runs on a 2 CPU machine. To Do - re-run on an 8 CPU machine
Comparison with gdal_merge.py, also multiple runs.

## Results
Plot of elapsed time for creating a mosaic. Median over all trials, and
the 90% confidence interval, from the 40 runs for each value of numthreads. 

![Plot of moamosaic runtime against number of threads](moamosaictiming.png)
