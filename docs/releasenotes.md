# Release Notes

## Version 1.0.2 (2026-02-25)
* Fix bug in intersection calculation ([#10](https://github.com/ubarsc/moamosaic/pull/10))
* Add ``--minoverviewsize`` option ([#13](https://github.com/ubarsc/moamosaic/pull/13))
* Prevent GDAL3 axis swaps when reprojecting ([#17](https://github.com/ubarsc/moamosaic/pull/17))

## Version 1.0.1 (2024-01-14)
* Basic stats and pyramid layers (overviews) on output file are now
  handled incrementally, block by block, which saves making two extra
  passes through the data.

## Version 1.0.0 (2024-01-14)
* Initial release
