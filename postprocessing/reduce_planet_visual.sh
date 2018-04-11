#!/bin/bash

# Routines to process Planet Visual tiles into a single compressed GeoTiff.
#
# Input: Neighboring (or overlapping) 4-band, 8-bit GeoTiffs
# Output: A single LZW-compressed 3-band GeoTiff to pwd, named after the
#     first 12 characters of the first input GeoTiff.

# Usage:
# $ bash reduce_planet_visual.sh tile1.tif tile2.tif tile3.tif
# The routine also accepts wildcards:
# $ bash reduce_planet_visual.sh *.tif

base=$(basename $1)
base=${base%.*}
outfile=$(echo $base | head -c12)

# merge 
gdal_merge.py -of GTiff -o $outfile-Merged.tif $@

# reduce Planet Visual 4-band to RGB
gdal_translate -b 1 -b 2 -b 3 $outfile-Merged.tif $outfile-RGB.tif
rm $outfile-Merged.tif

# compress
gdal_translate -co COMPRESS=LZW $outfile-RGB.tif $outfile-ReducedLZW.tif
rm $outfile-RGB.tif*

# TODO:  allow crop to bbox in WGS84/NAD83 UTM coords
# gdal_translate -projwin 395344.353321 7950169.12085 407866.707565 7937850.93542 -of GTiff /full/path/to/input.tif /full/path/to/output.tif
# See http://www.synnatschke.de/geo-tools/coordinate-converter.php for
# WGS84 to lat/lon conversions (web form)
