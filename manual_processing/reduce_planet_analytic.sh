#!/bin/bash

# Routines to process Planet Analytic tiles into a single color-corrected Tiff
#
# Input: Neighboring (or overlapping) 4-band, 16-bit GeoTiffs
# Output: An LZW-compressed 3-band, 8-bit GeoTiff to pwd, named after the
#     first 12 characters of the first input GeoTiff (*-Raw.tif),
#     along with an LZW-compressed color-corrected 3-band Tiff (*-cc.tif)

# Usage:
# $ bash reduce_planet_analytic.sh tile1.tif tile2.tif tile3.tif
# The routine also accepts wildcards:
# $ bash reduce_planet_analytic.sh *.tif

base=$(basename $1)
base=${base%.*}
outfile=$(echo $base | head -c12)

# merge 
gdal_merge.py -of GTiff -o $outfile-Merged.tif $@

# reduce Planet Analytic 4-band to RGB
gdal_translate -b 3 -b 2 -b 1 $outfile-Merged.tif $outfile-RGB.tif
rm $outfile-Merged.tif

# reduce 16bit to 8bit
gdal_translate -ot Byte -scale 0 65535 0 255 $outfile-RGB.tif $outfile-Raw.tif
rm $outfile-RGB.tif*

# color correct
python3 reduce_planet_analytic.py $outfile-Raw.tif

# compress
gdal_translate -co COMPRESS=LZW $outfile-Raw.tif $outfile-RawLZW.tif
gdal_translate -co COMPRESS=LZW $outfile-cc.tif $outfile-ccLZW.tif

rm $outfile-Raw.tif
rm $outfile-cc.tif
rm $outfile*aux.xml

# TODO:  allow crop to bbox in WGS84/NAD83 UTM coords
# gdal_translate -projwin 395344.353321 7950169.12085 407866.707565 7937850.93542 -of GTiff /full/path/to/input.tif /full/path/to/output.tif
# See http://www.synnatschke.de/geo-tools/coordinate-converter.php for
# WGS84 to lat/lon conversions (web form)#!/bin/bash

