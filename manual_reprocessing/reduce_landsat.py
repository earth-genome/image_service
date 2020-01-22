"""Routines to process Landsat Surface Reflectance tiles. 

Ordered from https://earthexplorer.usgs.gov, scenes are delivered as tar
files containing each band as a separte TIF, with filenames of form

LC08_L1TP_037034_20170309_20180125_01_T1_sr_band2.tif

For Landsat8, R-G-B images are built from bands 4-3-2. For Landsat5, R-G-B 
are built from bands 3-2-1.  

Usage: Untar everything into a folder. Multiple scenes are fine, as the 
program will untangle them. The only restrictions are that all band files
for a scene must share a common prefix, with filename of form prefixband?.tif,
and be Int16 or Uint16 in type.

Then: 

$ python reduce_landsat.py 4 3 2 -g footprint.geojson -d image_dir -wp 3500

For help:
$ python reduce_landsat.py -h

The band ordering 4 3 2 or 3 2 1 is required. The scenes will be
cropped to the footprint, if given. The image_dir defaults to pwd if not
specified. 

The -wp flag sets the white point of the output images. The Landsat
histograms are confined to a small part of the possible 16-bit
range. In light testing, 3500 (max 2**16 - 1 = 65535) seems reasonable
default white point for a linear rescaling of the histogram. Adjust
this if the output image is overly dark or overly saturated.

The routine outputs one geotiff for each processed scene.

"""

import argparse
import glob
import json
import os
import subprocess
import sys

import _env
from geobox import geobox
from geobox import geojsonio

WHITE_PT = 3500
BIT_DEPTH = 16

def build_rgb(prefix, files, bounds, **kwargs):
    """Build an RGB image from individual color bands.

    Arguments: 
        prefix: common filename prefix for image bands
        files: list of R, G, B, geotiffs 
        bounds: lat/lon coordinates, ordered [minx, miny, maxx, maxy], or []
        **kwargs, including:
            white_point: The 16-bit image value that should be reset to white
            bit_depth: bit-depth for output image, either 8 or 16

    Output: A geotiff

    Returns: Geotiff filename
    """
    vrtfile = combine_bands(prefix, files)
    outfile = crop_and_rescale(vrtfile, bounds, **kwargs)
    os.remove(vrtfile)
    return outfile

def combine_bands(prefix, files):
    """Assemble R-G-B image bands into GDAL .vrt file."""
    combined = prefix + '.vrt'
    commands = ['gdalbuildvrt', '-separate', combined, *files]
    subprocess.call(commands)
    return combined

def crop_and_rescale(vrtfile, bounds, **kwargs):
    """Crop virtual image to bounds and linearly rescale the histogram.

    Outputs a geotiff; returns the filename.
    """
    tiffile = vrtfile.split('.vrt')[0] + '.tif'
    commands = [
        'gdal_translate', vrtfile, tiffile,
        '-co', 'COMPRESS=LZW',
        '-colorinterp', 'red,green,blue'
    ]
    if bounds:
        gdal_bounds = [str(bounds[n]) for n in (0, 3, 2, 1)]
        commands += ['-projwin_srs', 'EPSG:4326', '-projwin', *gdal_bounds]
    if kwargs['bit_depth'] == 8:
        commands += [
            '-ot', 'Byte',
            '-scale', '0', str(kwargs['white_point']), '0', '255'
        ]
    elif kwargs['bit_depth'] == 16:
        commands += [
            '-ot', 'UInt16',
            '-scale', '0', str(kwargs['white_point']), '0', '65535'
        ]
    else:
        raise ValueError('Invalid output bit depth: {}.'.format(
            kwargs['bit_depth']))
    subprocess.call(commands)
    return tiffile

# Image file handling

def partition(filenames, bandlist):
    """Partition input filenames by common prefixes and filter by bandlist.

    Returns: dict of prefixes and filenames
    """
    prefixes = set([f.split('band')[0] for f in filenames])
    partition = {p:[f for f in filenames if p in f] for p in prefixes}
    filtered = {p:filter_bands(p, files, bandlist) for p,files 
                    in partition.items()}
    return filtered

def filter_bands(prefix, files, bandlist):
    """Select from list of files those bands numbered in bandlist.

    Returns: List of filenames
    """
    bandfiles = [prefix + 'band{}.tif'.format(b) for b in bandlist]
    for f in bandfiles:
        if f not in files:
            raise FileNotFoundError('Missing color bands for {}'.format(prefix))
    return bandfiles


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process Landsat surface reflectance tiles. Routine ' +
            'will attempt to process all files in pwd with filenames ' +
            'of form *band?.tif.'
    )
    parser.add_argument(
        'bandlist',
        type=str,
        nargs='+',
        help='Band numbers to assemble in R-G-B order. E.g. 4 3 2 ' +
            'for Landsat8 or 3 2 1 for Landsat5.'
    )
    parser.add_argument(
        '-g', '--geojson',
        type=str,
        help='Geojson file expressing area of interest for optional crop.'
    )
    parser.add_argument(
        '-wp', '--white_point',
        type=int,
        default=WHITE_PT,
        help='16-bit image value to be reset to white. Default: {}'.format(
            WHITE_PT)
    )
    parser.add_argument(
        '-d', '--image_dir',
        type=str,
        default='',
        help='Directory containing image band files. Defaults to pwd.'
    )
    parser.add_argument(
        '-b', '--bit_depth',
        type=int,
        default=BIT_DEPTH,
        help='Bit-depth of output image, either 8 or 16. Default: {}'.format(
            BIT_DEPTH)
    )
    args = parser.parse_args()

    geoms = geojsonio.load_geometries(args.geojson) if args.geojson else []
    bounds = geobox.bbox_from_geometries(geoms).bounds if geoms else []

    image_files = glob.glob(os.path.join(args.image_dir, '*band?.tif'))
    grouped = partition(image_files, args.bandlist)
    for prefix, files in grouped.items():
        try:
            build_rgb(prefix, files, bounds, **vars(args))
        except FileNotFoundError as e:
            print('{}\nContinuing...'.format(repr(e)))


    
