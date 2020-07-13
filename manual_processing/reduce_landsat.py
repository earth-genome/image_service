"""Routines to process Landsat Surface Reflectance tiles. 

Ordered from https://earthexplorer.usgs.gov, scenes are delivered as tar
files containing each band as a separte TIF, with filenames of form

LC08_L1TP_037034_20170309_20180125_01_T1_sr_band2.tif

For Landsat8, R-G-B images are built from bands 4-3-2. For Landsat5, R-G-B 
are built from bands 3-2-1.  

Usage: Untar everything into a folder. Multiple scenes are fine, as
the program will untangle them. All band files for a scene must share
a common prefix, with filename of form prefixband?.tif or .TIF.

Then: 

$ python reduce_landsat.py 4 3 2 -g footprint.geojson -d image_dir -wp 3500

For help:
$ python reduce_landsat.py -h

The band ordering 4 3 2 or 3 2 1 is required. The scenes will be
cropped to the footprint, if given. The image_dir defaults to pwd if not
specified. 

The -wp flag sets the white point of the output images. The Landsat
histograms are confined to a small part of the possible 16-bit (or in
older Level-1 images, 8-bit) range. Values in the range 2000-4000
often work for 16-bit data. Adjust this if the output image is overly
dark or overly saturated.

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

DEFAULT_BIT_DEPTH = 16

def build_rgb(prefix, paths, bounds, **kwargs):
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
    vrtfile = combine_bands(prefix, paths)
    outfile = crop_and_rescale(vrtfile, bounds, **kwargs)
    os.remove(vrtfile)
    return outfile

def combine_bands(prefix, paths):
    """Assemble R-G-B image bands into GDAL .vrt file."""
    combined = prefix + '.vrt'
    commands = ['gdalbuildvrt', '-separate', combined, *paths]
    subprocess.call(commands)
    return combined

def crop_and_rescale(vrtfile, bounds, bit_depth=16, **kwargs):
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
    wp = kwargs.get('white_point')
    if bit_depth == 8:
        commands += ['-ot', 'Byte']
        if wp:
            commands += ['-scale', '0', str(wp), '0', '255']
    elif bit_depth == 16:
        commands += ['-ot', 'UInt16']
        if wp:
            commands += ['-scale', '0', str(wp), '0', '65535']
        else:
            raise ValueError(f'Invalid output bit depth: {bit_depth}.')
    subprocess.call(commands)
    return tiffile

def partition(paths, bands):
    """Partition input paths by common prefixes and filter by bands.

    Returns: dict of prefixes and paths
    """
    prefixes = set([p.split('band')[0] for p in paths])
    partition = {}
    for prefix in prefixes:
        partition.update({
            prefix: [p for b in bands for p in paths if prefix in p and
                         f'band{b}.' in p]})
    return partition

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process Landsat surface reflectance tiles. Routine ' +
            'will attempt to process all files in pwd with filenames ' +
            'of form *band?.tif or .TIF.'
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
        help='Image integer bit value to be reset to white.'
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
        default=16,
        help=('Bit-depth of output image, either 8 or 16. Default: 16. ' 
              'User is responsible for adjusting white point on change '
              'of bit depth.')
    )
    args = parser.parse_args()

    geoms = geojsonio.load_geometries(args.geojson) if args.geojson else []
    bounds = geobox.bbox_from_geometries(geoms).bounds if geoms else []
    
    base = os.path.join(args.image_dir, '*band?')
    paths = [glob.glob(base + ext) for ext in ['.tif', '.TIF']]
    paths = [p for sublist in paths for p in sublist]
        
    grouped = partition(paths, args.bandlist)
    for prefix, grouped_paths in grouped.items():
        build_rgb(prefix, grouped_paths, bounds, **vars(args))


    
