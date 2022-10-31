"""Routines to process Landsat Surface Reflectance tiles. 

Depends on: Local GDAL installation, called via subprocess module.

Ordered from https://earthexplorer.usgs.gov, scenes are delivered as tar
files containing each band as a separte TIF.

For Landsat8, R-G-B images are built from bands 4-3-2. For Landsat5, R-G-B 
are built from bands 3-2-1.  

Usage: Untar everything into a folder. Multiple scenes are fine, as
the program will untangle them. All band files for a scene must share
a common prefix, with filename of form prefix{band_sig}?.tif or .TIF. 
The variable band_sig is set with user flag and typically will be '_B' or
'band', depending on Landsat file name format. 

Then: 

$ python reduce_landsat.py 4 3 2 -g footprint.geojson -d image_dir -wp 3500

For help:
$ python reduce_landsat.py -h

The band ordering 4 3 2 or 3 2 1 is required. The scenes will be
cropped to the footprint, if given. The image_dir defaults to pwd if not
specified. 

The -wp, -bp flags designate input image pixel values to become white
and black points of the output.  The Landsat histograms are confined
to a small part of the possible 16-bit (or in older Level-1 images,
8-bit) range. Adjust these if the output image is overly dark or
overly saturated.

The routine outputs one geotiff for each processed scene.

"""

import argparse
import glob
import json
import os
import subprocess
import sys

import numpy as np
import rasterio

import _env
from geobox import geobox
from geobox import geojsonio

# In Landsat Collection 2, 0 is the NaN fill value
PIXEL_RANGES = {
    'uint16': (1, 65535),
    'uint8': (1, 255)
}

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
    if kwargs.get('mask'):
        write_mask(outfile, next(iter(paths)))
    os.remove(vrtfile)
    return outfile

def combine_bands(prefix, paths):
    """Assemble R-G-B image bands into GDAL .vrt file."""
    combined = prefix + '.vrt'
    commands = ['gdalbuildvrt', '-separate', combined, *paths]
    subprocess.call(commands)
    return combined

def crop_and_rescale(vrtfile, bounds, pixel_ranges=PIXEL_RANGES, **kwargs):
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

    with rasterio.open(vrtfile) as f:
        dtype = f.profile['dtype']
    input_range = pixel_ranges.get(dtype)
    if not input_range:
        raise ValueError(f'Unexpected input dtype {dtype}.')
    wp = kwargs.get('white_point', max(input_range))
    bp = kwargs.get('black_point', min(input_range))
    
    bit_depth = kwargs.get('bit_depth')
    if bit_depth == 8:
        commands += ['-ot', 'Byte']
    elif bit_depth == 16:
        commands += ['-ot', 'UInt16']
    else:
        raise ValueError(f'Invalid output bit depth: {bit_depth}.')
    commands += ['-scale', str(bp), str(wp),
                    *[str(r) for r in pixel_ranges.get(f'uint{bit_depth}')]]
    subprocess.call(commands)
    return tiffile

def write_mask(outfile, raw_tile):
    """Write a no-data mask to outfile based on nodata values in raw_tile."""
    with rasterio.open(raw_tile) as f:
        nodata = f.profile['nodata']
        raw = f.read()[0]
    im = rasterio.open(outfile, 'r+')
    msk = np.ones(im.shape, dtype='bool')
    msk[raw == nodata] = False
    im.write_mask(msk)
    im.close()

def partition(paths, bands, band_sig):
    """Partition input paths by common prefixes and filter by bands.

    Returns: dict of prefixes and paths
    """
    prefixes = set([p.split(band_sig)[0] for p in paths])
    partition = {}
    for prefix in prefixes:
        partition.update({
            prefix: [p for b in bands for p in paths if prefix in p and
                         f'{band_sig}{b}.' in p]})
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
        '-bp', '--black_point',
        type=int,
        help='Image integer bit value to be reset to black (1).'
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
    parser.add_argument(
        '-m', '--mask',
        action='store_true',
        help='Flag: If set, a sidecar no-data mask will be created.'
    )
    parser.add_argument(
        '-bs', '--band_sig',
        type=str,
        default='_B',
        help='The common string immediately preceding Landsat band numbers '
             'in file paths. E.g. paths of form LC09*T1_SR_B4.TIF would take '
             '"_B" or "TI_SR_B". Default: "_B".'
    )
    args = parser.parse_args()

    geoms = geojsonio.load_geometries(args.geojson) if args.geojson else []
    bounds = geobox.bbox_from_geometries(geoms).bounds if geoms else []

    base = os.path.join(args.image_dir, f'*{args.band_sig}?')
    paths = [glob.glob(base + ext) for ext in ['.tif', '.TIF']]
    paths = [p for sublist in paths for p in sublist]
        
    grouped = partition(paths, args.bandlist, args.band_sig)
    for prefix, grouped_paths in grouped.items():
        build_rgb(prefix, grouped_paths, bounds, **vars(args))


    
