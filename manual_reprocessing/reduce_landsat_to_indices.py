"""Routines to process Landsat Surface Reflectance tiles into landcover
indices, following and drawing from reduce_landsat.py.

Requires: A full GDAL install, including python package osgeo. 

Ordered from https://earthexplorer.usgs.gov, scenes are delivered as tar
files containing each band as a separte TIF, with filenames of form

LC08_L1TP_037034_20170309_20180125_01_T1_sr_band2.tif

For Landsat8, NIR-R-G-B bands are numbered 5 4 3 2. For Landsat5,
corresponding bands are numbered 4 3 2 1. (Blue band is never used and
can be omitted.)

Usage: Untar everything into a folder. Multiple scenes are fine, as
the program will untangle them. The only restrictions are that all
band files for a scene must share a common prefix, with filename of
form prefixband?.tif, and be Int16 in type.

$ python reduce_landsat_to_indices.py 5 4 3 2 -i ndvi -g footprint.geojson -d image_dir

For help:
$ python reduce_landsat_to_indices.py -h

The band ordering 5 4 3 2 or 4 3 2 1 is required. The scenes will be
cropped to the footprint, if given. The image_dir defaults to pwd if
not specified.

The routine outputs a Float32 grayscale image for each scene.

"""

import argparse
import glob
from inspect import getsourcefile
import os
import subprocess
import sys

import rasterio

current_dir = os.path.dirname(os.path.abspath(getsourcefile(lambda:0)))
sys.path.insert(1, os.path.dirname(current_dir))
import reduce_landsat
from webapp.grabbers.utilities.geobox import geobox
from webapp.grabbers.utilities.geobox import geojsonio

INDICES = ['ndvi', 'ndwi']

def build_index(prefix, files, bounds, index):
    """Build a landcover index from NIR, color bands.

    Arguments: 
        prefix: common filename prefix for image bands
        files: list of NIR, R, G, B geotiffs
        bounds: lat/lon coordinates, ordered [minx, miny, maxx, maxy], or []
        index: one of the known INDICES

    Output: A float32, grayscale geotiff

    Returns: Geotiff filename
    """
    nirpath = crop(prefix + 'nir', files[0], bounds) 
    if index == 'ndvi':
        colorpath = crop(prefix + 'color', files[1], bounds)
    elif index == 'ndwi':
        colorpath = crop(prefix + 'color', files[2], bounds)
    else:
        raise ValueError('Landcover index not recognized.')
    
    outfile = calculate_index(nirpath, colorpath, index)
    for f in (nirpath, colorpath):
        os.remove(f)
    return outfile

def crop(prefix, bandfile, bounds):
    """Crop bandfile to geographic bounds.

    Output: Writes a geotiff prefix.tif.
    """
    cropfile = prefix + '.tif'
    commands = ['gdal_translate', bandfile, cropfile]
    if bounds:
        gdal_bounds = [str(bounds[n]) for n in (0, 3, 2, 1)]
        commands += ['-projwin_srs', 'EPSG:4326', '-projwin', *gdal_bounds]
    subprocess.call(commands)
    return cropfile

def calculate_index(nirpath, colorpath, index):
    with rasterio.open(nirpath) as f:
        nir = f.read().astype('float32')

    with rasterio.open(colorpath) as f:
        color = f.read().astype('float32')
        profile = f.profile.copy()

    if index == 'ndvi':
        computed = (nir - color)/(nir + color)
    elif index == 'ndwi':
        computed = (color - nir)/(color + nir)
    else:
        raise ValueError('Landcover index not recognized.')

    profile.update({'count': 1, 'dtype': rasterio.float32})
    outfile = nirpath.split('nir.tif')[0] + index + '.tif'
    with rasterio.open(outfile, 'w', **profile) as f:
        f.write(computed)
    return outfile

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
        help='Band numbers to assemble in NIR-R-G-B order. E.g. 5 4 3 2 ' +
            'for Landsat8 or 4 3 2 1 for Landsat5.'
    )
    parser.add_argument(
        '-i', '--indices',
        type=str,
        nargs='+',
        choices=INDICES,
        default=INDICES,
        help='Indices to compute, from {}. Defaults to all.'.format(INDICES)
    )
    parser.add_argument(
        '-g', '--geojson',
        type=str,
        help='Geojson file expressing area of interest for optional crop.'
    )
    parser.add_argument(
        '-d', '--image_dir',
        type=str,
        default='',
        help='Directory containing image band files. Defaults to pwd.'
    )
    args = parser.parse_args()

    geoms = geojsonio.load_geometries(args.geojson) if args.geojson else []
    bounds = geobox.bbox_from_geometries(geoms).bounds if geoms else []
    image_files = glob.glob(os.path.join(args.image_dir, '*band?.tif'))
    grouped = reduce_landsat.partition(image_files, args.bandlist)
    for prefix, files in grouped.items():
        for index in args.indices:
            try:
                build_index(prefix, files, bounds, index)
            except FileNotFoundError as e:
                print('{}\nContinuing...'.format(repr(e)))


    
