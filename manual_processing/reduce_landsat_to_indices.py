"""Routines to process Landsat Surface Reflectance tiles into landcover
indices, following and drawing from reduce_landsat.py.

Requires: A full GDAL install.

Ordered from https://earthexplorer.usgs.gov, scenes are delivered as tar
files containing each band as a separte TIF.

For Landsat8, NIR-R-G-B bands are numbered 5 4 3 2. For Landsat5,
corresponding bands are numbered 4 3 2 1. (Blue band is never used and
can be omitted.)

Usage: Untar everything into a folder. Multiple scenes are fine, as
the program will untangle them. All band files for a scene must share
a common prefix, with filename of form prefixband?.tif or .TIF.
The variable band_sig is set with user flag and typically will be '_B' or
'band', depending on Landsat file name format. 

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
import os
import subprocess
import sys

import rasterio

import _env
import reduce_landsat
from geobox import geobox
from geobox import geojsonio

INDICES = ['ndvi', 'ndwi']

def build_index(prefix, paths, bounds, index):
    """Build a landcover index from NIR, color bands.

    Arguments: 
        prefix: common filename prefix for image bands
        paths: list of NIR, R, G, B geotiffs
        bounds: lat/lon coordinates, ordered [minx, miny, maxx, maxy], or []
        index: one of the known INDICES

    Output: A float32, grayscale geotiff

    Returns: Geotiff filename
    """
    nirpath = crop(prefix + 'nir', paths[0], bounds) 
    if index == 'ndvi':
        colorpath = crop(prefix + 'color', paths[1], bounds)
    elif index == 'ndwi':
        colorpath = crop(prefix + 'color', paths[2], bounds)
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
        mask = f.read_masks(1)
        profile = f.profile.copy()
    if index == 'ndvi':
        computed = (nir - color)/(nir + color)
    elif index == 'ndwi':
        computed = (color - nir)/(color + nir)
    else:
        raise ValueError('Landcover index not recognized.')

    profile.update({'count': 1, 'dtype': rasterio.float32, 'nodata': None})
    outfile = nirpath.split('nir.tif')[0] + index + '.tif'
    with rasterio.open(outfile, 'w', **profile) as f:
        f.write(computed)
        f.write_mask(mask)
    return outfile

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

    grouped = reduce_landsat.partition(paths, args.bandlist, args.band_sig)
    for prefix, grouped_paths in grouped.items():
        for index in args.indices:
            build_index(prefix, grouped_paths, bounds, index)


    
