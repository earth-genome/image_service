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

current_dir = os.path.dirname(os.path.abspath(getsourcefile(lambda:0)))
sys.path.insert(1, os.path.dirname(current_dir))
import reduce_landsat
from utilities.geobox import geobox
from utilities.geobox import geojsonio

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
    nir = crop_and_retype(prefix + 'nir', files[0], bounds)
    if index == 'ndvi':
        colorband = crop_and_retype(prefix + 'color', files[1], bounds)
    elif index == 'ndwi':
        colorband = crop_and_retype(prefix + 'color', files[2], bounds)
    outfile = calculate_index(nir, colorband, index)
    for f in (nir, colorband):
        os.remove(f)
    return outfile

def crop_and_retype(prefix, bandfile, bounds):
    """Crop bandfile and retype to Float32.

    Outputs a geotiff; returns the filename
    """
    vrtfile, cropfile = prefix + '.vrt', prefix + '.tif'
    commands1 = ['gdalbuildvrt', vrtfile, bandfile,
                '-srcnodata', '-9999', '-vrtnodata', '0']
    commands2 = ['gdal_translate', vrtfile, cropfile, '-a_nodata', 'None',
                '-ot', 'Float32',
                '-scale', '-65535', '65535', '-1.0', '1.0']
    if bounds:
        gdal_bounds = [str(bounds[n]) for n in (0, 3, 2, 1)]
        commands2 += ['-projwin_srs', 'EPSG:4326', '-projwin', *gdal_bounds]
    subprocess.call(commands1)
    subprocess.call(commands2)
    os.remove(vrtfile)
    return cropfile

def calculate_index(nir, colorband, index):
    outfile = nir.split('nir.tif')[0] + index + '.tif'
    commands = ['gdal_calc.py', '-A', nir, '-B', colorband,
                '--outfile={}'.format(outfile)]
    if index == 'ndvi':
        commands += ['--calc=(A-B)/(A+B)']
    elif index == 'ndwi':
        commands += ['--calc=(B-A)/(A+B)']
    else:
        raise ValueError('Landcover index not recognized.')
    subprocess.call(commands)
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


    
