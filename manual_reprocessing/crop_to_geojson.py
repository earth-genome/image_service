"""Routine to crop a GeoTIFF to the bounding box of a GeoJSON vector geometry.

Usage:
$ python crop_to_geojson.py img.tif -g box.json

Outputs: imgcrop.tif

Requires: gdal_translate
"""

import argparse
from inspect import getsourcefile
import os
import subprocess
import sys

current_dir = os.path.dirname(os.path.abspath(getsourcefile(lambda:0)))
sys.path.insert(1, os.path.dirname(current_dir))
from webapp.geobox import geobox
from webapp.geobox import geojsonio


def crop(geotiff, geojson, outpath):
    """Crop geotiff to bounding box of geojson and save to outpath."""

    bbox = geobox.bbox_from_geometries(geojsonio.load_geometries(geojson))
    gdal_bounds = [str(bbox.bounds[n]) for n in (0, 3, 2, 1)]

    commands = ['gdal_translate', geotiff, outpath, 
                    '-projwin_srs', 'EPSG:4326', '-projwin', *gdal_bounds]
    subprocess.call(commands)
    return outpath

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'geotiff',
        type=str,
        help='Filename of GeoTIFF to crop.'
    )
    parser.add_argument(
        '-o', '--outpath',
        type=str,
        help=('Optional path for output file (defaults to ' 
                  '<input file prefix>crop.tif).')
    )
    req_group = parser.add_argument_group(title='required flags')
    req_group.add_argument(
        '-g', '--geojson',
        type=str,
        required=True,
        help='GeoJSON file expressing area of interest for crop.'
    )
    args = parser.parse_args()

    croppath = args.geotiff.split('.tif')[0] + 'crop.tif'
    outpath = args.outpath if args.outpath else croppath

    crop(args.geotiff, args.geojson, outpath)
    print('Wrote {}'.format(outpath))
