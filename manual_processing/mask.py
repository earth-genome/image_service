"""Wrapper to mask a geotiff with vector features.  

The masking is done by the rasterio.mask module, ref:
 https://rasterio.readthedocs.io/en/stable/api/rasterio.mask.html

The handling offered here, beyond that of the rasterio cli, is to map the 
vector features into the coordinate system of the geotiff. 

Usage: $ python mask.py img.tif feature_collection.json [-nd, -f, -i]
See: $ python mask.py --help
Output: A file img-masked.tif.

"""
import argparse
import json
import os
import sys

import rasterio
import rasterio.mask

import _env
from geobox import geojsonio
from geobox import projections

def mask(geotiff, geojson, clean=False, filled=False, **kwargs):
    """Mask geotiff with geojson features.

    Arguments:
        geotiff: A GeoTiff
        geojson: Path to a GeoJSON Feature or Feature Collection
        clean: bool: To delete input file after processing
        filled: bool: To fill masked areas with nodata value, or if not,
            to return a masked image.
        **kwargs: Further options to pass to rasterio.mask.mask(), e.g.:
            nodata: Override nodata value. Defaults to value for geotiff, or 0.
            invert: bool: To mask the areas _inside_ the vector shapes.
    
    Returns: Path to the masked geotiff.
    """
    with rasterio.open(geotiff) as dataset:
        profile = dataset.profile.copy()
        epsg_code = profile['crs']['init'].split('epsg:')[-1]
        geoms = geojsonio.load_geometries(geojson)
        geoms = [projections.project_geojson_geom(g, epsg_code) for g in geoms]
        masked, _ = rasterio.mask.mask(dataset, geoms, filled=filled, **kwargs)

    outpath = geotiff.split('.tif')[0] + '-masked.tif'
    with rasterio.open(outpath, 'w', **profile) as of:
        of.write(masked)
        if not filled:
            gdalmask = (~masked.mask)[0]
            of.write_mask(gdalmask)
    if clean:
        os.remove(geotiff)
    return outpath

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'geotiff',
        help='Georeferenced tif file to serve as a model raster container.')
    parser.add_argument(
        'geojson',
        help='File containing a GeoJSON Feature or Feature Collection.')
    parser.add_argument(
        '-nd', '--nodata', type=float,
        help=('Override nodata value. Defaults to value for input geotiff, ' +
              'if available, or 0.'))
    parser.add_argument(
        '-f', '--filled', dest='filled', action='store_true',
        help=('Flag. If set, the masked areas are filled with nodata value.'))
    parser.add_argument(
        '-i', '--invert', action='store_true',
        help='Flag. If set, the area _inside_ vector shapes will be masked.')
    args = parser.parse_args()

    outpath = mask(**vars(args))
    print('Wrote {}'.format(outpath))
