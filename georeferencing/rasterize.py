"""Wrapper to burn georeferenced vector shapes to geotiff.  

The burning is done by the rasterio.features module. The wrapper accepts
GeoJSON features and an existing geotiff as input. The geotiff defines a 
model for a container (raster extent, projection, dtype, etc.) into which 
the features are burned. (The input geotiff is not modified.)

Usage: $ python rasterize.py feature_collection.json model_geotiff.tif

Output: A file features-burned.tif.

"""
import argparse
from inspect import getsourcefile
import json
import os
import sys

import rasterio
import rasterio.features

current_dir = os.path.dirname(os.path.abspath(getsourcefile(lambda:0)))
sys.path.insert(1, os.path.dirname(current_dir))
import pixel_limits
from webapp.grabbers.utilities.geobox import geojsonio
from webapp.grabbers.utilities.geobox import projections

def burn(geojson, geotiff):
    """Burn features in geojson into a container defined by existing geotiff."""
    with rasterio.open(geotiff) as f:
        profile = f.profile.copy()

    epsg_code = profile['crs']['init'].split('epsg:')[-1]
    shape = (profile['height'], profile['width'])
    pixel_max = pixel_limits.get_max(profile['dtype'])
    geoms = geojsonio.load_geometries(geojson)
    geoms = [projections.project_geojson_geom(g, epsg_code) for g in geoms]

    raster = rasterio.features.rasterize(((geom, pixel_max) for geom in geoms),
                                         out_shape=shape,
                                         transform=profile['transform'])
    
    raster = raster.reshape((1,) + shape)
    profile.update({'count': 1})
    rasterfile = '.'.join(geojson.split('.')[:-1]) + '-burned.tif'
    with rasterio.open(rasterfile, 'w', **profile) as f:
        f.write(raster)

    return rasterfile


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'geojson',
        help='File containing a GeoJSON Feature or Feature Collection.')
    parser.add_argument(
        'geotiff',
        help='Georeferenced tif file to serve as a model raster container.')
    args = parser.parse_args()
        
    outpath = burn(**vars(args))
    print('Wrote {}'.format(outpath))
