"""Wrapper to extract georeferenced vector shapes from a geotiff.  

The extraction is done by the rasterio.features module. The wrapper accepts
GeoJSON features and an existing geotiff as input. The geotiff defines a 
model for a container (raster extent, projection, dtype, etc.) into which 
the features are burned. (The input geotiff is not modified.)

Usage: rasterize.py feature_collection.json model_geotiff.tif

Output: A file features-burned.tif.

"""
import argparse
from inspect import getsourcefile
import json
import os
import sys

import numpy as np
import rasterio
import rasterio.features

current_dir = os.path.dirname(os.path.abspath(getsourcefile(lambda:0)))
sys.path.insert(1, os.path.dirname(current_dir))
import pixel_limits
from webapp.grabbers.utilities.geobox import geojsonio
from webapp.grabbers.utilities.geobox import projections

def extract_shapes(geotiff, raster_vals=None, source_projection=False):
    """Extract vector shapes from geotiff.

    Arguments: 
        geotiff: Path to a georeferenced tif.
        raster_vals: A list of band values defining the shapes to extract;
            e.g. [255, 0, 0] would extract as shapes all purely red pixels
            in a 3-band unit8 image. If None, pure white is used as default. 
        source_projection: If True, keep coordinates in the CRS of the 
            geotiff; otherwise use GeoJSON standard decimal lon/lat.

    Output: Writes to file a GeoJSON Feature Collection

    Returns: The path to the written file
    """
    with rasterio.open(geotiff) as f:
        img = f.read()
        profile = f.profile

    epsg_code = profile['crs']['init'].split('epsg:')[-1]
    pixel_max = pixel_limits.get_max(profile['dtype'])
    bands = list(range(profile['count']))
    
    if not raster_vals:
        raster_vals = [pixel_max for _ in bands]
    if len(raster_vals) != len(bands):
        msg = ('Raster values ({}) must match number of bands ({}).'.format(
            len(raster_vals), len(bands)) + 'Or give none for pure white.')
        raise ValueError(msg)

    band_mask = np.all(img.T == raster_vals, axis=-1).T
    mask = np.asarray([band_mask for _ in bands])
    shapes = rasterio.features.shapes(img, mask=mask,
                                      transform=profile['transform'])
    geoms = (s[0] for s in shapes)

    geojson = '.'.join(geotiff.split('.')[:-1]) + '-features.json'
    if source_projection:
        geojsonio.write_geometries(geoms, geojson, epsg_code=epsg_code)
    else:
        geoms = [projections.project_geojson_geom(g, epsg_code, inverse=True)
                     for g in geoms]
        geojsonio.write_geometries(geoms, geojson, epsg_code=None)

    return geojson

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'geotiff',
        help='Georeferenced tif from which to extract vector shapes.')
    parser.add_argument(
        '-rv', '--raster_vals',
        type=int,
        nargs='+',
        help=('Pixel values that define shapes to extract, one for each band.'
              'E.g. -rv 255 0 0 to indicate pure red for a 3-band uint8 image. '
              'If not given, shapes will be extracted where pixels are pure '
              'white (equivalent to -rv 255 255 255 for uint8).'))
    parser.add_argument(
        '-sp', '--source_projection',
        action='store_true',
        help=('Flag. If set, the vector shapes will be returned using the '
              'projection of the source geotiff instead of GeoJSON-standard '
              'decimal lon/lat.'))
    args = parser.parse_args()

    outpath = extract_shapes(**vars(args))
    print('Wrote {}'.format(outpath))
