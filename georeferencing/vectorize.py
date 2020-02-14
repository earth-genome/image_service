"""Wrapper to extract georeferenced vector shapes from a geotiff.  

External functions: extract_shapes, extract_valid

"""
import argparse
import json
import os
import sys

import numpy as np
import rasterio
import rasterio.features
import shapely.geometry

import _env
from geobox import geojsonio
from geobox import projections
import pixel_limits

ESSENTIAL_PROFILE = ['driver', 'dtype', 'width', 'height', 'crs', 'transform']

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

def extract_valid(geotiff, nodata=None, smoothing=.001):
    """Get a smoothed vector boundary of valid data values in a GeoTiff.

    Arguments:
        geotiff: Path to a GeoTiff
        nodata:  An override nodata value; if None, a nodata value specified
            in the geotiff header will be used.
        smoothing: Float passed to shapely object.simplify(). 
            Larger values result in smoother features. If zero, no smoothing
            is applied.

    Returns: Path to a GeoJSON
    """
    black_white = _write_black_white(geotiff, nodata)
    full_geojson = extract_shapes(black_white)
    
    with open(full_geojson) as f:
        boundary = json.load(f)
    if smoothing:
        boundary = _simplify(boundary, smoothing)
    
    boundary_file = geotiff.split('.tif')[0] + '-boundary.json'
    with open(boundary_file, 'w') as f:
        json.dump(boundary, f)
        
    os.remove(full_geojson)
    os.remove(black_white)
    return boundary_file

def _write_black_white(geotiff, nodata=None):
    """Write a new geotiff with valid as white, nodata values as black."""
    with rasterio.open(geotiff) as f:
        img = f.read()
        prof = {k:v for k,v in f.profile.items() if k in ESSENTIAL_PROFILE}
        nodata = nodata if nodata is not None else f.profile.get('nodata')
    if nodata is None:
        raise ValueError('A nodata value is required.')

    prof.update({'count': 1})
    pmax = pixel_limits.get_max(prof['dtype'])
    black_white = np.all(img==nodata, axis=0).astype(prof['dtype'])
    black_white = (pmax - pmax*black_white).reshape(1, *black_white.shape)

    bw_path = geotiff.split('.tif')[0] + 'bw.tif'
    with rasterio.open(bw_path, 'w', **prof) as f:
        f.write(black_white)
    return bw_path
    
def _simplify(gj_object, smoothing_factor):
    """Simplify and delete resulting null features."""
    geoms = geojsonio.list_geometries(gj_object)
    smoothed_and_cleaned = []
    for g in geoms:
        shape = shapely.geometry.asShape(g)
        smoothed = shapely.geometry.mapping(
            shape.simplify(smoothing_factor, preserve_topology=False))
        if smoothed['coordinates']:
            smoothed_and_cleaned.append(smoothed)
    return geojsonio.format_geometries(smoothed_and_cleaned)

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
