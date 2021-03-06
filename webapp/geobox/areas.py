"""Compute areas of GeoJSON features, using UTM projections."""

import argparse
from inspect import getsourcefile
import json
import os
import sys

import numpy as np
import shapely.geometry

current_dir = os.path.dirname(os.path.abspath(getsourcefile(lambda:0)))
sys.path.insert(0, os.path.dirname(current_dir))
from geobox import projections

def compute_area(feature, projection=None):
    """Compute area of a geojson Feature given in geographic coordinates.
    
    If epsg_code is None, a UTM zone is inferred from the geometry centroid.
    
    Arguments:
        feature: A geojson Feature. Coordinates are specified 
            [[[lon1, lat1, z1], [lon2, lat2, z2], ...]]
        projection: Integer component of an EPSG code, e.g. 32601, or 1
            to indicate that a UTM code should be computed
    """
    if projection:
        if projection == 1:
            centroid = shapely.geometry.asShape(feature['geometry']).centroid
            lon, lat = centroid.x, centroid.y
            epsg_code = projections.get_utm_code(lat, lon)
        else:
            epsg_code = projection
        projected_geom = _project(feature['geometry'], epsg_code=epsg_code)
    else:
        projected_geom = feature['geometry']

    polygon = shapely.geometry.asShape(projected_geom)
    return polygon.area

def compute_areas(feature_collection_fname, projection=None, overwrite=True):
    """Compute areas for all features in a FeatureCollection file.

    Arguments:
        feature_collection_fname: Filename for a GeoJSON Feature Collection
        projection: Integer component of an EPSG code, or 1 to indicate
            that a UTM projection should be determined.
        overwrite: If True, overwite input file to include computed areas

    Returns: List of areas for features in the collection
    """
    with open(feature_collection_fname, 'r') as f:
        geojsons = json.load(f)

    areas = []
    for feature in geojsons['features']:
        area = compute_area(feature, projection=projection)
        feature['properties'].update({'area': '{:.3g}'.format(area)})
        areas.append(area)

    if overwrite:
        geojsons.update({
            'Total area': '{:.2g}'.format(np.sum(areas)),
            'Number of features': '{}'.format(len(areas))
        })
        with open(feature_collection_fname, 'w') as f:
            json.dump(geojsons, f, indent=4)
    return areas

def _project(geom, epsg_code=None):
    """Project each geographic coordinate of input geom."""
    projected_geom = json.loads(json.dumps(geom))
    for i, component in enumerate(geom['coordinates']):
        for j, point in enumerate(component):
            lon, lat = point[:2]
            easting, northing = projections.project_to_utm(lat, lon,
                                                           epsg_code=epsg_code)
            projected_geom['coordinates'][i][j] = [easting, northing]
    return projected_geom
                    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Compute areas of GeoJSON features, using UTM projections.'
    )
    parser.add_argument(
        'filename',
        type=str,
        help='A filename for a GeoJSON FeatureCollection'
    )
    parser.add_argument(
        '-p', '--projection',
        type=int,
        help=('Integer component of an EPSG code, or 1 to have the ' +
              'relevant UTM code computed. If not given, the input ' + 
              'coordinates will be presumed to be already projected.')
    )
    parser.add_argument(
        '-o', '--overwrite',
        action='store_true',
        help=('Flag. If set, input json will be overwitten to include ' +
              'computed areas.')
    )
    args = vars(parser.parse_args())
    print(args)
    filename = args.pop('filename')
    areas = compute_areas(filename, **args)
    print('Areas: {}'.format(areas))
    print('Number: {}\nTotal area: {}'.format(len(areas), np.sum(areas)))
