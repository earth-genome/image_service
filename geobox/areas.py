"""Compute areas of GeoJSON features, using UTM projections."""

import argparse
import json
import sys

import numpy as np
import shapely.geometry

import projections

def compute_area(feature, epsg_code=None):
    """Compute area of a geojson Feature given in geographic coordinates.
    
    If epsg_code is None, a UTM zone is inferred from the geometry centroid.
    
    Arguments:
        feature: A geojson Feature. Coordinates are specified 
            [[[lon1, lat1, z1], [lon2, lat2, z2], ...]]
        epsg_code: Integer component of an EPSG code, e.g. 32601
    """
    if not epsg_code:
        centroid = shapely.geometry.asShape(feature['geometry']).centroid
        lon, lat = centroid.x, centroid.y
        epsg_code = projections.get_utm_code(lat, lon)

    projected_geom = _project(feature['geometry'], epsg_code=epsg_code)
    polygon = shapely.geometry.asShape(projected_geom)
    return polygon.area

def compute_areas(feature_collection_fname, overwrite=True):
    """Compute areas for all features in a FeatureCollection file.

    Arguments:
        feature_collection_fname: Filename for a GeoJSON Feature Collection
        overwrite: If True, overwite input file to include computed areas

    Returns: List of areas for features in the collection
    """
    with open(feature_collection_fname, 'r') as f:
        geojsons = json.load(f)

    areas = []
    for feature in geojsons['features']:
        area = compute_area(feature)
        feature['properties'].update({'area': '{:.2g}'.format(area)})
        areas.append(area)

    if overwrite:
        geojsons.update({'Total area': '{:.2g}'.format(np.sum(areas))})
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
        '-f', '--filename',
        type=str,
        help='A filename for a GeoJSON FeatureCollection'
    )
    parser.add_argument(
        '-o', '--overwrite',
        action='store_true',
        help=('Flag. If set, input json will be overwitten to include ' +
              'computed areas.')
    )
    args = parser.parse_args()
    print(vars(args))
    areas = compute_areas(args.filename, overwrite=args.overwrite)
    print('Areas: {}\nTotal: {:.2g}'.format(areas, np.sum(areas)))
