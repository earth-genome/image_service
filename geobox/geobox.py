"""Short routines for creating and manipulating geocoordinate boxes.

External functions:
    get_side_distances
    make_bbox
    bbox_from_scale
    square_bbox_from_scale
    osm_to_shapely_box

"""

import numpy as np
from shapely import geometry

from . import conversions

def get_side_distances(bbox):
    """Determine width and height of bbox in km, given coords in lat/lon.

    Argument bbox: shapely box

    Returns: width, height in km
    """
    lon, lat = bbox.centroid.x, bbox.centroid.y
    x_coords, y_coords = bbox.boundary.coords.xy
    deltalon = np.max(x_coords) - np.min(x_coords)
    deltalat = np.max(y_coords) - np.min(y_coords)
    deltax = conversions.dist_from_longitude(deltalon, lat)
    deltay = conversions.dist_from_latitude(deltalat)
    return deltax, deltay

def make_bbox(lat, lon, deltalat, deltalon):
    """Return a bounding box centered on given latitude/longitude.

    Returns:  a shapely Polygon.
    """
    bbox = [lon-deltalon/2., lat-deltalat/2.,
                         lon+deltalon/2., lat+deltalat/2.]
    return geometry.box(*bbox)

def bbox_from_scale(lat, lon, scale):
    """Make a bounding box given lat/lon and scale in km."""
    bbox = make_bbox(lat, lon,
                     conversions.latitude_from_dist(scale),
                     conversions.longitude_from_dist(scale, lat))
    return bbox

def square_bbox_from_scale(lat, lon, scale):
    """Make a bounding box given lat/lon and scale in km.

    This routine reverses the compression in latitude from geoprojection
    by increasing the increment in latitude by 1/cos(lat).
    """
    deltalat = conversions.latitude_from_dist(scale)/np.cos(
        np.radians(np.abs(lat)))
    deltalon = conversions.longitude_from_dist(scale, lat)
    bbox = make_bbox(lat, lon, deltalat, deltalon)
    return bbox

def osm_to_shapely_box(osm_bbox):
    """Convert a bounding box in OSM convention to a shapely box.

    OSM retuns strings in order (S Lat, N Lat, W Lon, E Lon),
        while a shapely box takes arguments:
        shapely.geometry.box(minx, miny, maxx, maxy, ccw=True)

    Arugment osm_bbox: boundingbox from an OSM record

    Returns: shapely box
    """
    bbox = np.array(osm_bbox, dtype=float)
    return geometry.box(*bbox[[2,0,3,1]])

def google_to_shapely_box(viewport):
    """Convert a Google viewport to a shapely box.

    Argument viewport: A Google viewport is a dict of form:
        {'northeast': {'lat': -33.9806474, 'lng': 150.0169685},
          'southwest': {'lat': -39.18316069999999, 'lng': 140.9616819}}

    Returns: shapely box
    """
    points = geometry.asMultiPoint([[p['lng'], p['lat']]
                                    for p in viewport.values()])
    return geometry.box(*points.bounds)
    

