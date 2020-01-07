"""Routines for creating and manipulating geocoordinate boxes."""

import numpy as np
from shapely import geometry
from shapely.ops import unary_union

from geobox import conversions

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

    Returns: A shapely Polygon.
    """
    bounds = [lon-deltalon/2., lat-deltalat/2.,
                         lon+deltalon/2., lat+deltalat/2.]
    return geometry.box(*bounds)

def bbox_from_scale(lat, lon, scale):
    """Make a bounding box given lat/lon and scale in km."""
    bbox = make_bbox(lat, lon,
                     conversions.latitude_from_dist(scale),
                     conversions.longitude_from_dist(scale, lat))
    return bbox

def square4326_bbox_from_scale(lat, lon, scale):
    """Make a bounding box given lat/lon and scale in km.

    This routine reverses the expansion in longitude due to EPSG:4326
    projection by decreasing the increment in longitude by cos(lat).
    (E.g. Digital Globe uses EPSG:4326 by default.)
    """
    deltalat = conversions.latitude_from_dist(scale)
    deltalon = (conversions.longitude_from_dist(scale, lat) *
        np.cos(np.radians(np.abs(lat))))
    bbox = make_bbox(lat, lon, deltalat, deltalon)
    return bbox

def bbox_from_geometries(geoms):
    """Determine a bounding box for a list of geojson geometries.

    Returns: shapely box
    """
    union = unary_union([geometry.asShape(g) for g in geoms])
    return geometry.box(*union.bounds)
    
def osm_to_shapely_box(osm_bbox):
    """Convert a bounding box in OSM convention to a shapely box.

    OSM retuns strings in order (S Lat, N Lat, W Lon, E Lon),
        while a shapely box takes arguments:
        shapely.geometry.box(minx, miny, maxx, maxy, ccw=True)

    Arugment osm_bbox: boundingbox from an OSM record

    Returns: shapely box
    """
    bounds = np.array(osm_bbox, dtype=float)
    return geometry.box(*bounds[[2,0,3,1]])

def viewport_to_shapely_box(viewport):
    """Convert a Google or OpenCage viewport to a shapely box.

    Argument viewport: A viewport is a dict of form:
        {'northeast': {'lat': -33.9806474, 'lng': 150.0169685},
          'southwest': {'lat': -39.18316069999999, 'lng': 140.9616819}}

    Returns: shapely box
    """
    points = geometry.asMultiPoint([[p['lng'], p['lat']]
                                    for p in viewport.values()])
    return geometry.box(*points.bounds)
    
def shapely_to_gdal_box(bbox):
    """Convert a shapely box to a coordinate list ordered for gdal_translate.

    Argument bbox:  Shapely box, whose bounds are ordered
        (minx, miny, maxx, maxy)

    Returns: Tuple in order (upper-left-x, upper-left-y, lower-right-x,
        lower-right-y).
    """
    bounds = np.asarray(bbox.bounds)
    return list(bounds[[0, 3, 2, 1]])
