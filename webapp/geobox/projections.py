"""Routines for projection of geocoordinates."""

import json

import pyproj

def get_utm_code(lat, lon):
    """Compute the UTM EPSG zone code in which lat, lon fall."""
    basecode = 32601 if lat > 0 else 32701
    return basecode + int((180 + lon)/6.)

def project_to_utm(lat, lon, epsg_code=None):
    """Project lat, lon to UTM northing, easting.

    If UTM epsg_code is None, it is inferred from lat, lon.

    Arguments:
        lat, lon: decimal lat, lon
        epsg_code: integer component of UTM EPSG code, e.g. 32601

    Returns: easting, northing (in meters)
    """
    if not epsg_code:
        epsg_code = get_utm_code(lat, lon)
    projector = pyproj.Proj('epsg:{}'.format(epsg_code))
    return projector(lon, lat)

def project_geojson_geom(geom, epsg_code, inverse=False):
    """Reproject the coordinates in a geojson-like geometry.

    The forward projection takes coordinates from decimal (lon, lat) to 
        coordinates defined by epsg_code; the inverse transform is 
        available with inverse=True.

    Returns: A copy of the dict geom, with coordinates updated.
    """
    proj = pyproj.Proj('epsg:{}'.format(epsg_code))

    def _recurse_arrays(coord_array, proj, inverse):
        try:
            iter(coord_array[0])
        except TypeError:
            return list(proj(*coord_array[:2], inverse=inverse))
        return [_recurse_arrays(c, proj, inverse) for c in coord_array]

    new_coords = _recurse_arrays(geom['coordinates'], proj, inverse)
    projected = geom.copy()
    projected.update({'coordinates': new_coords})
    return projected
