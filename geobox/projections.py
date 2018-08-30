"""Routines for projection of geocoordinates."""

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
    proj = pyproj.Proj(init='epsg:{}'.format(epsg_code))
    return proj(lon, lat)
