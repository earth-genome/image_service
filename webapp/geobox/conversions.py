"""Short routines for converting increments in lat/lon to kilometer distances.

External functions:
    latitude_from_dist
    dist_from_latitude
    longitude_from_dist
    dist_from_longitude

"""

import numpy as np

# Conversion for latitudes:
KM_PER_DEGREE = 111

def latitude_from_dist(dist):
    """Convert a ground distance to decimal degrees latitude."""
    return float(dist)/KM_PER_DEGREE

def dist_from_latitude(deltalat):
    "Convert an increment in latitude to a ground distance in km."""
    return float(deltalat)*KM_PER_DEGREE

def longitude_from_dist(dist, lat):
    """Convert a ground distance to decimal degrees longitude."""
    return dist/(np.cos(np.radians(np.abs(lat)))*KM_PER_DEGREE)

def dist_from_longitude(deltalon, lat):
    "Convert an increment in longitude to a ground distance in km."""
    return deltalon*(np.cos(np.radians(np.abs(lat)))*KM_PER_DEGREE)

