"""Routine to determine a UTM EPSG code for a given lat, lon."""

def get_utm_code(lat, lon):
    """Compute a UTM EPSG code."""
    basecode = 32601 if lat > 0 else 32701
    return basecode + int((180 + lon)/6.)
