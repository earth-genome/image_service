"""Functions to read and write GeoJSON-like geometries to/from geojson 
objects or files.

Note that the use of CRS other than EPSG:4326 (decimal lat / lon) has been 
removed from the current GeoJSON standard, RFC 7946, Aug. 2016. Nonetheless
it is permitted as an option in format_geometries below. 
"""

import json

GEOMETRY_TYPES = ["Point", "MultiPoint", "LineString", "MultiLineString",
                  "Polygon", "MultiPolygon"]

def list_geometries(gj_object):
    """Extract geometries from a geojson object.

    Arguments: 
        gj_object: A GeoJSON FeatureCollection, Feature, or geometry object

    Returns: A list of geojson geometries
    """
    try:
        object_type = gj_object['type']
    except KeyError:
        raise KeyError('Invalid geojson format. A type is required.')
    
    if object_type == 'FeatureCollection':
        geoms = [f['geometry'] for f in gj_object['features']]
    elif object_type == 'Feature':
        geoms = [gj_object['geometry']]
    elif object_type == 'GeometryCollection':
        geoms = gj_object['geometries']
    elif object_type in GEOMETRY_TYPES:
        geoms = [gj_object]
    else:
        raise ValueError('Invalid geojson type <{}>.'.format(object_type))

    return geoms
    
def load_geometries(geojson_file):
    """Extract geometries from a geojson file."""
    with open(geojson_file, 'r') as f:
        gj_object = json.load(f)
    return list_geometries(gj_object)
    
def format_geometries(geoms, epsg_code=None):
    """Assemble geometries into a geojson Feature Collection.

    Arguments: 
        geoms: An iterable over geojson-like geometries
        epsg_code: Optional code to note if using a non-standard CRS

    Returns: A Feature Collection as dict
    """
    features = [{'type': 'Feature', 'properties': {}, 'geometry': g}
                    for g in geoms]
    collection = {'type': 'FeatureCollection', 'features': features}
    if epsg_code:
        collection.update({'crs': format_crs(epsg_code)})
    return collection

def write_geometries(geoms, geojson_file, epsg_code=None):
    """Write geometries to filename as a Feature Collection.

    Arguments: 
        geoms: An iterable over geojson-like geometries
        geojson_file: Output filename
        epsg_code: Optional code to note if using a non-standard CRS
    """
    collection = format_geometries(geoms, epsg_code=epsg_code)
    with open(geojson_file, 'w') as f:
        json.dump(collection, f)

def format_crs(epsg_code):
    """Format a CRS element for a GeojSON object.

    Returns: Dict
    """
    crs_element = {'type': 'EPSG', 'properties': {'code': epsg_code}}
    return crs_element
