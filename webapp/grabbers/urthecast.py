"""Class structure to automate searching the Urthecast catalog.


Class UCImageSearch: A class to grab an image respecting given specs.

    Attributes:
        bbox:  a shapely box, with (x,y) coordinates (lon, lat)
        latlon: centroid of bbox
        params: catalog search and image parameters
        catalog_filters: catalog params in DG format
        image_specs:  image paramsrameters
        grabber: DG image grabbing class object 

    External methods:
        __call__: Search the database for available image(s).
            Returns: Dict of catalog record(s) and Dask object(s) for the
                area defined by bbox
        search_catalog:
            Returns a list of relevant records.

"""
import numpy as np
import requests
from shapely import geometry, wkt
import xml.etree.ElementTree as ET



def query(lat, lon, scale, begin_date, end_date, sensor='DE1'):

    minx, miny, maxx, maxy = bbox_from_scale(lat, lon, scale).bounds

    tree = ET.Element(
        'ogc:Filter', 
        attrib={
            'xmlns:ogc': 
            'http://www.opengis.net/ogc'
        }
    )

    conditions = ET.SubElement(tree, 'ogc:And')
  
    date_condition = ET.SubElement(conditions, 'ogc:PropertyIsBetween')
    date_name = ET.SubElement(date_condition, 'ogc:PropertyName')
    date_name.text = 'image_date'
    
    date_lower = ET.SubElement(date_condition, 'ogc:LowerBoundary')
    date_lower_literal = ET.SubElement(date_lower, 'ogc:Literal')
    date_lower_literal.text = '2015-02-20 00:00'

    date_upper = ET.SubElement(date_condition, 'ogc:UpperBoundary')
    date_upper_literal = ET.SubElement(date_upper, 'ogc:Literal')
    date_upper_literal.text = '2018-02-13 23:59'

    bbox = ET.SubElement(conditions, 'ogc:BBOX')
    bbox_geom_name = ET.SubElement(bbox, 'ogc:PropertyName')
    bbox_geom_name.text = 'the_geom'

    bbox_geom = ET.SubElement(
        bbox, 'gml:Box', 
        attrib={
            'xmlns:gml': 'http://www.opengis.net/gml', 
            'srsName': 'EPSG:4326'
        }
    )

    bbox_coords = ET.SubElement(
        bbox_geom, 
        'gml:coordinates', 
        attrib={
            'decimal': '.',
            'cs': ',',
            'ts': ' '
        }
    )

    bbox_coords.text = '%s,%s %s,%s' %(minx, miny, maxx, maxy)

    feature_query = ET.tostring(tree)


    params = {
        'SERVICE': 'WFS',
        'VERSION' : '1.1.0',
        'REQUEST': 'GetFeature',
        'TYPENAME': 'DE1',
        'SRSNAME': 'EPSG:4326',
        'FILTER': feature_query
    }

    BASE = 'http://www.deimos-imaging.com/cgi-bin/mapwfs-ext-2018'

    return requests.get(BASE, params=params).text



def parse_response(xml):
    ns = {
        # namespaces for the XML response
        'ms': 'http://mapserver.gis.umn.edu/mapserver',
        'gml': 'http://www.opengis.net/gml', 
        'wfs': 'http://www.opengis.net/wfs', 
        'ogc': 'http://www.opengis.net/ogc'
    }

    root = ET.fromstring(xml)

    def _parse_feature(feature):
        # only one child per feature, where the tag depends on the sensor (DE1
        # vs. DE2)
        [id_element] = [i for i in feature]
        name = feature.find('.//{%s}image_name' % ns['ms']).text 
        idx = id_element.attrib['{%s}id' % ns['gml']] 
        date = feature.find('.//{%s}image_date' % ns['ms']).text 
        cloud_percent = int(feature.find('.//{%s}cloudpercent' % ns['ms']).text)

        return dict(
            name=name, 
            id=idx, 
            date=date, 
            cloud_percent=cloud_percent
        )

    return [_parse_feature(f) for f in root.findall('gml:featureMember', ns)]



class UCImageSearch(object):
    
    """Class UCImageSearch: Tool to search Urthecast catalog given specs.

    Attributes:
        bbox:  a shapely box, with (x,y) coordinates (lon, lat)
        latlon: centroid of bbox
        params: catalog search and image parameters
        catalog_filters: catalog params in DG format
        image_specs:  image parameters
        grabber: DG image grabbing class object 
    """

    def __init__(self, bbox):
        self.bbox = bbox
        self.latlon = self.bbox.centroid

            
    def search_catalog(self):
        """Search the DG catalog for relevant imagery."""
        records = []
        return records

