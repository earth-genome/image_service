"""Class structure to automate searching and downloadling from DG catalog.

API ref: http://gbdxtools.readthedocs.io/en/latest/index.html

Class DGImageGrabber: A class to grab an image respecting given specs.

    Attributes:
        bbox:  a shapely box, with (x,y) coordinates (lon, lat)
        latlon: centroid of bbox
        image_source:  DG catalog source and sensor(s)
        N_images: number of images to retrieve
        min_intersect: minimum percent area overlap of bbox & image
        params: catalog search and image parameters
        catalog_filters: catalog params in DG format
        image_specs:  image parameters
        grabber: DG image grabbing class object 

    External methods:
        __call__: Search the database for available image(s).
            Returns: Dict of catalog record(s) and Dask object(s) for the
                area defined by bbox
        search_catalog:
            Returns a list of relevant records.

Notes on Attribute image_source:

image_source is from ('WV', 'Landsat8'), and maybe eventually also 'TMS'.
This refers both to a DG image class ('WV' ~ CatalogImage,
'Landsat8' ~ LandsatImage, future 'TMS' ~ TmsImage), and in the case of 
'WV', the assignment entails also assumptions about particular satellite 
sensors expressed below in build_filters().  If image_source is None,
guess_resolution() is called to make a best guess based on bbox size
and hard-coded SCALE parameters.  

See below also for functions to convert distances to lat/lon and
to create bounding boxes given various inputs.
"""

import numpy as np
import gbdxtools
from shapely import geometry, wkt

# Conversion for latitudes:
KM_PER_DEGREE = 111

# Default catalog and image parameters:
CATALOG_PARAMS = {
    'clouds': 10,   # max percentage allowed cloud cover
    'offNadirAngle': None,  # (relation, angle), e.g. ('<', 10)
    'startDate': '2012-01-01T09:51:36.0000Z',
    'endDate': None
}
IMAGE_SPECS = {
    'band_type': 'MS',
    'pansharpen': False,
    'acomp': True,
    'proj': 'EPSG:4326'  # DG default
    #TODO: orthorectification?
}
DEFAULT_PARAMS = CATALOG_PARAMS.copy()
DEFAULT_PARAMS.update(IMAGE_SPECS)

# Image scale thresholds for guess_resolution(), width/height in km:
SMALL_SCALE = 2.0
MID_SCALE = 10.0 


class DGImageGrabber(object):
    
    """Class DGImageGrabber: Tool to grab a DG image respecting given specs.

    Attributes:
        bbox:  a shapely box, with (x,y) coordinates (lon, lat)
        latlon: centroid of bbox
        image_source:  DG catalog source and sensor(s)
        N_images: number of images to retrieve
        min_intersect: minimum percent area overlap of bbox & image
        params: catalog search and image parameters
        catalog_filters: catalog params in DG format
        image_specs:  image parameters
        grabber: DG image grabbing class object 

    External methods:
        __call__: Search the database for available image(s).
            Returns: Dict of catalog record(s) and Dask object(s) for the
                area defined by bbox
        search_catalog:
            Returns a list of relevant records.
    """

    def __init__(self, bbox, image_source=None, N_images=3,
                 min_intersect=.9, **params):
        self.bbox = bbox
        self.latlon = self.bbox.centroid
        if image_source is None:
            self.image_source, params['pansharpen'] = guess_resolution(bbox)
        else:
            self.image_source = image_source
        self.N_images = N_images
        self.min_intersect = min_intersect
        self.params = DEFAULT_PARAMS.copy()
        if params != {}:
            self.params.update(params)
        self.catalog_filters = self.build_filters()
        self.image_specs = {
            k:v for k,v in self.params.items() if k in IMAGE_SPECS.keys()
        }
        self.grabber = self.build_grabber()

    def __call__(self, bbox=None):
        """Grab most recent available images satifying instance parameters.

        Argument: possible new bbox to use in lieu of self.bbox

        Returns: List of images (areas of interest) as Dask objects,
            list of corresponding catalog records
        """
        if bbox is None:
            bbox = self.bbox
        records = self.search_catalog()
        records_by_date = sorted(records,
                                key=lambda t: t['properties']['timestamp'])
        imgs, recs_retrieved = [], []
        while len(records_by_date) > 0 and len(imgs) < self.N_images:
            record = records_by_date.pop()
            id = record['identifier']
            footprint = wkt.loads(record['properties']['footprintWkt'])
            intersect = bbox.intersection(footprint).area/(bbox.area)
            print 'Catalog ID {}:'.format(id)
            print 'Timestamp: {}, Sensor: {}'.format(
                record['properties']['timestamp'],
                record['properties']['sensorPlatformName']
            )
            print 'Percent area intersecting bounding box: {:.2f}'.format(
                intersect)
            if intersect < self.min_intersect:
                continue
            else:
                print 'Trying...'
            try:
                img = self.grabber(id, **self.image_specs)
                imgs.append(img.aoi(bbox=bbox.bounds))
                recs_retrieved.append(record)
                print 'Retrieved ID {}'.format(id)
            except Exception as e:
                print 'Exception: {}'.format(e)
                pass
        print 'Found {} images of {} requested.'.format(len(imgs),
                                                        self.N_images)
        return imgs, recs_retrieved
        
    def build_filters(self):
        """Build filters to search DG catalog."""
        filters = []
        if self.params['clouds'] is not None:
            cloudcover = 'cloudCover < {:d}'.format(
                int(self.params['clouds']))
            filters.append(cloudcover)
        if self.params['offNadirAngle'] is not None:
            relation, angle = params['offNadirAngle']
            offNadir = 'offNadirAngle {} {}'.format(relation, angle)
            filters.append(offNadir)
        if self.image_source == 'Landsat8':
            sensors = "(sensorPlatformName = 'LANDSAT08')"
        else:
            sensors = ("(sensorPlatformName = 'WORLDVIEW02' OR " +
                    "sensorPlatformName = 'WORLDVIEW03_VNIR' OR " +
                    "sensorPlatformName = 'GEOEYE01')")
        filters.append(sensors)
        return filters
        
    def build_grabber(self):
        """Return an appropriate DG image_class object."""
        if self.image_source == 'Landsat8':
            grabber = gbdxtools.LandsatImage           
        else:
            grabber = gbdxtools.CatalogImage
        return grabber
            
    def search_catalog(self):
        """Search the DG catalog for relevant imagery."""
        cat = gbdxtools.catalog.Catalog()
        records = cat.search_point(
            self.latlon.y, self.latlon.x,
            filters=self.catalog_filters,
            startDate=self.params['startDate'],
            endDate=self.params['endDate'])
        return records

def guess_resolution(bbox):
    """Guess a resolution given bbox and SCALE parameters.

    Returns: An image_source and a proposal for pansharpening
    """
    size = np.max(get_side_distances(bbox))
    pansharpen = False
    if size <= MID_SCALE:
        image_source = 'WV'
        if size <= SMALL_SCALE:
            pansharpen = True
    else:
        image_source = 'Landsat8'
    return image_source, pansharpen

def get_side_distances(bbox):
    """Determine width and height of bbox in km, given coords in lat/lon."""
    lon, lat = bbox.centroid.coords[:][0]
    x_coords, y_coords = bbox.boundary.coords.xy
    deltalon = np.max(x_coords) - np.min(x_coords)
    deltalat = np.max(y_coords) - np.min(y_coords)
    deltax = dist_from_longitude(deltalon, lat)
    deltay = dist_from_latitude(deltalat)
    return deltax, deltay

def make_bbox(lat, lon, deltalat, deltalon):
    """Return a bounding box centered on given latitude/longitude.

    Returns:  a shapely Polygon.
    """
    bbox = [lon-deltalon/2., lat-deltalat/2.,
                         lon+deltalon/2., lat+deltalat/2.]
    return geometry.box(*bbox)

# TODO: given a generic region (could be geojson), create
# the bounding box. for geojson first: poly = geometry.asShape(geojson).
# the for a shapely object do: object.bounds.

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


