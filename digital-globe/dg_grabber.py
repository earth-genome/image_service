"""Class structure to automate searching and downloadling from DG catalog.

API ref: http://gbdxtools.readthedocs.io/en/latest/index.html

Class DGImageGrabber: A class to grab an image respecting given specs.

    Attributes:
        bbox:  a shapely box, with (x,y) coordinates (lon, lat)
        latlon: centroid of bbox
        image_source:  DG catalog source and sensor(s)
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

image_source is from ('WV', 'DG-Legacy', 'Landsat8'), and maybe eventually
also 'TMS'. This refers both to a DG image class ('WV', 'DG-Legacy' ~
CatalogImage, 'Landsat8' ~ LandsatImage, future 'TMS' ~ TmsImage), and in
the case of  'WV', 'DG-Legacy', the assignment entails also assumptions
about particular satellite  sensors expressed below in build_filters().
If image_source is None, guess_resolution() is called to make a best
guess based on bbox size and hard-coded SCALE parameters.  

See below also for functions to convert distances to lat/lon, 
to create bounding boxes given various inputs, and to write images to disk.
"""

import numpy as np
from shapely import geometry, wkt
import matplotlib.pyplot as plt
import gbdxtools


# Conversion for latitudes:
KM_PER_DEGREE = 111

# Default catalog and image parameters:
CATALOG_PARAMS = {
    'clouds': 10,   # max percentage allowed cloud cover
    'offNadirAngle': None,  # (relation, angle), e.g. ('<', 10)
    'startDate': '2008-09-01T00:00:00.0000Z',
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
        min_intersect: minimum percent area overlap of bbox & image
        params: catalog search and image parameters
        catalog_filters: catalog params in DG format
        image_specs:  image parameters
        grabber: DG image grabbing class object 

    External methods:
        __call__: Search the database for available image(s).
            Returns: Dict of catalog record(s) and Dask object(s) for the
                area defined by bbox, with options to write to disk.
        search_catalog:
            Returns a list of relevant records.
    """

    def __init__(self, bbox, image_source=None, min_intersect=.9, **params):
        self.bbox = bbox
        self.latlon = self.bbox.centroid
        if image_source is None:
            self.image_source, params['pansharpen'] = guess_resolution(bbox)
        else:
            self.image_source = image_source
        self.min_intersect = min_intersect
        self.params = DEFAULT_PARAMS.copy()
        if params != {}:
            self.params.update(params)
        self.catalog_filters = self.build_filters()
        self.image_specs = {
            k:v for k,v in self.params.items() if k in IMAGE_SPECS.keys()
        }
        self.grabber = self.build_grabber()

    def __call__(self, bbox=None, N_images=3,
                 write_styles=None, file_header=''):
        """Grab most recent available images satifying instance parameters.

        Arguments:
            bbox: a possible new bbox to use in lieu of self.bbox
            N_images: number of images to retrieve
            write_styles: list of possible output image styles, from:
                'DRA' (Dynamical Range Adjusted RGB PNG)
                'Raw' (Raw RGB PNG)
            file_header: optional prefix for output image files

        Returns: List of images (areas of interest) as Dask objects,
            list of corresponding catalog records
        """
        if bbox is None:
            bbox = self.bbox
        records = self.search_catalog()
        records_by_date = sorted(records,
                                key=lambda t: t['properties']['timestamp'])
        imgs, recs_retrieved = [], []
        while len(records_by_date) > 0 and len(imgs) < N_images:
            record = records_by_date.pop()
            id = record['identifier']
            footprint = wkt.loads(record['properties']['footprintWkt'])
            intersection = bbox.intersection(footprint)
            intersect_frac = intersection.area/bbox.area
            print('Catalog ID {}:'.format(id))
            print('Timestamp: {}, Sensor: {}'.format(
                record['properties']['timestamp'],
                record['properties']['sensorPlatformName']))
            print('Percent area intersecting bounding box: {:.2f}'.format(
                intersect_frac))
            if intersect_frac < self.min_intersect:
                continue
            else:
                print('Trying...')
            try:
                img = self.grabber(id, **self.image_specs)
                imgs.append(img.aoi(bbox=intersection.bounds))
                recs_retrieved.append(record)
                print('Retrieved ID {}'.format(id))
            except Exception as e:
                print('Exception: {}'.format(e))
                pass
        print('Found {} images of {} requested.'.format(len(imgs),
                                                        N_images))
        if len(imgs) > 0 and write_styles is not None:
            filenames = build_filenames(bbox, recs_retrieved, file_header)
            for img, filename in zip(imgs, filenames):
                for style in write_styles:
                    self.write_img(img, filename, style=style)
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
        elif self.image_source == 'DG-Legacy':
            sensors = ("(sensorPlatformName = 'QUICKBIRD02' OR " +
                    "sensorPlatformName = 'IKONOS')")
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

    def write_img(self, img, filename, style='DRA'):
        """Write a DG dask image to file with given filename.
                              
        Input style: 'DRA' or 'Raw' 
        """
        if style == 'DRA':
            rgb = img.rgb()
        elif style == 'Raw':
            multispec = img.read()
            num_bands, rows, cols = multispec.shape
            rgb = np.zeros((rows, cols, 3))
            # Guesses based on typical DG band structures:
            if num_bands == 4:
                bands = [2, 1, 0]
            elif num_bands == 8:
                bands = [4, 2, 1]
            else:
                print('{}-band format not recognized. '.format(bands) +
                          'No file written.\n')
                return
            for n, b in enumerate(bands):
                rgb[:,:,n] = multispec[b,:,:]
            # TODO: determine from DG the correct bit range
            print('Unexpected bit range: {:d}-{:d}. '.format(
                    int(np.min(rgb)), int(np.max(rgb))) +
                    'Rescaling by max pixel value.') 
            rgb = rgb/np.max(rgb)
        filename += style + '.png'
        print('\nSaving to {}\n'.format(filename))
        plt.imsave(filename, rgb)
        return
    
# TODO: Figure out DG img.geotiff method and add support to the above.
# (currently the method returns good geotiff headers but uint16 tifs
# with all values very close to zero).  A la: 
"""
        elif style == 'geotiff':
            bands = ms.shape[0]
            outfile = outfile + '.tif'
            # Guesses based on typical DG band structures:
            if bands == 4:
                ms.geotiff(path=outfile, proj=self.proj, bands=[2,1,0])
            elif bands == 8:
                ms.geotiff(path=outfile, proj=self.proj, bands=[4,2,1])
            else:
                print('{}-band format not recognized.'.format(bands) +
                        ' No file written.\n')
"""
        
def build_filenames(bbox, records, file_header=''):
    """Build a filename for image output.

    Uses: catalog id and date, centroid lat/lon, and optional file_header

    Return: filename prefix, ready to append '.png', '.tif', etc.
    """
    lon, lat = bbox.centroid.coords[:][0]
    size = np.max(get_side_distances(bbox))
    tags = ('_lat{:.4f}lon{:.4f}size{:.1f}km'.format(lat, lon, size))
    filenames = [(file_header + r['identifier'] + '_' +
                   r['properties']['timestamp'] + tags) for r in records]
    return filenames
        

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
        image_source = 'DG-Legacy'
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

def bbox_from_scale(lat, lon, scale):
    """Make a bounding box given lat/lon and scale in km."""
    bbox = make_bbox(lat, lon, latitude_from_dist(scale),
                     longitude_from_dist(scale, lat))
    return bbox

def square_bbox_from_scale(lat, lon, scale):
    """Make a bounding box given lat/lon and scale in km.

    This routine reverses the compression in latitude from geoprojection
    by increasing the increment in latitude by 1/cos(lat).
    """
    deltalat = latitude_from_dist(scale)/np.cos(np.radians(np.abs(lat)))
    deltalon = longitude_from_dist(scale, lat)
    bbox = make_bbox(lat, lon, deltalat, deltalon)
    return bbox

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


