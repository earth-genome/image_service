"""Class structure to automate searching and downloadling from DG catalog.
API ref: http://gbdxtools.readthedocs.io/en/latest/index.html
Class DGImageGrabber: A class to grab an image respecting given specs.
    Attributes:
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
Usage example:
> bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
> g = DGImageGrabber(image_source='WV', pansharpen=True)
> g(bbox, N_images=1, write_styles=['DRA'], file_header='SanFrancisco')
"""

import numpy as np
import sys

import gbdxtools
from shapely import wkt
import matplotlib.pyplot as plt

sys.path.append('../')
from geobox import geobox

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

    def __init__(self, image_source=None, min_intersect=.9, **params):
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

    def __call__(self, bbox, N_images=2, write_styles=None, file_header=''):
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
        if self.image_source is None:
            self.image_source, pansharpen = guess_resolution(bbox)
            self.image_specs.update({'pansharpen': pansharpen})
        lat, lon = bbox.centroid.y, bbox.centroid.x
        records = self.search_catalog(lat, lon)
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
        # reset initialized value
        self.image_specs.update({'pansharpen': self.params['pansharpen']})
        return imgs, recs_retrieved, records_by_date
        
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
            
    def search_catalog(self, lat, lon):
        """Search the DG catalog for relevant imagery."""
        cat = gbdxtools.catalog.Catalog()
        records = cat.search_point(
            lat, lon,
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
            # TODO: Currently it seems like the bulk (say at 95% cutoff)
            # of the multispectral histograms fall in 12-bit range,
            # though WV2,3 datasheets specificy 11-bits.
            # Assume 2**12 = 4096 as max value and reset outliers to 1.0.
            PIXEL_MAX = 4096
            print('\nRescaling raw pixel values assuming a {}-bit '.format(
                int(np.log2(PIXEL_MAX))) + 'range. Clipping outliers.')
            ninetyfifth = np.percentile(rgb, 95)
            print('95th percentile of the image histogram ' +
                      'has pixel value {}. '.format(int(ninetyfifth)) +
                      'Cutting histogram at value {}.'.format(PIXEL_MAX))
            rgb = rgb/PIXEL_MAX
            rgb[np.where(rgb > 1)] = 1.0
        filename += style + '.png'
        print('\nSaving to {}\n'.format(filename))
        plt.imsave(filename, rgb)
        return