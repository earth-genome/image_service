""" Routine to download an image from the DG catalog given lat/lon and scale.

API ref: http://gbdxtools.readthedocs.io/en/latest/index.html

Class DBImageGrabber: A class to grab an image respecting given specs.

    Attributes:
        lat, lon: latitude and longitude of image center
        scale: half size of image (width or height) in km
        clouds: maximum allowed cloud cover
        startDate, endDate: beginning/end dates for catalog search

    External methods:
        __call__: Search the database for most recent available image.
            Returns: Catalog record and Dask object for area of interest
        search_catalog:
            Returns a list of relevant records.

Assumed image specs:  Atmosphere compensated, multispectral, pansharpened
   if scale < SMALL_SCALE.
   For scale < MID_SCALE, DG imagery is sourced; otherwise Landsat8.  

Usage via main:
    python grab_draft.py 38.8977 -77.0365 -s .5
    Outputs: PNG image of bounding box of 

For command line syntax with additional options:
python grab_draft.py -h


"""

import sys
import numpy as np
import matplotlib.pyplot as plt
import argparse

import gbdxtools 

# Image size thresholds (half width or height) in km:
SMALL_SCALE = .6 
MID_SCALE = 3.0  

# Conversion for latitudes:
KM_PER_DEGREE = 111

class DGImageGrabber(object):
    
    """Class DGImageGrabber: Tool to grab a DG image respecting given specs.

    Attributes:
        lat, lon: latitude and longitude of image center
        scale: size of image (width or height) in km
        clouds: maximum allowed cloud cover
        startDate, endDate: beginning/end dates for catalog search

    External methods:
        __call__: Search the database for most recent available image.
            Returns: Catalog record and Dask object for area of interest 
        search_catalog:
            Returns a list of relevant records.
    """

    def __init__(self, lat, lon, scale=1.0, clouds=10,
                 startDate='2016-01-01T09:51:36.0000Z', endDate=None):
        
        self.lat = lat
        self.lon = lon
        self.scale = scale
        self.clouds = clouds
        self.startDate = startDate
        self.endDate = endDate
        self.filters = self.build_filters()
        self.bbox = make_bbox(lat, lon,
                              dist_to_latitude(scale),
                              dist_to_longitude(scale,lat))

        # image_class from CatalogImage, LandsatImage (&eventual TmsImage?)
        if self.scale <= MID_SCALE:
            self.grabber = gbdxtools.CatalogImage
        else:
            self.grabber = gbdxtools.LandsatImage
        self.grabber_params = self.build_grabber_params()

    def __call__(self):
        """Grab most recent available image satifying instance parameters.

        Returns:  Catalog record, image as Dask object
        """
        records = self.search_catalog()
        records_by_date = sorted(records,
                                key=lambda t: t['properties']['timestamp'])
        record, img = None, None
        while len(records_by_date) > 0:
            # TODO: check bbox against footprintWkt
            record = records_by_date.pop()
            id = record['identifier']
            print 'Trying Catalog ID {}:'.format(id)
            print 'Timestamp: {}, Sensor: {}'.format(
                record['properties']['timestamp'],
                record['properties']['sensorPlatformName']
            )
            try:
                img = self.grabber(id, **self.grabber_params)
                break
            except Exception as e:
                print 'Exception: {}'.format(e)
                pass 
        return record, img
        
    def build_filters(self):
        """Build filters to search DG catalog."""
        cloudcover = 'cloudCover < {:d}'.format(int(self.clouds))
        if self.scale <= MID_SCALE:
            sensors = ("(sensorPlatformName = 'WORLDVIEW02' OR " +
                    "sensorPlatformName = 'WORLDVIEW03_VNIR' OR " +
                    "sensorPlatformName = 'GEOEYE01')")
        else:
            sensors = "(sensorPlatformName = 'LANDSAT08')"
        # possible for future:
        # offNadirAngle = xx
        return [cloudcover, sensors]

    def build_grabber_params(self):
        """Format parameters for call to DG image_class object."""
        params = {}
        if self.scale <= SMALL_SCALE:
            params['pansharpen'] = True
        else:
            params['pansharpen'] = False
        params['acomp'] = True
        # future: add projection or orthorectification (how)?
        return params
            
    def search_catalog(self):
        """Search the DG catalog for relevant imagery."""
        cat = gbdxtools.catalog.Catalog()
        results = cat.search_point(
            self.lat, self.lon,
            filters=self.filters,
            startDate=self.startDate,
            endDate=self.endDate)
        return results

def make_bbox(lat, lon, deltalat, deltalon):
    """Return a bounding box centered on given latitude/longitude."""
    return [lon-deltalon, lat-deltalat, lon+deltalon, lat+deltalat]

def dist_to_latitude(dist):
    """Convert a ground distance to decimal degrees latitude."""
    return float(dist)/KM_PER_DEGREE

def dist_to_longitude(dist, latitude):
    """Convert a ground distance to decimal degrees longitude."""
    return float(dist)/(np.cos(np.radians(latitude))*KM_PER_DEGREE)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Grab an image from GBDX.'
    )
    parser.add_argument(
        'lat',
        type=float,
        help='Latitude: {}'.format('38.8977')
    )
    parser.add_argument(
        'lon',
        type=float,
        help='Longitude: {}'.format('-77.0365')
    ) 
    parser.add_argument(
        '-s', '--scale',
        type=float,
        default=1.0,
        help='Approx. image size (half width or height) in km, ' +
            'e.g.: {}'.format(1.0)
    )
    parser.add_argument(
        '-c', '--clouds',
        type=int,
        default=10,
        help='Maximum percentage cloud cover, e.g.: {}'.format(10)
    )
    parser.add_argument(
        '-sd', '--startDate',
        type=str,
        default='2016-01-01T09:51:36.0000Z',
        help='Isoformat start date for image search: {}'.format(
            '2016-01-01T09:51:36.0000Z')
    )
    parser.add_argument(
        '-ed', '--endDate',
        type=str,
        default=None,
        help='Isoformat end date for image search: {}'.format(
            '2018-01-01T09:51:36.0000Z')
    )
    args = vars(parser.parse_args())
    lat = args.pop('lat')
    lon = args.pop('lon')
    grabber = DGImageGrabber(lat, lon, **args)
    record, img = grabber()
    if img is None:
        print ('No image found. Try expanding the date range or ' +
               'change the scale to access a different image source.')
        sys.exit(1)
    aoi = img.aoi(bbox=grabber.bbox)
    rgb = aoi.rgb()  # alternately, for raw multispectrum: raw = aoi.read()
    outfile = (record['identifier'] + '_' +
                record['properties']['timestamp'] +
                '_lat{:.4f}lon{:.4f}size{:.2f}km'.format(
                grabber.lat, grabber.lon, grabber.scale) + '.png')
    print 'Record:\n{}'.format(record)
    print '\nSaving to {}'.format(outfile)
    plt.imsave(outfile, rgb)

