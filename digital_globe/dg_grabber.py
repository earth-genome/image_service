"""Class structure to automate searching and downloadling from DG catalog.

API ref: http://gbdxtools.readthedocs.io/en/latest/index.html

Class DGImageGrabber: A class to grab an image respecting given specs.

    External methods:
        __call__: Grab most recent available images consistent with specs.
        search_catalog
        search_clean

External function:
    write_img: Write a DG Dask image to file.

Usage with default specs:
> bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
> g = DGImageGrabber()
> g(bbox, N_images=1, write_styles=['GeoTiff'], file_header='SanFrancisco')


Catalog and image specs have defaults set in dg_default_specs.json, which,
as of writing, takes form:
{
    "clouds": 10,   # maximum allowed percentage cloud cover
    "offNadirAngle": null,   # (relation, angle), e.g. ('<', 10)
    "startDate": "2008-09-01T00:00:00.0000Z",  # for catalog search
    "endDate": null,  # for catalog search
    "band_type": "MS",  # mulit-spectral
    "acomp": false,
    "proj": "EPSG:4326",
    "min_intersect": 0.9,  # min fractional overlap between bbox and scene
    "image_source": 'WV',
    "pansharpen": null,
    "pansharp_scale": 3.0  # in km; used by _check_highres(), which sets
        pansharpen=True below this scale
}
            
Further notes on image_source and pansharpening:

image_source is from ('WV', 'DG-Legacy'). This refers both to a DG image
class ('WV', 'DG-Legacy' ~ CatalogImage), and in
the case of  'WV', 'DG-Legacy', the assignment entails also assumptions
about particular satellite sensors expressed below in _build_filters().

parameter pansharpen can take values None, False or True.  If None,
_allow_highres() is called to determine whether pansharpen should
be True or False according to whether image is smaller or larger than
pansharp_scale.

"""

import datetime
import json
import os
import subprocess
import sys

import dateutil
import matplotlib.pyplot as plt
import numpy as np
from shapely import wkt
import tifffile
import gbdxtools  # bug in geo libraries.  import this *after* shapely

from geobox import geobox
from postprocessing import color

# Default catalog and image parameters:
DEFAULT_SPECS_FILE = os.path.join(os.path.dirname(__file__),
                                  'dg_default_specs.json')
with open(DEFAULT_SPECS_FILE, 'r') as f:
    DEFAULT_SPECS = json.load(f)

class DGImageGrabber(object):
    
    """Class DGImageGrabber: Tool to grab a DG image respecting given specs.

    Attributes:
        specs: dict of catalog and image specs (see above for format and
           defaults)
        filters: DG-formatted specs for catalog search

    External methods:
        __call__: Grab most recent available images consistent with specs.
        search_catalog:  Find relevant image records.
        search_clean: Find and return streamlined image records.
    """

    def __init__(self, **specs):
        self.specs = DEFAULT_SPECS.copy()
        self.specs.update(specs)
        self.specs = _enforce_date_formatting(**self.specs)
        self.filters = self._build_filters(**self.specs)

    def __call__(self, bbox, N_images=2, write_styles=None, file_header=''):
        """Grab most recent available images consistent with specs.

        Arguments:
            bbox: a shapely box
            N_images: number of images to retrieve
            write_styles: list of possible output image styles, from:
                'DRA' (Dynamical Range Adjusted RGB PNG)
                'GeoTiff' 
            file_header: optional prefix for output image files

        Returns: List of images as Dask objects, list of catalog records,
            and list of filenames of written images
        """
        records = self.search_catalog(bbox)
        
        specs = self.specs.copy()
        if specs['pansharpen'] is None:
            specs['pansharpen'] = self._check_highres(bbox)

        imgs, recs_retrieved = [], []
        while len(records) > 0 and len(imgs) < N_images:
            record = records.pop()
            id = record['identifier']
            print('Trying ID {}: '.format(id))
            print('Timestamp: {}, Sensor: {}'.format(
                record['properties']['timestamp'],
                record['properties']['sensorPlatformName']))
            try:
                img = gbdxtools.CatalogImage(id, **specs) 
                footprint = wkt.loads(record['properties']['footprintWkt'])
                intersection = bbox.intersection(footprint)
                imgs.append(img.aoi(bbox=intersection.bounds))
                recs_retrieved.append(record)
                print('Retrieved ID {}'.format(id))
            except Exception as e:
                print('Exception: {}'.format(e))
        print('Found {} images of {} requested.'.format(len(imgs), N_images))

        recs_written = []
        if len(imgs) > 0 and write_styles is not None:
            prefixes = _build_filenames(bbox, recs_retrieved, file_header)
            for img, rec, prefix in zip(imgs, recs_retrieved, prefixes):
                for style in write_styles:
                    path = write_img(img, prefix, style)
                    cleaned = _clean_record(rec)
                    cleaned.update({'path': path})
                    recs_written.append(cleaned)

        return recs_written
    
    def search_catalog(self, bbox):
        """Search the DG catalog for relevant imagery."""
        cat = gbdxtools.catalog.Catalog()
        records = cat.search_point(bbox.centroid.y, bbox.centroid.x,
                                   filters=self.filters,
                                   startDate=self.specs['startDate'],
                                   endDate=self.specs['endDate'])
        print('Initial search found {} records.'.format(len(records))) 
        records = [r for r in records if self._well_overlapped(bbox, r)]
        records.sort(key=lambda r: r['properties']['timestamp'])
        return records

    def search_clean(self, bbox):
        """Search the DG catalog for relevant imagery.

        Returns: streamlined records, as defined in _clean_records
        """
        records = self.search_catalog(bbox)
        return [_clean_record(r) for r in records]
    
    def _build_filters(self, **specs):
        """Build filters to search DG catalog."""
        filters = []
        if specs['clouds']:
            cloudcover = 'cloudCover < {:d}'.format(int(specs['clouds']))
            filters.append(cloudcover)
        if specs['offNadirAngle']:
            relation, angle = specs['offNadirAngle']
            offNadir = 'offNadirAngle {} {}'.format(relation, angle)
            filters.append(offNadir)
        if specs['image_source'] == 'DG-Legacy':
            sensors = ("(sensorPlatformName = 'QUICKBIRD02' OR " +
                    "sensorPlatformName = 'IKONOS')")
        else:
            sensors = ("(sensorPlatformName = 'WORLDVIEW02' OR " +
                    "sensorPlatformName = 'WORLDVIEW03_VNIR' OR " +
                    "sensorPlatformName = 'GEOEYE01')")
        filters.append(sensors)
        return filters

    # Functions to enforce certain specs.

    def _check_highres(self, bbox):
        """Allow highest resolution when bbox smaller than pansharp_scale."""
        size = np.mean(geobox.get_side_distances(bbox))
        return True if size < self.specs['pansharp_scale'] else False

    def _well_overlapped(self, bbox, record):
        """Check whether bbox and record overlap at level min_intersect."""
        footprint = wkt.loads(record['properties']['footprintWkt'])
        intersection = bbox.intersection(footprint)
        intersect_frac = intersection.area/bbox.area
        wo = True if intersect_frac > self.specs['min_intersect'] else False
        if not wo:
            print('Rejectd ID {}: Overlap with bbox {:.1f}%'.format(
                record['properties']['catalogID'],
                100 * intersect_frac))
        return wo

    
def write_img(img, file_prefix, style):
    """Write a DG dask image to file.
                              
    Argument style: 'DRA' or 'GeoTiff' 
    """
    if style.lower() == 'dra':
        rgb = img.rgb()
        filename = file_prefix + 'DRA.png'
        print('\nSaving to {}\n'.format(filename))
        plt.imsave(filename, rgb)
        
    elif style.lower() == 'geotiff':
        bands = img.shape[0]
        filename = file_prefix + '.tif'
        print('\nSaving to {}\n'.format(filename))
        if bands == 4:
            img.geotiff(path=filename,
                        proj=DEFAULT_SPECS['proj'],
                        bands=[2,1,0])
        elif bands == 8:
            img.geotiff(path=filename,
                        proj=DEFAULT_SPECS['proj'],
                        bands=[4,2,1])
        else:
            print('Image file format not recognized. No image written.\n')
            return None

    else:
        print('Write style must be DRA or GeoTiff. No image written.\n')
        return None
        
    return filename


# DG-specific formatting functions

def _enforce_date_formatting(**specs):
    """Ensure dates are given in DG-required format.
    
    The required format is a string with separator 'T' and timezone
        specifier Z: 'YYYY-MM-DDTHH:MM:SS.XXXXZ'
    """
    for date in ('startDate', 'endDate'):
        if specs[date]: 
            parsed = dateutil.parser.parse(specs[date])
            formatted = parsed.isoformat(timespec='milliseconds')
            formatted = formatted.split('+')[0] + 'Z'
            specs[date] = formatted
    return specs

def _clean_record(record):
    """Streamline DG image record."""
    goodtags = ['catalogID', 'cloudCover', 'sensorPlatformName',
                'timestamp', 'vendor', 'browseURL']
    return {k:v for k,v in record['properties'].items() if k in goodtags}
    
def _build_filenames(bbox, records, file_header=''):
    """Build a filename for image output.

    Uses: catalog id and date, centroid lat/lon, and optional file_header

    Return: filename prefix, ready to append '.png', '.tif', etc.
    """
    tags = ('bbox{:.4f}_{:.4f}_{:.4f}_{:.4f}'.format(
        *bbox.bounds))
    filenames = [(file_header + r['identifier'] + '_' +
                   r['properties']['timestamp'] + tags) for r in records]
    return filenames




