"""Class structure to automate searching and downloadling from DG catalog.

API ref: http://gbdxtools.readthedocs.io/en/latest/index.html

Class DGImageGrabber: A class to grab an image respecting given specs.

    External methods:
        __call__: Grab most recent available images consistent with specs.
        search_catalog: Find relevant image records.

Usage:
> bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
> g = DGImageGrabber(image_source='WV', pansharpen=True)
> g(bbox, N_images=1, write_styles=['GeoTiff'], file_header='SanFrancisco')


Catalog and image specs have defaults set in dg_default_specs.json, which,
as of writing, takes form:
{
    "clouds": 10,   # maximum allowed percentage cloud cover
    "offNadirAngle": null,   # (relation, angle), e.g. ('<', 10)
    "startDate": "2008-09-01T00:00:00.0000Z",  # for catalog search
    "endDate": null,  # for catalog search
    "band_type": "MS",  # mulit-spectral
    "pansharpen": false,
    "acomp": false,
    "proj": "EPSG:4326",
    "image_source": null,  # see below
    "min_intersect": 0.9,  # min fractional overlap between bbox and scene
    "pansharp_scale": 2.0  # in km; used by guess_resolution(), which sets
        pansharpen=True below this scale
}
            
Notes on image_source:

image_source is from ('WV', 'DG-Legacy'), and maybe eventually
also 'TMS'. This refers both to a DG image class ('WV', 'DG-Legacy' ~
CatalogImage, future 'TMS' ~ TmsImage), and in
the case of  'WV', 'DG-Legacy', the assignment entails also assumptions
about particular satellite sensors expressed below in _build_filters().
If image_source is None, _guess_resolution() is called to make a best
guess based on bbox size and specs['pansharp_scale'].

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

DEFAULT_SOURCE = 'WV'

class DGImageGrabber(object):
    
    """Class DGImageGrabber: Tool to grab a DG image respecting given specs.

    Attributes:
        specs: dict of catalog and image specs (see above for format and
           defaults)

    External methods:
        __call__: Grab most recent available images consistent with specs.
        search_catalog:  Find relevant image records.
    """

    def __init__(self, **specs):
        self.specs = DEFAULT_SPECS.copy()
        self.specs.update(specs)
        self.specs = self._enforce_date_formatting(**self.specs)

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
        specs = self.specs.copy()
        if specs['image_source'] is None:
            image_source, pansharpen = self._guess_resolution(bbox)
            specs.update({
                'image_source': image_source,
                'pansharpen': pansharpen})

        lat, lon = bbox.centroid.y, bbox.centroid.x
        filters = self._build_filters(**specs)
        records = self.search_catalog(lat, lon, filters, **specs)
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
            if intersect_frac < specs['min_intersect']:
                continue
            else:
                print('Trying...')
            try:
                img = gbdxtools.CatalogImage(id, **specs)
                imgs.append(img.aoi(bbox=intersection.bounds))
                recs_retrieved.append(record)
                print('Retrieved ID {}'.format(id))
            except Exception as e:
                print('Exception: {}'.format(e))
                pass
        print('Found {} images of {} requested.'.format(len(imgs), N_images))
        
        if len(imgs) > 0 and write_styles is not None:
            filenames = _build_filenames(bbox, recs_retrieved, file_header)
            written_fnames = []
            for img, filename in zip(imgs, filenames):
                for style in write_styles:
                    write_name = self._write_img(img, filename, style)
                    written_fnames.append(write_name)

        return imgs, recs_retrieved, written_fnames

    def search_catalog(self, lat, lon, filters, **specs):
        """Search the DG catalog for relevant imagery."""
        cat = gbdxtools.catalog.Catalog()
        records = cat.search_point(lat, lon,
                                   filters=filters,
                                   startDate=specs['startDate'],
                                   endDate=specs['endDate'])
        return records
    
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

    def _write_img(self, img, filename, style):
        """Write a DG dask image to file with given filename.
                              
        Argument style: 'DRA' or 'GeoTiff' 
        """
        if style.lower() == 'dra':
            rgb = img.rgb()
            filename += 'DRA.png'
            print('\nSaving to {}\n'.format(filename))
            plt.imsave(filename, rgb)
        
        elif style.lower() == 'geotiff':
            bands = img.shape[0]
            filename += '.tif'
            print('\nSaving to {}\n'.format(filename))
            if bands == 4:
                img.geotiff(path=filename,
                            proj=self.specs['proj'],
                            bands=[2,1,0])
            elif bands == 8:
                img.geotiff(path=filename,
                            proj=self.specs['proj'],
                            bands=[4,2,1])
            else:
                print('Image file format not recognized. No image written.\n')
                return None

        else:
            print('Write style must be DRA or GeoTiff. No image written.\n')
            return None
        
        return filename

    def _guess_resolution(self, bbox):
        """Guess a resolution given bbox.

        Returns: DEFAULT_SOURCE and a proposal for pansharpening
        """
        size = np.max(geobox.get_side_distances(bbox))
        pansharpen = True if size <= self.specs['pansharp_scale'] else False
        return DEFAULT_SOURCE, pansharpen

    def _enforce_date_formatting(self, **specs):
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




