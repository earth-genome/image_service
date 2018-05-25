"""Class structure to automate searching and downloadling from DG catalog.

API ref: http://gbdxtools.readthedocs.io/en/latest/index.html

Class DGImageGrabber: A class to grab an image respecting given specs.

    External methods:
        __call__: Grab most recent available images consistent with specs.
        grab_id:  Grab and write image for a known catalogID.
        search:  Given a boundingbox, search for relevant image records.
        search_clean: Search and return streamlined image records.
        search_latlon:  Given lat, lon, search for relevant image records.
        search_latlon_clean:  Search and return streamlined image records.
        search_id: Retrieve catalog record for input catalogID.
        retrieve:  Retrieve dask images objects.
        write_img:  Write a dask image to file.

Usage with default specs (defaults except for N_images, write_styles):
> bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
> g = DGImageGrabber()
> g(bbox, N_images=3, write_styles=['matte', 'contrast'],
    file_header='SanFrancisco')

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
    "image_source": [
	    "WORLDVIEW02",
	    "WORLDVIEW03_VNIR",
	    "GEOEYE01"
    ],
    "pansharpen": null,
    "pansharp_scale": 2.5  # in km; used by _check_highres(), which sets
        pansharpen=True below this scale if pansharpen is None
    "write_styles": []
}
            
The parameter image_source is from
['WORLDVIEW02, 'WORLDVIEW03_VNIR', 'GEOEYE01', QUICKBIRD02', 'IKONOS'].
The first three are are fairly comparable in resolution
(.3-.5 meters/pixel if pansharpened) and are currently active.
The latter two have resolution roughly half that and we decomissioned in 2015.

The parameter pansharpen can take values None, False or True.  If None,
_allow_highres() is called to determine whether pansharpen should
be True or False according to whether image is smaller or larger than
pansharp_scale.

A number of idiosyncrasies of the code, including use of asyncio, are
applied to mirror the syntax of planet_grabber.

"""

import asyncio
import datetime
import json
import os
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

    External methods:
        __call__:  Scheduling wrapper for async execution of grab().
        async grab: Grab most recent available images consistent with specs.
        async grab_by_id:  Grab and write image for a known catalogID.
        prep_scenes: Search and collect dask images and their records.
        async grab_scene: Download and reprocess scene assets.
        search:  Given a boundingbox, search for relevant image records.
        search_clean: Search and return streamlined image records.
        search_latlon:  Given lat, lon, search for relevant image records.
        search_latlon_clean:  Search and return streamlined image records.
        search_id: Retrieve catalog record for input catalogID.
        retrieve:  Retrieve dask images objects.
        write_img:  Write a dask image to file.
    """

    def __init__(self, **specs):
        self.specs = DEFAULT_SPECS.copy()
        self.specs.update(specs)
        self._search_filters = _build_search_filters(**self.specs)
        self._catalog = gbdxtools.catalog.Catalog()

    def __call__(self, bbox, file_header='', **grab_specs):
        """Scheduling wrapper for async execution of grab()."""
        loop = asyncio.get_event_loop()
        recs_written = loop.run_until_complete(asyncio.ensure_future(
            self.grab(bbox, file_header=file_header, **grab_specs)))
        return recs_written


    async def grab(self, bbox, file_header='', **grab_specs):
        """Grab the most recent available images consistent with specs.

        Arguments:
            bbox: a shapely box
            file_header: optional prefix for output image files
            grab_specs: to override certain elements of self.specs, possibly:
                N_images: number of images to retrieve
                write_styles: list of possible output image styles, from:
                    'DGDRA' (DG Dynamical Range Adjusted RGB PNG), 
                    styles defined in color.STYLES
            
        Returns: List of records of written images
        """
        scenes = self.prep_scenes(bbox, **grab_specs)

        recs_written = []
        for scene in scenes:
            written = await self.grab_scene(bbox, scene, file_header,
                                            **grab_specs)
            recs_written.append(written)
            
        return recs_written

    def prep_scenes(self, bbox, **grab_specs):
        """Search and collect available dask images and their records."""
        specs = self.specs.copy()
        specs.update(**grab_specs)
        records = self.search(bbox)[::-1]
        daskimgs, recs_retrieved = self.retrieve(bbox, records, **specs)
        return zip(daskimgs, recs_retrieved)

    async def grab_scene(self, bbox, scene, file_header, **grab_specs):
        """Download and reprocess scene assets."""
        specs = self.specs.copy()
        specs.update(**grab_specs)
        written = self.download(bbox, *scene, file_header, **specs)
        return written
    
    def grab_by_id(self, bbox, catalogID, *args, file_header='', **grab_specs):
        """Grab and write image for a known catalogID."""
        specs = self.specs.copy()
        specs.update(**grab_specs)
        
        record = self.search_id(catalogID)
        daskimgs, _ = self.retrieve(bbox, [record], **specs)
        if not daskimgs:
            raise Exception('Catolog entry for id {} not returned.'.format(
                catalogID))
        written = self.download(bbox, daskimgs[0], record, file_header,
                                **specs)
        return written
        
    def search(self, bbox):
        """Search the catalog for relevant imagery."""
        startDate, endDate = _enforce_date_formatting(**self.specs)
        records = self._catalog.search(searchAreaWkt=bbox.wkt,
                             filters=self._search_filters,
                             startDate=startDate,
                             endDate=endDate)
        records = [r for r in records if self._well_overlapped(bbox, r)]
        records.sort(key=lambda r: r['properties']['timestamp'], reverse=True)
        print('Search found {} records.'.format(len(records)), flush=True) 
        return records

    def search_latlon(self, lat, lon):
        """Search the catalog for relevant imagery."""
        startDate, endDate = _enforce_date_formatting(**self.specs)
        records = self._catalog.search_point(lat, lon,
                                   filters=self._search_filters,
                                   startDate=startDate,
                                   endDate=endDate)
        records.sort(key=lambda r: r['properties']['timestamp'], reverse=True)
        return records

    def search_id(self, catalogID, *args):
        """Retrieve catalog record for input catalogID."""
        return self._catalog.get(catalogID)

    def search_clean(self, bbox, N_records=10):
        """Search the catalog for relevant imagery.

        Returns: streamlined records, as defined in _clean_records()
        """
        records = self.search(bbox)
        return [_clean_record(r) for r in records[:N_records]]

    def search_latlon_clean(self, lat, lon, N_records=10):
        """Search the catalog for relevant imagery.

        Returns: streamlined records, as defined in _clean_records()
        """
        records = self.search_latlon(lat, lon)
        return [_clean_record(r) for r in records[:N_records]]

    def retrieve(self, bbox, records, N_images=1, **specs):
        """Retrieve dask images from the catalog.

        Arugment records:  DG catalog records for the sought images.

        Returns:  Lists of the dask image objects and the associaed records.
        """
        if specs['pansharpen'] is None:
            specs['pansharpen'] = self._check_highres(bbox)
            
        daskimgs, recs_retrieved = [], []
        while len(records) > 0 and len(daskimgs) < N_images:
            record = records.pop()
            catalogID, props = record['identifier'], record['properties']
            print('Trying ID {}:\n {}, {}'.format(
                catalogID, props['timestamp'], props['sensorPlatformName']))
            try:
                daskimg = gbdxtools.CatalogImage(catalogID, **specs) 
                footprint = wkt.loads(props['footprintWkt'])
                intersection = bbox.intersection(footprint)
                daskimgs.append(daskimg.aoi(bbox=intersection.bounds))
                recs_retrieved.append(record)
                print('Retrieved ID {}'.format(catalogID))
            except Exception as e:
                print('Exception: {}'.format(e))
        print('Found {} images of {} requested.'.format(
            len(daskimgs), N_images), flush=True)
        return daskimgs, recs_retrieved

    def download(self, bbox, daskimg, record, file_header, **specs):
        """Download dask image asset and write to disk.

        Returns: Asset record, cleaned and with paths to images added.
        """
        prefix = _build_filename(bbox, record, file_header)
        paths = self.write_img(daskimg, prefix, **specs)
        cleaned = _clean_record(record)
        cleaned.update({'paths': paths})
        return cleaned
    
    def write_img(self, daskimg, file_prefix, write_styles=[], **specs):
        """Write a DG dask image to file.
                              
        Argument write_styles: from 'DGDRA' or styles defined in
            postprocessing.color.  If empty, a raw GeoTiff is written.
                
        Returns: Local paths to images.
        """
        output_paths = []
        styles = [style.lower() for style in write_styles]

        # deprecated: DG color correction 
        if 'dgdra' in styles:
            outpath = write_dg_dra(daskimg, file_prefix)
            output_paths.append(outpath)
            styles.remove('dgdra')
            if not styles:
                return paths

        # grab the raw geotiff
        bands = daskimg.shape[0]
        path = file_prefix + '.tif'
        print('\nStaging at {}\n'.format(path), flush=True)
        if bands == 4:
            daskimg.geotiff(path=path, bands=[2,1,0], **specs)
        elif bands == 8:
            daskimg.geotiff(path=path, bands=[4,2,1], **specs)
        output_paths.append(path)

        def correct_and_write(img, path, style):
            """Correct color and write to file."""
            corrected = color.STYLES[style](img)
            outpath = path.split('.tif')[0] + '-' + style + '.png'
            print('\nStaging at {}\n'.format(outpath), flush=True)
            plt.imsave(outpath, corrected)
            return outpath
        
        img = color.coarse_adjust(tifffile.imread(path))
        for style in styles:
            if style in color.STYLES.keys():
                output_paths.append(correct_and_write(img, path, style))
    
        return output_paths

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
                record['properties']['catalogID'], 100 * intersect_frac))
        return wo

# deprecated image writing: 

def write_dg_dra(daskimg, file_prefix):
    """Write an image using the DG rgb() method (DG's DRA routines)."""
    rgb = daskimg.rgb()
    filename = file_prefix + 'DGDRA.png'
    print('\nSaving to {}\n'.format(filename))
    plt.imsave(filename, rgb)
    return filename

# DG-specific formatting functions

def _enforce_date_formatting(**specs):
    """Ensure dates are given in DG-required format.
    
    The required format is a string with separator 'T' and timezone
        specifier Z: 'YYYY-MM-DDTHH:MM:SS.XXXXZ'
    """
    dates = []
    for date in ('startDate', 'endDate'):
        if specs[date]: 
            parsed = dateutil.parser.parse(specs[date])
            formatted = parsed.isoformat(timespec='milliseconds')
            formatted = formatted.split('+')[0] + 'Z'
            dates.append(formatted)
        else:
            dates.append(None)
    return dates

def _build_search_filters(**specs):
    """Build filters to search catalog."""
    sensors = "(" + " OR ".join(["sensorPlatformName = '{}'".format(source)
                       for source in specs['image_source']]) + ")"
    filters = [sensors]
    filters.append('cloudCover < {:d}'.format(int(specs['clouds'])))
    if specs['offNadirAngle']:
        filters.append('offNadirAngle {} {}'.format(specs['offNadirAngle']))
    return filters

def _clean_record(record):
    """Streamline image record."""
    keymap = {  # maps record keys to our standardized nomenclature
        'vendor': 'provider',
        'sensorPlatformName': 'sensor',
        'catalogID': 'catalogID',
        'timestamp': 'timestamp',
        'cloudCover': 'clouds',
        'panResolution': 'resolution',
        'browseURL': 'thumbnail'
    }
    cleaned = {keymap[k]:v for k,v in record['properties'].items()
               if k in keymap.keys()}
    return cleaned   
    
def _build_filename(bbox, record, file_header=''):
    """Build a filename for image output.

    Uses: catalog id and date, centroid lat/lon, and optional file_header

    Return: filename prefix, ready to append '.png', '.tif', etc.
    """
    tags = ('bbox{:.4f}_{:.4f}_{:.4f}_{:.4f}'.format(*bbox.bounds))
    filename = (file_header + record['identifier'] + '_' +
                record['properties']['timestamp'] + tags)
    return filename




