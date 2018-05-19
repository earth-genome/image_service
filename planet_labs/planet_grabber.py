"""Class structure to automate searching and downloadling from Planet Labs.

API ref: https://planetlabs.github.io/planet-client-python/index.html

Class PlanetGrabber: A class to grab an image respecting given specs.

Usage with default specs (default except for N_images):
> bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
> g = PlanetGrabber()
> g(bbox, N_images=3, file_header='SanFrancisco')


Catalog and image specs have defaults set in planet_default_specs.json, which,
as of writing, takes form:
{
    "clouds": 10,    # maximum allowed percentage cloud cover
    "min_intersect": 0.9,
    "startDate": "2008-09-01T00:00:00.0000Z",    # for catalog search
    "endDate": null,
    "item_types": [
	    "PSScene3Band",
	    "PSOrthoTile",
	    "REOrthoTile",
	    "SkySatScene"
    ],
    "asset_types": [    # We prefer assets in this (reverse) order:
	    "analytic",
	    "ortho_visual",
	    "visual"
    ],
    "write_styles": [
    "matte",
	"contrast",
	"dra",
	"desert"
    ]
}

"""

import asyncio
import json
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
from planet import api
from shapely import geometry
import tifffile

from postprocessing import color
from postprocessing import gdal_routines

# Default catalog and image parameters:
DEFAULT_SPECS_FILE = os.path.join(os.path.dirname(__file__),
                                  'planet_default_specs.json')
with open(DEFAULT_SPECS_FILE, 'r') as f:
    DEFAULT_SPECS = json.load(f)

# For asynchronous handling of scene activation and download, in seconds:
WAITTIME = 15

class PlanetGrabber(object):
    
    """Class PlanetGrabber: Tool to grab a DG image respecting given specs.

    Attributes:
        specs: dict of catalog and image specs (see above for format and
           defaults)

    External methods:
        __call__: Grab most recent available images consistent with specs.
        grab_by_id:  Grab and write image for a known catalogID.
        search:  Given a boundingbox, search for relevant image records.
        search_clean: Search and return streamlined image records.
        search_latlon:  Given lat, lon, search for relevant image records.
        search_latlon_clean:  Search and return streamlined image records.
        search_id: Retrieve catalog record for input catalogID.
        async retrieve_asset: Activate an asset and add its reference to record.
        download: Download an asset.
        reprocess: Reprocess a downloaded image and clean record to return.
    """

    def __init__(self, **specs):
        self.specs = DEFAULT_SPECS.copy()
        self.specs.update(specs)
        self._search_filters = _build_search_filters(**self.specs)
        self._client = api.ClientV1()

    def __call__(self, bbox, file_header='', **grab_specs):
        """Grab the most recent available images consistent with specs.

        Arguments:
            bbox: a shapely box
            file_header: optional prefix for output image files
            grab_specs: to override certain elements of self.specs, possibly:
                N_images: number of images to retrieve
                write_styles: list of possible output image styles
                    (from color.STYLES)
            
        Returns: List of records of written images
        """
        specs = self.specs.copy()
        specs.update(**grab_specs)
            
        records = self.search(bbox)[::-1]
        records = [r for r in records if self._well_overlapped(bbox, r)]

        retrieve_tasks = [
            asyncio.ensure_future(self.retrieve_asset(record))
                for record in records[-specs['N_images']:]
        ]

        async def async_handler(tasks, bbox, file_header, **specs):
            recs_written = []
            for future in asyncio.as_completed(tasks):
                asset, record = await future
                print('Retrieved {}\nDownloading...'.format(record['id']),
                      flush=True)
                path = self.download(asset, file_header)
                written = self.reprocess(bbox, record, path, **specs)
                recs_written.append(written)
            return recs_written

        loop = asyncio.get_event_loop()
        recs_written = loop.run_until_complete(
            async_handler(retrieve_tasks, bbox, file_header, **specs))
        return recs_written

    def grab_by_id(self, bbox, catalogID, item_type, file_header='', **specs):
        """Grab and write image for a known catalogID."""
        record = self.search_id(catalogID, item_type)
        asset, record = self.retrieve_asset(record)
        if not asset:
            raise Exception('Catolog entry for id {} not returned.'.format(
                catalogID))
        path = self.download(asset, file_header)
        written = self.reprocess(bbox, record, path, **specs)
        return written
    
    def search(self, bbox, MAX_RECORDS=2500):
        """Search the catalog for relevant imagery."""
        aoi = geometry.mapping(bbox)
        query = api.filters.and_filter(
            api.filters.geom_filter(aoi), *self._search_filters)
        request = api.filters.build_search_request(query,
            item_types=self.specs['item_types'])
        response = self._client.quick_search(request, sort='acquired desc')
        return list(response.items_iter(limit=MAX_RECORDS))

    def search_latlon(self, lat, lon):
        """Search the catalog for relevant imagery."""
        point = geometry.Point(lon, lat)
        return self.search(point)

    def search_id(self, catalogID, item_type):
        """Retrieve catalog record for input catalogID."""
        response = self._client.get_item(item_type, catalogID)
        return response.get()

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
    
    async def retrieve_asset(self, record):
        """Activate an asset and add its reference to record."""
        assets = self._client.get_assets_by_id(
            record['properties']['item_type'], record['id']).get()
        asset, asset_type = self._activate(assets)
        print('Activating {}: {}'.format(record['id'], asset_type))
        print('This could take several minutes.', flush=True)
        while not self._is_active(asset):
            await asyncio.sleep(WAITTIME)
            assets = self._client.get_assets_by_id(
                record['properties']['item_type'], record['id']).get()
            asset = assets[asset_type]
        record['properties'].update({'asset_type': asset_type})
        return asset, record

    def _activate(self, assets):
        """Activate the best available asset."""
        asset_types = self.specs['asset_types'].copy()
        while asset_types:
            asset_type = asset_types.pop()
            if asset_type in assets.keys():
                asset = assets[asset_type]
                self._client.activate(asset)
                break
        return asset, asset_type
            
    def _is_active(self, asset):
        """Check asset activation status."""
        return True if asset['status'] == 'active' else False

    def download(self, asset, file_header):
        """Download asset and write to filename."""
        body = self._client.download(asset).get_body()
        path = file_header + body.name
        body.write(file=path)
        return path

    def reprocess(self, bbox, record, path, write_styles=[], **specs):
        """Reprocess a downloaded image and clean record to return.

        Depending on asset type, this function:
            crops image to bbox,
            reforms band structure,
            color corrects (perhaps producing mutliple versions of the image),
            cleans and adds image paths to the record.

        Returns: Updated record
        """
        paths = []
        styles = [style.lower() for style in write_styles]
        
        path = gdal_routines.crop(path, bbox)
        path = gdal_routines.reband(path, _get_bandmap(record))
        paths.append(path)
        print('\nStaging at {}\n'.format(path), flush=True)

        def correct_and_write(img, path, style):
            """Correct color and write to file."""
            corrected = color.STYLES[style](img)
            outpath = path.split('LZW.tif')[0] + '-' + style + '.png'
            print('\nStaging at {}\n'.format(outpath), flush=True)
            plt.imsave(outpath, corrected)
            return outpath
            
        img = tifffile.imread(path)
        if (record['properties']['asset_type'] == 'visual' or
            record['properties']['asset_type'] == 'ortho_visual'):

            # add this minimal tweak, since Planet visual is already corrected:
            paths.append(correct_and_write(img, path, 'expanded'))

        for style in styles:
            if style in color.STYLES.keys():
                paths.append(correct_and_write(img, path, style))

        cleaned = _clean_record(record)
        cleaned.update({'paths': paths})
        return cleaned

    # Functions to enforce certain specs.

    def _well_overlapped(self, bbox, record):
        """Check whether bbox and record overlap at level min_intersect."""
        footprint = geometry.asShape(record['geometry'])
        intersection = bbox.intersection(footprint)
        intersect_frac = intersection.area/bbox.area
        wo = True if intersect_frac > self.specs['min_intersect'] else False
        if not wo:
            print('Rejectd ID {}: Overlap with bbox {:.1f}%'.format(
                record['id'], 100 * intersect_frac))
        return wo


# Planet-specific formatting functions

def _build_search_filters(**specs):
    """Build filters to search catalog."""
    sf = [api.filters.range_filter('cloud_cover', lt=specs['clouds']/100)]
    if specs['startDate']:
        sf.append(api.filters.date_range('acquired', gt=specs['startDate']))
    if specs['endDate']:
        sf.append(api.filters.date_range('acquired', lt=specs['endDate']))
    return sf

def _clean_record(record):
    """Streamline image record."""
    keymap = {  # maps record keys to our standardized nomenclature
        'provider': 'provider',
        'item_type': 'item_type',
        'asset_type': 'asset_type',
        'acquired': 'timestamp',
        'cloud_cover': 'clouds',
        'pixel_resolution': 'resolution',
        'gsd': 'gsd'
    }
    cleaned = {'catalogID': record['id']}
    cleaned.update({'thumbnail': record['_links']['thumbnail']})
    cleaned.update({'full_record': record['_links']['_self']})
    cleaned.update({keymap[k]:v for k,v in record['properties'].items()
        if k in keymap.keys()})
    cleaned['clouds'] *= 100
    return cleaned   
    
def _get_bandmap(record):
    """Find the band order for R-G-B bands for the input record."""
    bandmaps = {   
        'PSScene3Band': {
            'visual': (1, 2, 3),
            'analytic': (1, 2, 3)
        },
        'PSOrthoTile': {
            'visual': (1, 2, 3),
            'analytic': (3, 2, 1)
        },
        'REOrthoTile': {
            'visual': (1, 2, 3),
            'analytic': (3, 2, 1)
        },
        'SkySatScene': {
            'ortho_visual': (3, 2, 1)
        }
    }
    item_type = record['properties']['item_type']
    asset_type = record['properties']['asset_type']
    try:
        bands = bandmaps[item_type][asset_type]
    except KeyError as e:
        raise KeyError('{}: Bandmap not defined for {}:{}'.format(
            repr(e), item_type, asset_type))
    return bands



