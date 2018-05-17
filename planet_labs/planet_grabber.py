"""Class structure to automate searching and downloadling from Planet Labs.

API ref: https://planetlabs.github.io/planet-client-python/index.html

Class PlanetGrabber: A class to grab an image respecting given specs.

    External methods:
        __call__: Grab most recent available images consistent with specs.
        search:  Given a boundingbox, search for relevant image records.
        search_clean: Search and return streamlined image records.
        search_latlon:  Given lat, lon, search for relevant image records.
        search_latlon_clean:  Search and return streamlined image records.

Usage with default specs (defaults except for N_images, write_styles):
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
    "write_styles": []
}

"""

import asyncio
import datetime
import json
import os
import subprocess
import sys

import dateutil
import matplotlib.pyplot as plt
import numpy as np
from planet import api
from shapely import geometry
import tifffile

from geobox import geobox
from postprocessing import color

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
        grab_id:  Grab and write image for a known catalogID.
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
        self._client = api.ClientV1()

    def __call__(self, bbox, file_header='', **grab_specs):
        """Grab the most recent available images consistent with specs.

        Arguments:
            bbox: a shapely box
            file_header: optional prefix for output image files
            grab_specs: to override certain elements of self.specs, possibly:
                N_images: number of images to retrieve
                write_styles: list of possible output image styles, from:
                    'DGDRA' (DG Dynamical Range Adjusted RGB PNG)
                    color-corrected styles defined in postprocessing.color
                    (if empty, a raw GeoTiff is written)
            

        Returns: List of records of written images
        """
        specs = self.specs.copy()
        specs.update(**grab_specs)
            
        records = self.search(bbox)[::-1]

        # add function here to check min_intersect / pull all intersecting
        # records w/ roughly the same datetime.
        # then adjust records[-specs['N_images']:] accordingly

        retrieve_tasks = [
            asyncio.ensure_future(
                self.retrieve_asset(record))
            for record in records[-specs['N_images']:]
        ]

        async def async_handler(tasks, bbox, file_header, **specs):
            recs_written = []
            for future in asyncio.as_completed(tasks):
                asset, record = await future
                print('Retrieved {}\nDownloading...'.format(record['id']))
                path = self.download(
                    asset, _build_filename(bbox, record, file_header))
                print('Done.')
                written = self.reprocess(bbox, record, path, **specs)
                recs_written.append(written)
            return recs_written

        loop = asyncio.get_event_loop()
        recs_written = loop.run_until_complete(
            async_handler(retrieve_tasks, bbox, file_header, **specs))
        return recs_written
        
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
        assets = self._client.get_assets_by_id(
            record['properties']['item_type'], record['id']).get()
        asset, asset_type = self._activate(assets)
        # TODO: remove counter / debugging print statements
        counter = 0 
        while not self._is_active(asset):
            await asyncio.sleep(WAITTIME)
            assets = self._client.get_assets_by_id(
                record['properties']['item_type'], record['id']).get()
            asset = assets[asset_type]
            counter += 1
            print('{}th wait cycle on asset:\n{}'.format(counter, asset))
        record.update({'asset_type': asset_type})
        return asset, record

    def _activate(self, assets):
        """Activate the best available asset."""
        asset_types = self.specs['asset_types'].copy()
        while asset_types:
            asset_type = asset_types.pop()
            if asset_type in assets.keys():
                asset = assets[asset_type]
                print('Activating:\n{}'.format(asset))
                client.activate(asset)
                break
        return asset, asset_type
            
    def _is_active(self, asset):
        """Check asset activation status."""
        return True if asset['status'] == 'active' else False

    def download(self, asset, filename):
        """Download asset and write to filename."""
        body = self._client.download(asset).get_body()
        body.write(file=filename)
        return filename

    # WIP.  Need to add, potentially, merging of tiles, cropping,
    # handling for analytic asset_type
    def reprocess(self, bbox, record, path, **specs):
        """Reprocess downloaded images and clean records to return.

        Depending on asset type, this function:
            crops image to bbox,
            reforms band structure,
            color corrects (perhaps producing mutliple versions of the image),
            cleans and adds image paths to the record.

        Returns: Updated record
        """
        if (record['asset_type'] == 'visual' or
            record['asset_type'] == 'ortho_visual'):
            outpath = path.split('.tif')[0] + 'LZW.tif'
            subprocess.call(['gdal_translate', '-co', 'COMPRESS=LZW',
                             '-b', '1', '-b', '2', '-b', '3',
                             path, outpath])
            os.remove(path)
            paths = [outpath]
            
        cleaned = _clean_record(record)
        cleaned.update({'paths': paths})
        return cleaned

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
    
def _build_filename(bbox, record, file_header=''):
    """Build a filename for image output.

    Uses: catalog id and date, centroid lat/lon, and optional file_header

    Return: filename prefix, ready to append '.png', '.tif', etc.
    """
    tags = ('bbox{:.4f}_{:.4f}_{:.4f}_{:.4f}'.format(*bbox.bounds))
    filename = (file_header + record['id'] + '_' +
                record['properties']['acquired'] + tags +
                record['asset_type'] + '.tif')
    return filename




