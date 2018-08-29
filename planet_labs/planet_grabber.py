"""Class structure to automate searching and downloadling from Planet Labs.

API ref: https://planetlabs.github.io/planet-client-python/index.html

Class PlanetGrabber: A class to grab an image respecting given specs.

Usage with default specs (default except for N_images):
> bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
> g = PlanetGrabber()
> g(bbox, N_images=3, file_header='SanFrancisco')

Because planet images are relatively small, multiple images often must be
pulled and assembled to cover a bounding box. The minimal processing unit
is therefore denoted a 'scene,' which includes all the images (or their
records) required to produce one final image of the bounding box.  A call
will attempt to produce N_images such scenes.  

Catalog and image specs have defaults set in planet_default_specs.json, which,
as of writing, takes form:
{
    "clouds": 10,    # maximum allowed percentage cloud cover
    "min_intersect": 0.9,
    "startDate": "2008-09-01T00:00:00.0000Z",    # for catalog search
    "endDate": null,
    "N_images": 1,
    "skip_days": 0, # min days between scenes if N_images > 1
    "item_types": [
	    "PSScene3Band",
	    "PSOrthoTile",
	    "REOrthoTile",
	    "SkySatScene"
    ],
    "asset_types": [   
	    "analytic",
	    "ortho_visual",
	    "visual"
    ],
    "write_styles": [
        "matte",
	    "contrast",
	    "dra",
	    "desert"
    ],
    "thumbnails": false
}

"""

import asyncio
import datetime
import json
import os
import sys

import dateutil
import numpy as np
from planet import api
from shapely import geometry
from shapely.ops import cascaded_union
import skimage.io

from postprocessing import color
from postprocessing import gdal_routines
from postprocessing import resample

# Default file for catalog and image parameters:
DEFAULT_SPECS_FILE = os.path.join(os.path.dirname(__file__),
                                  'planet_default_specs.json')

# For asynchronous handling of scene activation and download, in seconds:
WAITTIME = 10

class PlanetGrabber(object):
    
    """Class PlanetGrabber: Tool to grab Planet images respecting given specs.

    Attributes:
        specs: dict of catalog and image specs (see above for format and
           defaults)

    External methods:
        __call__:  Scheduling wrapper for async execution of grab().
        async grab: Grab most recent available images consistent with specs.
        async grab_by_id:  Grab and write image for a known catalogID.
        prep_scenes: Search and group search records into scenes.
        grab_scene: Retrieve, download, and reprocess scene assets.
        search:  Given a boundingbox, search for relevant image records.
        search_clean: Search and return streamlined image records.
        search_latlon:  Given lat, lon, search for relevant image records.
        search_latlon_clean:  Search and return streamlined image records.
        search_id: Retrieve catalog record for input catalogID.
        async retrieve_asset: Activate an asset and add its reference to record.
        download: Download as asset.
        geo_process: Reproject, crop to bbox, extract output bands, merge.
        color_process: Correct color (producing mutliple versions of the image).
    """

    def __init__(self, specs_filename=DEFAULT_SPECS_FILE, **specs):
        with open(specs_filename, 'r') as f:
            self.specs = json.load(f)
        self.specs.update(specs)
        self._search_filters = _build_search_filters(**self.specs)
        self._client = api.ClientV1()

    def __call__(self, bbox, file_header='', **grab_specs):
        """Scheduling wrapper for async execution of grab()."""
        loop = asyncio.get_event_loop()
        recs_written = loop.run_until_complete(asyncio.ensure_future(
            self.grab(bbox, file_header=file_header, **grab_specs)))
        return recs_written

    async def grab(self, bbox, file_header, **grab_specs):
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
        scenes = self.prep_scenes(bbox, **grab_specs)
        grab_tasks = [
            asyncio.ensure_future(
                self.grab_scene(bbox, scene, file_header, **grab_specs))
            for scene in scenes
        ]

        done, _ = await asyncio.wait(grab_tasks)
        recs_written = []
        for task in done:
            try:
                recs_written.append(task.result())
            except Exception as e:
                print('During grab_scene(): {}'.format(repr(e)))
        return recs_written

    def prep_scenes(self, bbox, **grab_specs):
        """Search and group search records into scenes.

        Returns: List of lists of records.  
        """
        specs = self.specs.copy()
        specs.update(**grab_specs)
        records = self.search(bbox)[::-1]
        scenes = self._group_into_scenes(bbox, records, **specs)
        return scenes

    async def grab_scene(self, bbox, scene, file_header, **grab_specs):
        """Retrieve, download, and reprocess scene assets."""
        specs = self.specs.copy()
        specs.update(**grab_specs)
        active_assets = await self._retrieve_for_scene(scene)
        print('Retrieved {}\nDownloading...'.format(
              [r['id'] for r in scene]), flush=True)
        staged_assets = self._download_for_scene(active_assets, file_header)
        written = self._reprocess(bbox, staged_assets, scene, **specs)
        return written

    async def grab_by_id(self, bbox, catalogID, item_type, file_header='',
                   **grab_specs):
        """Grab and write image for a known catalogID."""
        specs = self.specs.copy()
        specs.update(**grab_specs)

        scene = [self.search_id(catalogID, item_type)]
        active_assets = await self.retrieve_assets(catalogID, item_type)
        if not active_assets:
            raise Exception('Catolog entry for id {} not returned.'.format(
                catalogID))

        staged_assets = self._download_for_scene([active_assets], file_header)
        written = self._reprocess(bbox, staged_assets, scene, **specs)
        
        return written
    
    def search(self, bbox, max_records=500):
        """Search the catalog for relevant imagery."""
        aoi = geometry.mapping(bbox)
        query = api.filters.and_filter(
            api.filters.geom_filter(aoi), *self._search_filters)
        request = api.filters.build_search_request(query,
            item_types=self.specs['item_types'])
        response = self._client.quick_search(request, sort='acquired desc')

        # The final iteration over response items is time expensive, ergo
        # max_records.  However, if 'skip_days' is large we will need as many
        # records as possible to fulfill a pull request.
        if self.specs['skip_days']:
            max_records = None
        return list(response.items_iter(limit=max_records))

    def search_latlon(self, lat, lon, max_records=500):
        """Search the catalog for relevant imagery."""
        point = geometry.Point(lon, lat)
        return self.search(point, max_records=max_records)

    def search_id(self, catalogID, item_type):
        """Retrieve catalog record for input catalogID."""
        response = self._client.get_item(item_type, catalogID)
        return response.get()

    def search_clean(self, bbox, N_records=10):
        """Search the catalog for relevant imagery.

        Returns: streamlined records, as defined in _clean_records()
        """
        records = self.search(bbox, max_records=N_records)
        return [_clean_record(r) for r in records]

    def search_latlon_clean(self, lat, lon, N_records=10):
        """Search the catalog for relevant imagery.

        Returns: streamlined records, as defined in _clean_records()
        """
        records = self.search_latlon(lat, lon, max_records=N_records)
        return [_clean_record(r) for r in records]

    async def _retrieve_for_scene(self, scene):
        """Schedule asset retrieval for records in scene.

        Returns: List of dicts of activated assets.
        """
        tasks = [
            self.retrieve_assets(
                record['id'], record['properties']['item_type'])
            for record in scene
        ]
        done, _ = await asyncio.wait(tasks)
        return [task.result() for task in done]
    
    async def retrieve_assets(self, catalogID, item_type):
        """Initiate and monitor asset activation.

        Returns: Dict of activated assets.
        """
        assets = self._client.get_assets_by_id(item_type, catalogID).get()
        activated = self._activate(assets)
        print('Activating {}: {}'.format(catalogID, list(activated.keys())))
        print('This could take several minutes.', flush=True)
        while not self._are_active(list(activated.values())):
            await asyncio.sleep(WAITTIME)
            assets = self._client.get_assets_by_id(item_type, catalogID).get()
            activated = {asset_type: assets[asset_type] for asset_type
                          in activated.keys()}
        return activated

    def _activate(self, assets):
        """Activate assets.

        Returns:  Dict of asset_types and activated asset records.
        """
        activated = {}
        for asset_type in self.specs['asset_types']:
            if asset_type in assets.keys():
                asset = assets[asset_type]
                self._client.activate(asset)
                activated.update({asset_type: asset})
        return activated

    def _are_active(self, assets):
        """Check list of assets for activation status."""
        for asset in assets:
            if asset['status'] != 'active':
                return False
        return True
    
    def _is_active(self, asset):
        """Check asset activation status."""
        return True if asset['status'] == 'active' else False

    def _download_for_scene(self, active_assets, file_header):
        """Download mulitple assets."""
        staged = [d.copy() for d in active_assets]
        for asset_dict in staged:
            for asset_type, asset in asset_dict.items():
                path = self.download(asset, file_header)
                asset_dict[asset_type] = path
        return staged
            
    def download(self, asset, file_header):
        """Download an asset."""
        body = self._client.download(asset).get_body()
        path = file_header + body.name
        body.write(file=path)
        return path

    def _reprocess(self, bbox, staged_assets, records, **specs):
        """Run combined geo- and color- postprocessing routines.

        Returns: Cleaned, combined record, including paths to final images.
        """
        source_epsg_codes = [_get_epsg_code(record) for record in records]
        target_epsg_code = _get_epsg_code(_sort_by_overlap(bbox, records)[0])
        footprint = bbox.intersection(_get_footprint(records))
        item_type = records[0]['properties']['item_type']

        scene_record = _merge_records(
            [_clean_record(record) for record in records])
        scene_record.update({'paths': []})

        for asset_type in set([k for sa in staged_assets for k in sa.keys()]):
            output_bands = _get_bandmap(item_type, asset_type)
            paths = [sa[asset_type] for sa in staged_assets]
            merged_path = self.geo_process(
                footprint, paths,
                source_epsg_codes, target_epsg_code, output_bands)
            output_paths = self.color_process(merged_path, asset_type, **specs)
            if self.specs['thumbnails']:
                resample.make_thumbnails(output_paths)
            scene_record['paths'] += output_paths
        return scene_record
                
    def geo_process(self, footprint, paths, source_epsg_codes, target_epsg_code,
                    output_bands):
        """Reproject, crop to footprint, extract output bands, merge.

        Arguments:
            footprint: shapely polygon
            paths: list of paths to component images of a scene
            records: image records
            source_epsg_code: list of integer EPSG codes
            target_epsg_code: integer EPSG code
                (typically here codes are WGS 84 / UTM zone codes, e.g., 32617)
            output_bands: a list of bands by number (indexed from 1)
            
        Returns:  Path to output image.
        """
        reprojected = []
        if target_epsg_code:
            for path, source_code in zip(paths, source_epsg_codes):
                if source_code and source_code != target_epsg_code:
                    path = gdal_routines.reproject(path, target_epsg_code)
                reprojected.append(path)
        else:
            reprojected = paths

        reshaped = []
        for path in reprojected:
            path = gdal_routines.crop_and_reband(path, footprint, output_bands)
            reshaped.append(path)
         
        if len(reshaped) > 1:
            scene_path = gdal_routines.merge(reshaped)
        else:
            scene_path = reshaped[0]
        return scene_path
        
    def color_process(self, path, asset_type, write_styles=[], **specs):
        """Correct color (producing mutliple versions of the image).

        Returns:  Updated record with paths to color-corrected images.
        """
        output_paths = []
        styles = [style.lower() for style in write_styles]

        def correct_and_write(img, path, style):
            """Correct color and write to file."""
            corrected = color.STYLES[style](img)
            outpath = path.split('.tif')[0] + '-' + style + '.png'
            print('\nStaging at {}\n'.format(outpath), flush=True)
            skimage.io.imsave(outpath, corrected)
            return outpath
            
        img = skimage.io.imread(path)
        if (asset_type == 'visual' or asset_type == 'ortho_visual'):

            # add this minimal tweak, since Planet visual is already corrected:
            output_paths.append(correct_and_write(img, path, 'expanded'))

        else:
            for style in styles:
                if style in color.STYLES.keys():
                    output_paths.append(correct_and_write(img, path, style))

        if self.specs['thumbnails']:
            os.remove(path)
        else:
            output_paths.append(path)
        return output_paths

    # Functions for grouping records returned by search into
    # collections that can be stitched to cover the requested scene:
    
    def _group_into_scenes(self, bbox, records, **specs):
        """Find groups of overlapping, same-day images. 

        Returns:  List of lists of records
        """
        scenes = []
        records = json.loads(json.dumps(records))
        while records and len(scenes) < specs['N_images']:
            date, groups = self._pop_day(records)
            groups = self._filter_by_overlap(bbox, groups)
            groups = self._filter_copies(groups)
            group_records = [v['records'] for v in groups.values()]
            while group_records and len(scenes) < specs['N_images']:
                scenes.append(group_records.pop())
                if self.specs['skip_days']:
                    self._fastforward(records, date)
                    break
        return scenes

    def _pop_day(self, records):
        """Pop a day's worth of records and sort by satellite id and item type.

        Argument records:  Image records sorted by date

        Output:  The day's records are popped from input variable records.

        Returns: The date and a dict of groups of records for the day
        """
        record = records.pop()
        item_type = record['properties']['item_type']
        sat_id = record['properties']['satellite_id']
        date0 = dateutil.parser.parse(record['properties']['acquired']).date()

        groups = {(sat_id, item_type): {'records': [record]}}
        while records:
            record = records.pop()
            date = dateutil.parser.parse(
                record['properties']['acquired']).date()
            if date == date0:
                item_type = record['properties']['item_type']
                sat_id = record['properties']['satellite_id']
                try: 
                    groups[(sat_id, item_type)]['records'].append(record)
                except KeyError:
                    groups.update({(sat_id, item_type): {'records': [record]}})
            else:
                records.append(record)  # replace the next day's record 
                break

        return date, groups
    
    def _filter_by_overlap(self, bbox, groups):
        """Enforce min_intersect criteria on groups of records.

        Returns: A dict of groups of records
        """
        filtered = {}
        for k,v in groups.items():
            overlap = _get_overlap(bbox, _get_footprint(v['records']))
                          
            if overlap < self.specs['min_intersect']:
                ids = [record['id'] for record in v['records']]
                print('Rejected scene IDs {}: Overlap with bbox {:.1f}%'.format(
                    ids, 100 * overlap))
            else:
                filtered.update({
                    k: {
                        'records': v['records'],
                        'overlap': overlap
                    }
                })

        return filtered

    def _filter_copies(self, groups):
        """Eliminate redundant groups of records.

        PSOrthoTiles are constructed from PSScene3Band items.  If satellite
        ids and dates are the same, the underlying imagery is the same and 
        one set or the other can be safely deleted. The preference is to keep
        PSScene3Band because they are smaller and less expensive to process.

        Returns: Dict of groups of records.
        """
        filtered = groups.copy()
        sat_ids = set([sat_id for (sat_id, item_type) in filtered.keys()])
        for sat_id in sat_ids:
            try:
                if (filtered[(sat_id, 'PSScene3Band')]['overlap'] >= 
                    filtered[(sat_id, 'PSOrthoTile')]['overlap']):
                    filtered.pop((sat_id, 'PSOrthoTile'))
                else:
                    filtered.pop((sat_id, 'PSScene3Band'))
            except KeyError:
                pass
        
        return filtered

    def _fastforward(self, records, date):
        """Pop records until all are older than date by specs['skip_days']

        Arguments:
            records: Image records sorted by date
            date: Reference date to work back from

        Output: Records are popped from input variable records.

        Returns: None
        """
        target_date = date - datetime.timedelta(days=self.specs['skip_days'])
        while records:
            record = records.pop()
            date_aq = dateutil.parser.parse(
                record['properties']['acquired']).date()
            if date_aq <= target_date:
                records.append(record) # replace this record
                break
        return 
                
    
# geometric utilities

def _sort_by_overlap(bbox, records):
    """Sort records in group by area of overlap with bbox (large to small)."""
    recs_sorted = sorted(
        records,
        key= lambda rec: _get_overlap(bbox, geometry.asShape(rec['geometry'])),
        reverse=True)
    return recs_sorted

def _get_overlap(bbox, footprint):
    """Comptue overlap of bbox and footprint.

    Arguments bbox, footprint: Shapely polygons
    
    Returns: Decimal fractional area of intersection
    """
    intersection = bbox.intersection(footprint)
    intersect_frac = intersection.area/bbox.area
    return intersect_frac

def _get_footprint(records):
    """Find the union of geometries in records."""
    footprints = [geometry.asShape(rec['geometry']) for rec in records]
    return cascaded_union(footprints)
    

# Planet-specific formatting functions

def _build_search_filters(**specs):
    """Build filters to search catalog."""
    sf = [api.filters.range_filter('cloud_cover', lt=specs['clouds']/100)]
    if specs['startDate']:
        sf.append(api.filters.date_range('acquired', gt=specs['startDate']))
    if specs['endDate']:
        sf.append(api.filters.date_range('acquired', lt=specs['endDate']))
    return sf

def _merge_records(records):
    """Combine records for all images in a scene."""
    return {'component_images': [record for record in records]}

def _clean_record(record):
    """Streamline image record."""
    keymap = {  # maps record keys to our standardized nomenclature
        'provider': 'provider',
        'item_type': 'item_type',
        'asset_type': 'asset_type',
        'acquired': 'timestamp',
        'cloud_cover': 'clouds',
        'pixel_resolution': 'resolution',
        'gsd': 'gsd',
        'epsg_code': 'epsg_code',
        'satellite_id': 'satellite_id'
    }
    cleaned = {'catalogID': record['id']}
    cleaned.update({'thumbnail': record['_links']['thumbnail']})
    cleaned.update({'full_record': record['_links']['_self']})
    cleaned.update({keymap[k]:v for k,v in record['properties'].items()
        if k in keymap.keys()})
    cleaned['clouds'] *= 100
    return cleaned

def _get_epsg_code(record):
    """Extract an EPSG code if available, or else return None."""
    try:
        code = record['properties']['epsg_code']
    except KeyError:
        code = None
    return code

def _get_bandmap(item_type, asset_type):
    """Find the band order for R-G-B bands."""
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
    try:
        bands = bandmaps[item_type][asset_type]
    except KeyError as e:
        raise KeyError('{}: Bandmap not defined for {}:{}'.format(
            repr(e), item_type, asset_type))
    return bands



