"""Class to automate searching and downloadling from Planet Labs.

API ref: https://planetlabs.github.io/planet-client-python/index.html

Class PlanetGrabber: Descendant of class base.ImageGrabber

Usage with default specs: 

> from utilities.geobox import geobox
> bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
> g = PlanetGrabber()
> g(bbox)

Output images will be uploaded to Google cloud storage and relevant urls 
returned. To save images locally, instantiate with 'bucket=None' and an 
optional directory:
> g = PlanetGrabber(bucket=None, staging_dir='my_dir')

Because planet images are relatively small, multiple images often must be
pulled and assembled to cover a bounding box. The minimal processing unit
is therefore denoted a 'scene,' which includes all the images (or their
records) required to produce one final image of the bounding box.  A call
will attempt to produce N_images such scenes.  

Catalog and image specs have defaults set in default_specs.json, and can be 
overriden by passing either specs_filename=alternate_specs.json
or **kwargs to PlanetGrabber. As of writing, the Planet-relevant default 
specs take form:

{
    "clouds": 10,   # maximum allowed percentage cloud cover
    "min_intersect": 0.9,  # min fractional overlap between bbox and scene
    "startDate": "2008-09-01T00:00:00.0000Z",  # for catalog search
    "endDate": null,  # for catalog search
    "N_images": 1,
    "skip_days": 0, # min days between scenes if N_images > 1
    "write_styles": [
        "base",
        "vibrant"
    ],
    "landcover_indices": [],
    "thumbnails": false,
    "file_header": "",
    "item_types": [
        "PSScene3Band",
	    "PSOrthoTile",
	    "REOrthoTile"
    ],
    "asset_type": "analytic"
}

"""

import asyncio

import dateutil
import numpy as np
from planet import api
import shapely

import base
from postprocessing import gdal_routines

KNOWN_ITEM_TYPES = ['PSScene4Band', 'PSScene3Band', 'PSOrthoTile',
                    'REOrthoTile', 'SkySatScene']
KNOWN_ASSET_TYPES = ['analytic', 'ortho_visual', 'visual']

# For asynchronous handling of scene activation and download, in seconds:
WAITTIME = 10

# Planet band numbers for R-G-B-NIR bands:
BANDMAP = {   
    'PSScene3Band': {
        'visual': [1, 2, 3],
        'analytic': [1, 2, 3]
    },
    'PSScene4Band': {
        'analytic': [3, 2, 1, 4]
    },
    'PSOrthoTile': {
        'visual': [1, 2, 3],
        'analytic': [3, 2, 1, 4]
    },
    'REOrthoTile': {
        'visual': [1, 2, 3],
        'analytic': [3, 2, 1, 5]
    },
    'SkySatScene': {
        'ortho_visual': [3, 2, 1],
        'analytic': [3, 2, 1, 4]
    }
}

# To standardize image records:
KEYMAP = {  
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


class PlanetGrabber(base.ImageGrabber):
    """Tool to pull Planet Labs imagery.

    External attributes and methods are defined in the parent ImageGrabber. 
    """
    
    def __init__(self, client=None, **kwargs):
        if not client:
            client = api.ClientV1()
        super().__init__(client, **kwargs)
        if self.specs['landcover_indices']:
            self._tweak_landcover_specs()
        self._validate_asset_type()
        self._bandmap = {k:v.get(self.specs['asset_type'])
                             for k,v in BANDMAP.items()}
        self._keymap = KEYMAP.copy()
        self._search_filters = self._build_search_filters()

    # Initializations to Planet requirements

    def _tweak_landcover_specs(self):
        """Adjust item and asset types as required for landcover indices."""
        if 'PSScene3Band' in self.specs['item_types']:
            print('Replacing PSScene3Band with 4Band for landcover indices.')
            self.specs['item_types'].remove('PSScene3Band')
            self.specs['item_types'].append('PSScene4Band')
            self.specs['item_types'] = list(set(self.specs['item_types']))
        if self.specs['asset_type'] != 'analytic':
            print('Changing asset type to analytic, as required for landcover'
                  ' indices.')
            self.specs['asset_type'] = 'analytic'
                
    def _validate_asset_type(self):
        """Check for mismatched item/asset types and raise helpfully."""
        asset_type = self.specs['asset_type']
        for item_type in self.specs['item_types']:
            options = list(BANDMAP[item_type])
            if asset_type not in options:
                memo = ('Asset type <{}> is not available for {}. Options '
                        'are: {}').format(asset_type, item_type, options)
                raise KeyError(memo)

    def _build_search_filters(self):
        """Build filters to search catalog."""
        sf = [api.filters.range_filter('cloud_cover',
                                       lt=self.specs['clouds']/100)]
        if self.specs['startDate']:
            sf.append(api.filters.date_range('acquired',
                                             gt=self.specs['startDate']))
        if self.specs['endDate']:
            sf.append(api.filters.date_range('acquired',
                                             lt=self.specs['endDate']))
        return sf
                    
            
    # Search and scene preparation.

    def _search(self, bbox):
        """Search the catalog for relevant imagery.

        Returns: An iterator over image records. 
        """
        aoi = shapely.geometry.mapping(bbox)
        query = api.filters.and_filter(
            api.filters.geom_filter(aoi), *self._search_filters)
        request = api.filters.build_search_request(query,
            item_types=self.specs['item_types'])
        response = self.client.quick_search(request, sort='acquired desc')
        return response.items_iter(limit=None)

    def _search_id(self, catalogID, item_type):
        """Retrieve record for input catalogID."""
        response = self.client.get_item(item_type, catalogID)
        return response.get()

    def _clean(self, record):
        """Streamline image record."""
        cleaned = {'catalogID': record['id']}
        cleaned.update({'thumbnail': record['_links']['thumbnail']})
        cleaned.update({'full_record': record['_links']['_self']})
        cleaned.update({self._keymap[k]:v for k,v
            in record['properties'].items() if k in self._keymap})
        cleaned['clouds'] *= 100
        return cleaned

    def _compile_scenes(self, records, bbox):
        """Find groups of overlapping, same-day images. 

        Returns:  List of lists of records
        """
        scenes = []
        next_rec = next(records, None)
        while next_rec and len(scenes) < self.specs['N_images']:
            groups, next_rec = self._group_day(records, next_rec)
            groups = self._filter_by_overlap(bbox, groups)
            grouped_records = self._filter_copies(groups)
            if self.specs.get('skip_days') and grouped_records:
                scenes.append(next(iter(grouped_records)))
            else:
                scenes += grouped_records
                scenes = scenes[:self.specs['N_images']]
        return scenes

    def _group_day(self, records, base):
        """Collect a day's records, organized by satellite id and item type.

        Arguments:
            base: The first record from a day
            records:  Image record iterator

        Returns: A dict of records for the day and a new base record
        """
        item_type = base['properties']['item_type']
        sat_id = base['properties']['satellite_id']
        date_0 = dateutil.parser.parse(base['properties']['acquired']).date()

        groups = {(sat_id, item_type): [base]}
        for record in records:
            date = dateutil.parser.parse(
                record['properties']['acquired']).date()
            if date == date_0:
                item_type = record['properties']['item_type']
                sat_id = record['properties']['satellite_id']
                try: 
                    groups[(sat_id, item_type)].append(record)
                except KeyError:
                    groups.update({(sat_id, item_type): [record]})
            else:
                if self.specs.get('skip_days'):
                    if (date_0 - date).days < self.specs['skip_days']:
                        record = self._fastforward(records, date_0)
                break
        else:
            record = None
        return groups, record

    def _read_footprint(self, record):
        """Extract footprint in record as a shapely shape."""
        return shapely.geometry.asShape(record['geometry'])

    def _filter_by_overlap(self, bbox, groups):
        """Exclude groups that don't overlap sufficiently with bbox."""
        filtered = {}
        for key, records in groups.items():
            _, frac_area = self._get_overlap(bbox, *records)
            if self._well_overlapped(frac_area, *[r['id'] for r in records]):
                filtered.update({key: records})
        return filtered

    def _filter_copies(self, groups):
        """Reduce groups to unique scenes.  
        
        PSOrthoTiles are constructed from PSScene items. If satellite
        ids and dates are the same, the underlying imagery is the same and 
        one set or the other can be safely omitted. In this case, the
        accepted item_type is determined by the ordering in KNOWN_ITEM_TYPES.
        
        Returns: List of scenes (each scene a list of records)
        """
        filtered = {}
        sat_ids = set([sat_id for (sat_id, _) in groups])
        for sat_id in sat_ids:
            for item_type in KNOWN_ITEM_TYPES:
                if (sat_id, item_type) in groups:
                    filtered[sat_id] = groups[(sat_id, item_type)]
                    break
        return list(filtered.values())

    
    # Scene activation and download
    
    async def _download(self, scene, *args):
        """Download scene assets.

        Returns: List of paths to downloaded raw images.
        """
        activation_tasks = [
            self._activate(record['properties']['item_type'], record['id'])
                for record in scene
        ]
        results = await asyncio.gather(*activation_tasks)
        paths = [self._write(*result) for result in results]
        return paths
    
    async def _activate(self, item_type, catalogID):
        """Initiate and monitor asset activation.

        Raises: KeyError when requested asset type isn't available

        Returns: Activated asset and its catalogID 
            (so that ID tracks with asset during async processing)
        """
        assets = self.client.get_assets_by_id(item_type, catalogID).get()
        try: 
            asset = assets[self.specs['asset_type']]
        except KeyError:
            raise KeyError('Asset type <{}> not available for ID {}.'.format(
                self.specs['asset_type'], catalogID))
        
        self.client.activate(asset)
        print('Activating {}. '.format(catalogID) +
            'This could take several minutes.', flush=True)
        while not self._is_active(asset):
            await asyncio.sleep(WAITTIME)
            assets = self.client.get_assets_by_id(item_type, catalogID).get()
            asset = assets[self.specs['asset_type']]
        return asset, catalogID

    def _is_active(self, asset):
        """Check asset activation status."""
        return True if asset['status'] == 'active' else False
            
    def _write(self, asset, catalogID):
        """Call for the image data and write to disk."""
        body = self.client.download(asset).get_body()
        path = self._build_filename(catalogID)
        print('Staging at {}\n'.format(path), flush=True)
        body.write(file=path)
        return path

    def _build_filename(self, catalogID):
        """Compose an image filename."""
        filename = (self.specs['file_header'] + catalogID + '_' +
                    self.specs['asset_type'] + '.tif')
        return filename

    
    # Reprocessing
    
    def _mosaic(self, paths, records, bbox):
        """Assemble assets to geographic specs.

        Returns: Scene image path and cleaned, combined record.
        """
        paths = self._reorder(paths, records)
        central_record = next(iter(self._sort_by_overlap(bbox, records)))
                                  
        target_epsg_code = self._get_epsg_code(central_record)
        if target_epsg_code:
            paths = self._reproject(paths, records, target_epsg_code)

        overlap, _ = self._get_overlap(bbox, *records)
        bands = self._bandmap[central_record['properties']['item_type']]
        if not self.specs['landcover_indices']:
            bands = bands[:3]
        paths = [gdal_routines.crop_and_reband(path, overlap, bands) 
                     for path in paths]

        if len(paths) > 1:
            scene_path = gdal_routines.merge(paths)
        else:
            scene_path = next(iter(paths))
        scene_record = {'component_images': [self._clean(r) for r in records]}
        return scene_path, scene_record

    def _reorder(self, paths, records):
        """After async download, order paths to match order of their records."""
        ordered = []
        for r in records:
            ordered.append(next(path for path in paths if r['id'] in path))
        return ordered
        
    def _sort_by_overlap(self, bbox, records):
        """Sort records in group by overlap with bbox (large to small)."""
        recs_sorted = sorted(
            records, key=lambda rec: self._get_overlap(bbox, rec)[1],
            reverse=True)
        return recs_sorted

    def _get_epsg_code(self, record):
        """Extract an EPSG code if available."""
        return record['properties'].get('epsg_code', None)

    def _reproject(self, paths, records, target_epsg_code):
        """As required, reproject images to target_epsg_code."""
        reprojected = []
        for path, record in zip(paths, records):
            source_code = self._get_epsg_code(record)
            if source_code and source_code != target_epsg_code:
                path = gdal_routines.reproject(path, target_epsg_code)
            reprojected.append(path)
        return reprojected







