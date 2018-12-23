"""Abstract base class to automate search and download of satellite imagery.

External class methods are defined in the template class ImageGrabber, here,
and provider-dependent details expressed in its descendants 
dg_grabber.DGImageGrabber and planet_grabber.PlanetGrabber.

"""
 
from abc import ABC, abstractmethod
import asyncio
import datetime
import json
import os

import dateutil
import shapely

from postprocessing import color
from postprocessing import gdal_routines
from postprocessing import landcover
from postprocessing import resample
from utilities.geobox import geobox

SPECS_FILE = os.path.join(os.path.dirname(__file__), 'default_specs.json')

class ImageGrabber(ABC):
    """Template for image grabbing.

    Template attributes:
        client: Satellite provider API instantiated interface  
        specs: Dict of catalog and image specs

    Template external methods:
        __call__: Scheduling wrapper for async execution of pull()
        async pull: Pull the most recent images consistent with specs.
        async pull_by_id:  Pull and write image for a known catalogID.
        prep_scenes: Search and group search records into scenes.
        async grab_scene: Activate, download, and process scene assets.
        search_clean: Search and return streamlined image records.
        search_latlon_clean:  Search and return streamlined image records.
        search_id_clean: Retrieve record for input catalogID.
        photoshop: Convert a raw GeoTiff into visual and data products.
    """

    def __init__(self, client, specs_filename=SPECS_FILE, **specs):
        self.client = client
        with open(specs_filename, 'r') as f:
            self.specs = json.load(f)
        self.specs.update(specs)

        
    # Top level image grabbing functions

    def __call__(self, bbox):
        """Scheduling wrapper for async execution of pull()."""
        loop = asyncio.get_event_loop()
        recs_written = loop.run_until_complete(self.pull(bbox))
        return recs_written

    async def pull(self, bbox):
        """Pull the most recent images consistent with specs.
    
        Argument: bbox: a shapely box
            
        Returns: List of image records along with any exceptions
        """
        scenes = self.prep_scenes(bbox)
        grab_tasks = [self.grab_scene(bbox, scene) for scene in scenes]
        results = await asyncio.gather(*grab_tasks, return_exceptions=True)
        return results

    async def pull_by_id(self, bbox, catalogID, *args):
        """Pull and write image for a known catalogID."""
        records = [self._search_id(catalogID, *args)]
        try: 
            scene = next(iter(self._compile_scenes(bbox, records)))
        except StopIteration:
            return {}
        record = await self.grab_scene(bbox, scene)
        return record
        
    async def grab_scene(self, bbox, scene):
        """Activate, download, and process scene assets."""
        paths = await self._download(bbox, scene)
        merged_path, record = self._mosaic(bbox, paths, scene)
        output_paths = self.photoshop(merged_path)
        if self.specs['thumbnails']:
            resample.make_thumbnails(output_paths)
        record.update({'paths': output_paths})
        return record

    
    # Search and scene preparation
    
    def prep_scenes(self, bbox):
        """Search and group search records into scenes.

        Returns: List of lists of records.  
        """
        records = self._search(bbox)[::-1]
        scenes = self._compile_scenes(bbox, records)
        return scenes
    
    @abstractmethod   
    def _search(self):
        pass

    @abstractmethod
    def _search_id(self):
        pass

    def _search_latlon(self, lat, lon, eps=.001):
        """Search catalog for images containing lat, lon.

        Argument eps: Scale in km for a small box around lat, lon.
        """
        minibox = geobox.bbox_from_scale(lat, lon, eps)
        return self._search(minibox)
    
    def search_clean(self, bbox, max_records=None):
        """Search the catalog and return streamlined records."""
        records = self._search(bbox)
        return [self._clean_record(r) for r in records[:max_records]]

    def search_latlon_clean(self, lat, lon, max_records=None):
        """Search the catalog and return streamlined records."""
        records = self._search_latlon(lat, lon)
        return [self._clean_record(r) for r in records[:max_records]]

    def search_id_clean(self, catalogID, *args):
        """Retrieve record for input catalogID."""
        record = self._search_id(catalogID, *args)
        return self._clean_record(record)

    @abstractmethod
    def _clean_record(self):
        pass
    
    @abstractmethod
    def _compile_scenes(self):
        pass

    def _well_overlapped(self, bbox, records):
        """Check whether records meet specs for overlap with bbox."""
        intersection = bbox.intersection(self._footprint(records))
        fraction = intersection.area/bbox.area
        well_o = (fraction >= self.specs['min_intersect'])
        if not well_o:
            print('Rejecting scene. Overlap with bbox {:.1%}'.format(fraction))
        return well_o

    def _footprint(self, records):
        footprints = [self._read_footprint(r) for r in records]
        return shapely.ops.cascaded_union(footprints)

    @abstractmethod
    def _read_footprint(self):
        pass

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

    
    # Scene activation and download

    @abstractmethod
    def _download(self):
        pass

    @abstractmethod
    def _get_bandmap(self):
        pass

        
    # Reprocessing

    def _mosaic(self, bbox, paths, records):
        """Assemble assets to geographic specs."""
        merged = next(iter(paths))
        record = self._clean_record(next(iter(records)))
        return merged, record

    def photoshop(self, path):
        """Convert a raw GeoTiff into visual and data products."""
        output_paths = []
        if self.specs['landcover_indices']:
            output_paths.append(self._indexing(path))
            path = gdal_routines.reband(path, [1, 2, 3])
        output_paths.append(self._coloring(path))

        if self.specs['thumbnails'] and output_paths:
            os.remove(path)
        else:
            output_paths.append(path)
        return output_paths
            
    def _indexing(self, path):
        """Compute landcover indices.

        Returns: Paths to color-corrected images
        """
        output_paths = []
        indices = [index.lower() for index in self.specs['landcover_indices']
                   if index in landcover.INDICES]
        for index in indices:
            output_paths.append(landcover.compute_index(path, index))
        return output_paths
    
    def _coloring(self, path):
        """Produce styles of visual images.

        Returns: Paths to color-corrected images
        """
        output_paths = []
        styles = [style.lower() for style in self.specs['write_styles']
                  if style in color.STYLES.keys()]
        for style in styles:
            output_paths.append(color.ColorCorrect(style=style)(path))
        return output_paths

    
