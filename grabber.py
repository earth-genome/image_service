"""Abstract base class to automate search and download of satellite imagery.

External class methods are defined in the template class ImageGrabber, here,
and provider-dependent details expressed in its descendants 
dg_grabber.DGImageGrabber and planet_grabber.PlanetGrabber.

"""
 
from abc import ABC, abstractmethod
import asyncio
import datetime
from itertools import islice
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
        """Pull and write image for a known catalogID.

        Argument *args: For Planet, an item_type
        """
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
        records = self._search(bbox)
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
        return [self._clean(r) for r in islice(records, max_records)]
    
    def search_latlon_clean(self, lat, lon, max_records=None):
        """Search the catalog and return streamlined records."""
        records = self._search_latlon(lat, lon)
        return [self._clean(r) for r in islice(records, max_records)]

    def search_id_clean(self, catalogID, *args):
        """Retrieve record for input catalogID."""
        record = self._search_id(catalogID, *args)
        return self._clean(record)

    @abstractmethod
    def _clean(self, record):
        """Streamline image record."""
        pass
        
    @abstractmethod
    def _compile_scenes(self):
        pass

    def _get_overlap(self, bbox, records):
        """Find geographic intersection between bbox and records.

        Returns: A Shapely shape and fractional area relative to bbox."""
        footprints = [self._read_footprint(r) for r in records]
        overlap = bbox.intersection(shapely.ops.cascaded_union(footprints))
        return overlap, overlap.area/bbox.area

    def _well_overlapped(self, frac_area, IDs):
        """Check whether fractional area meets specs."""
        well_o = (frac_area >= self.specs['min_intersect'])
        if not well_o:
            print('Rejecting scene {}. Overlap with bbox {:.1%}'.format(
                IDs, frac_area))
        return well_o

    @abstractmethod
    def _read_footprint(self):
        pass

    def _fastforward(self, records, date):
        """Advance to a record older than date by self.specs['skip_days'].

        Arguments:
            records: Image record iterator
            date: Reference datetime.date object

        Returns: An image record, or None
        """
        for record in records:
            cleaned = self._clean(record)
            date_aq = dateutil.parser.parse(cleaned['timestamp']).date()
            if (date - date_aq).days > self.specs['skip_days']:
                return record

        
    # Scene activation and download

    @abstractmethod
    def _download(self):
        pass

        
    # Reprocessing

    def _mosaic(self, bbox, paths, records):
        """Assemble assets to geographic specs."""
        merged = next(iter(paths))
        record = self._clean(next(iter(records)))
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

    
