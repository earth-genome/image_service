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
import sys

import dateutil
import shapely

from postprocessing import color
from postprocessing import gdal_routines
from postprocessing import landcover
from postprocessing import resample
from utilities import cloud_storage
from utilities.geobox import geobox

SPECS_FILE = os.path.join(os.path.dirname(__file__), 'default_specs.json')

STAGING_DIR = os.path.join(os.path.dirname(__file__), 'tmp-staging')

def loop(function):
    """Scheduling wrapper for async execution."""
    def scheduled(*args, **kwargs):
        loop = asyncio.get_event_loop()
        output = loop.run_until_complete(function(*args, **kwargs))
        return output
    return scheduled

class ImageGrabber(ABC):
    """Template for image grabbing.

    Template attributes:
        client: Satellite provider API instantiated interface  
        bucket_tool: Class instance to access cloud storage bucket, or 
            None to save images locally
        specs: Dict of catalog and image specs

    Template external methods:
        __call__: Wrapper for async execution of pull().
        async pull: Pull the most recent images consistent with specs.
        async pull_by_id:  Pull and write image for a known catalogID.
        prep_scenes: Search and group search records into scenes.
        async grab_scene: Activate, download, and process scene assets.
        search_clean: Search and return streamlined image records.
        search_latlon_clean:  Search and return streamlined image records.
        search_id_clean: Retrieve record for input catalogID.
        photoshop: Convert a raw GeoTiff into visual and data products.
    """

    def __init__(self, client, bucket='bespoke-images',
                 staging_dir=STAGING_DIR, specs_filename=SPECS_FILE, **specs):

        self.client = client
        if bucket:
            try: 
                self.bucket_tool = cloud_storage.BucketTool(bucket)
            except Exception as e:
                print('Bucket name not recognized.')
                raise
        else:
            self.bucket_tool = None

        with open(specs_filename, 'r') as f:
            self.specs = json.load(f)
        self.specs.update(specs)
        if not os.path.exists(staging_dir):
            os.makedirs(staging_dir)
        self.specs.update({
            'file_header':
                os.path.join(staging_dir, self.specs.get('file_header', ''))
        })

        
    # Top level image grabbing functions

    def __call__(self, bbox):
        """Wrapper for async execution of pull()."""
        looped = loop(self.pull)
        return looped(bbox)

    async def pull(self, bbox):
        """Pull the most recent images consistent with specs.
    
        Argument: bbox: a shapely box
            
        Returns: List of image records along with any exceptions
        """
        scenes = self.prep_scenes(bbox)
        grab_tasks = [self.grab_scene(scene, bbox) for scene in scenes]
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
        
    async def grab_scene(self, scene, bbox):
        """Activate, download, and process scene assets."""
        paths = await self._download(scene, bbox)
        merged_path, record = self._mosaic(paths, scene, bbox)
        output_paths = self.photoshop(merged_path)
        if self.specs['thumbnails']:
            resample.make_thumbnails(output_paths)
        if self.bucket_tool:
            urls = self._upload(output_paths)
            record.update({'urls': urls})
        else:
            record.update({'paths': output_paths})
        return record

    def _upload(self, paths):
        """Upload staged image files to the bucket.

        Output:  Files are uploaded and local copies removed.
        Returns:  List of bucket urls.
        """
        urls = []
        for path in paths:
            urls.append(
                self.bucket_tool.upload_blob(path, os.path.split(path)[1]))
            os.remove(path)
        print('Uploaded images:\n{}'.format(urls), flush=True)
        return urls

    
    # Search and scene preparation
    
    def prep_scenes(self, bbox):
        """Search and group search records into scenes.

        Returns: List of lists of records.  
        """
        records = self._search(bbox)
        scenes = self._compile_scenes(records, bbox)
        return scenes
    
    @abstractmethod   
    def _search(self):
        pass

    @abstractmethod
    def _search_id(self):
        pass

    def _search_latlon(self, lat, lon, epsilon=.001):
        """Search catalog for images containing lat, lon.

        Argument epsilon: Scale in km for a small box around lat, lon.
        """
        minibox = geobox.bbox_from_scale(lat, lon, epsilon)
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

    def _get_overlap(self, bbox, *records):
        """Find geographic intersection between bbox and records.

        Returns: A Shapely shape and fractional area relative to bbox.
        """
        footprints = [self._read_footprint(r) for r in records]
        overlap = bbox.intersection(shapely.ops.cascaded_union(footprints))
        return overlap, overlap.area/bbox.area

    def _well_overlapped(self, frac_area, *IDs):
        """Check whether fractional area meets specs.
        
        Arguments:
            frac_area: Fractional area of overlap relative to bbox
            *IDs: Optional scene identifiers for informational print
            
        Returns: Boolean 
        """
        well_o = (frac_area >= self.specs['min_intersect'])
        if not well_o:
            print('Rejecting scene {}. '.format(IDs) +
                'Overlap with bbox {:.1%}'.format(frac_area), flush=True)
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

    def _mosaic(self, paths, records, bbox):
        """Assemble assets to geographic specs.

        Simplest case is handled here - when there is one path, one record.
        """
        merged = next(iter(paths))
        record = self._clean(next(iter(records)))
        return merged, record

    def photoshop(self, path):
        """Convert a raw GeoTiff into visual and data products."""
        output_paths = []
        if self.specs['landcover_indices']:
            output_paths += self._indexing(path)
            path = gdal_routines.reband(path, [1, 2, 3])
        output_paths += self._coloring(path)

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

    
