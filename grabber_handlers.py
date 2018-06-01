"""Classes for high-level management of image-grabbing processes.

--- --- 
WIP:  Currently these routines are tuned to run in the image_service Quart
web app. To run locally, simply decorate any async method with @loop, or
at runtime create a scheduled version of the function by passing it through
loop() explicitly, e.g. to pull for a bbox: 

> g = GrabberHandler(bucket_name, specs_filename='specs.json',
                     **more_image_specs)
> puller = loop(g.pull)
> records = puller(bbox, **override_image_specs)

Running from the interpreter, however, raises some issues with clean
shutdown on KeyboardInterrupt.  See loop() below.  

The minimal asynchronicity here could also be disabled entirely by
search-and-deleting the async and await keywords.  
--- ---

Usage:

To pull for a GeoJSON FeatureCollection:
> g = GeoJSONGrabber(bucket_name, specs_filename='specs.json',
                      **more_image_specs)
> updated_feature_collection = g.pull_for_geojson(features_filename)

(The FeatureCollection itself may contain image_specs which will override those
initialized.)  

To pull for the news wire:
> sg = StoryGrabber(WIRE_BUCKET, specs_filename='specs.json', **more_image_specs)
> sg.pull_for_wire()

To pull for a single DBItem story:
> sg = StoryGrabber(bucket_name, specs_filename='specs.json', **more_image_specs)
> records = sg.pull_for_story(story, **override_image_specs)

To pull for a shapely bbox:
> g = GrabberHandler(bucket_name, specs_filename='specs.json',
                     **more_image_specs)
> records = g.pull(bbox, **override_image_specs)

See pull_for_wire.py and pull_for_geojson.py for command-line wrappers.

Image specs determine additional image parameters and are passed through
grabber.py to individual (Digital Globe, Planet, etc.) grabbers.
They may be specified  via **kwargs, and/or via json-formatted file.
As of writing, default_specs.json contains:
{
    "clouds": 10,  # maximum allowed percentage cloud cover
    "min_intersect": 0.9,  # min fractional overlap between bbox and scene
    "startDate": "2008-09-06",  # Earliest allowed date for catalog search 
    "endDate": null, # Latest allowed date for catalog search
    "N_images": 3  # Number of images to pull for each bbox
    "write_styles": [  # Defined in postprocessing.color
        "matte",       
        "contrast",
        "dra",
        "desert"
    ]
}

The default begin-of-epoch startDate is specified somewhat arbitrarily as
the launch date of GeoEye-1.

See default_story_specs.json for additional specs used in the StoryHandler
context.

Additional kwargs corresponding to specs for individual grabbers may be passed
in the same way. (The parameters above in default_specs have nomenclature
common to all providers; other specs should have names *unique* to a given
provider to avoid unintended consequences of passing, e.g. a Planet
spec to a DG call.  See, e.g. digital_globe/dg_default_specs.json for
DG-specific parameters, such as image_source (='WV' by default).)

Outputs:

Images are uploaded to a cloud storage bucket. Image records, including bucket
urls to pulled images, are returned by pulling functions.  

Additionally, in the case of pull_for_geojson, the image records are added as
'properties' with key 'images' to the geojson features and a new
FeatureCollection is written to geojsonfile-images.json.  For
pull_for_story and pull_for_wire, the story core_locations are updated with
the records, and the story is reposted to the database.
"""

import asyncio
import datetime
import functools
import json
import os
import signal
import sys

import numpy as np
from shapely import geometry

from grab_imagery import cloud_storage
from grab_imagery import firebaseio
from grab_imagery import log_utilities
from grab_imagery.digital_globe import dg_grabber
from grab_imagery.geobox import geobox
from grab_imagery.geobox import conversions
from grab_imagery.planet_labs import planet_grabber


PROVIDER_CLASSES = {
    'digital_globe': dg_grabber.DGImageGrabber,
    'planet': planet_grabber.PlanetGrabber
}

# For staging, en route to bucket
STAGING_DIR = os.path.join(os.path.dirname(__file__), 'tmp-staging')

# News wire
STORY_SEEDS = firebaseio.DB(firebaseio.FIREBASE_URL)
DB_CATEGORY = '/WTL'

WIRE_START_DATE = (datetime.date.today()-datetime.timedelta(days=3)).isoformat()
WIRE_END_DATE = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()

WIRE_BUCKET = 'newswire-images'

def loop(function):
    """Scheduling wrapper to run async functions locally."""
    def scheduled(*args, **kwargs):
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(function(*args, **kwargs))
        
        # WIP: For clean shutdown, tasks need to be cancelled.
        # This isn't quite right though: sometimes the KeyboardInterrupt
        # (perhaps via the resulting CancelledError exception thrown to the
        # wrapped coroutine?) gets handled rather than raised. 
        try:
            output = loop.run_until_complete(task)
        except KeyboardInterrupt:
            task.cancel()
            output = loop.run_until_complete(
                asyncio.gather(task, return_exceptions=True))
        return output
    return scheduled

class GrabberHandler(object):
    """Class to manage providers and transfer images to a cloud storage bucket.

    Public Methods:
        async pull: Pull images for boundingbox.
        async pull_for id: Pull image for a given catalogID.


    Attributes:
        provider_classes: dict with class instantiators for pulling images,
            from modules in this repo; default PROVIDER_CLASSES above
        staging_dir: directory for local staging of images
        bucket_tool: class instance to access Google Cloud storage bucket
        logger: a Python logging.getLogger instance
        image_specs: dict of catalog search and image size specs
    """
    
    def __init__(self,
                 bucket_name,
                 providers=PROVIDER_CLASSES.keys(),
                 staging_dir=STAGING_DIR,
                 log_dest=sys.stderr,
                 specs_filename=os.path.join(os.path.dirname(__file__),
                                             'default_specs.json'),
                 **image_specs):
        
        self.provider_classes = {k:v for k,v in PROVIDER_CLASSES.items()
                          if k in providers}
        if not self.provider_classes:
            raise ValueError('Available providers: {}'.format(
                list(PROVIDER_CLASSES.keys())))
        
        self.staging_dir = staging_dir
        if not os.path.exists(self.staging_dir):
            os.makedirs(self.staging_dir)
            
        try: 
            self.bucket_tool = cloud_storage.BucketTool(bucket_name)
        except Exception as e:
            print('Bucket name not recognized: {}'.format(repr(e)))
            raise

        self.logger = log_utilities.get_stream_logger(log_dest)
        
        with open(specs_filename, 'r') as f:
            self.image_specs = json.load(f)
        self.image_specs.update(image_specs)

    async def pull(self, bbox, **image_specs):
        """Pull images for bbox.

        Arguments:
            bbox: a shapely box
            grab_specs: to override values in self.image_specs

        Output:  Images written posted to cloud storage bucket.

        Returns: List of image records (including bucket urls to images).
        """
        specs = self.image_specs.copy()
        specs.update(image_specs)
        grab_tasks = []
        
        for provider, grabber_class in self.provider_classes.items():
            grabber = grabber_class(**specs)
            conforming_bbox = self._enforce_size_specs(bbox, provider)
            print('Pulling for bbox {}.\n'.format(conforming_bbox.bounds))
            scenes = grabber.prep_scenes(conforming_bbox)

            grab_tasks += [
                asyncio.ensure_future(
                    grabber.grab_scene(
                        conforming_bbox, scene,
                        os.path.join(self.staging_dir, '')))
                for scene in scenes
            ]

        recs_written = []
        for task in asyncio.as_completed(grab_tasks):
            try: 
                written = await task
                urls = self._upload(written.pop('paths'))
                written.update({'urls': urls})
                recs_written.append(written)
            except Exception:
                self.logger.exception('Processing grab_tasks\n')

        print('Pulled {} scene(s).\n'.format(len(recs_written)), flush=True)
        return recs_written

    async def pull_by_id(self, provider, bbox, catalogID, item_type=None,
                   **image_specs):
        """Pull image for a given catalogID."""
        specs = self.image_specs.copy()
        specs.update(image_specs)
        
        grabber = self.provider_classes[provider](**specs)
        try: 
            record = await grabber.grab_by_id(
                bbox, catalogID, item_type, 
                file_header=os.path.join(self.staging_dir, ''),
                **specs)
            urls = self._upload(record.pop('paths'))
            record.update({'urls': urls})
        except Exception:
            self.logger.exception('Pulling for ID {}\n'.format(catalogID))
            
        return record

    def _upload(self, paths):
        """Upload staged image files to the bucket.

        Argument paths:  List of local paths to staged images

        Output:  Files are uploaded to bucket and local copies removed.
        
        Returns:  List of bucket urls.
        """
        urls = []
        for path in paths:
            try:
                url = self.bucket_tool.upload_blob(path, os.path.split(path)[1])
                urls.append(url)
            except Exception as e:
                self.logger.exception('Bucket error for {}\n'.format(path))
            os.remove(path)
        print('Uploaded images:\n{}\n'.format(urls), flush=True)
        return urls

    def _enforce_size_specs(self, bbox, provider):
        """Resize bbox if necesssary to make dimensions conform to size specs.

        Argument: shapely box

        Returns: shapely box
        """
        size_params = set(['min_size', 'max_size', 'bbox_rescaling'])
        if not size_params.intersection(self.image_specs.keys()):
            return bbox
        
        delx, dely = geobox.get_side_distances(bbox)
        try:
            rescaling = self.image_specs['bbox_rescaling']
            delx *= rescaling
            dely *= rescaling
        except KeyError:
            pass
        try:
            min_size = self.image_specs['min_size'][provider]
            if delx < min_size:
                delx = min_size
            if dely < min_size:
                dely = min_size
        except KeyError:
            pass
        try:
            max_size = self.image_specs['max_size'][provider]
            if delx > max_size:
                delx = max_size
            if dely > max_size:
                dely = max_size
        except KeyError:
            pass
                
        lon, lat = bbox.centroid.x, bbox.centroid.y
        deltalat = conversions.latitude_from_dist(dely)
        deltalon = conversions.longitude_from_dist(delx, lat) 
        bbox = geobox.make_bbox(lat, lon, deltalat, deltalon)

        return bbox

        
class StoryHandler(GrabberHandler):
    """Descendant class to pull images for stories in the WTL database.

    Descendant methods:
        async pull_for_story: pull images for all bboxes in a DBItem story.
        async pull_for_wire: Pull images for stories in a database.
    """
    def __init__(self,
                 bucket_name,
                 specs_filename=os.path.join(os.path.dirname(__file__),
                                             'default_story_specs.json'),
                 **image_specs):

        super().__init__(bucket_name,
                         specs_filename=specs_filename,
                         **image_specs)

    async def pull_for_story(self, story, **image_specs):
        """Pull images for all bboxes in a DBItem story."""
        
        print('Story: {}\n'.format(story.idx))
        try:
            core_locations = story.record['core_locations']
        except KeyError:
            print('No locations found.\n')
            return []
        
        image_records = []
        for name, data in core_locations.items():
            bbox = geometry.box(*data['boundingbox'])
            records = await self.pull(bbox, **image_specs)
            core_locations[name].update({'images': records})
            image_records += records
                
        story.record.update({'core_locations': core_locations})
        story_record = STORY_SEEDS.put_item(story)
        if not story_record:
            raise Exception('Posting image records to DB: {}\n'.format(
                image_records))
                
        return image_records
    
    async def pull_for_wire(self,
                      db=STORY_SEEDS,
                      category=DB_CATEGORY,
                      wireStartDate=WIRE_START_DATE,
                      wireEndDate=WIRE_END_DATE):
        """Pull images for stories in database between given dates.

        Arguments:
            db: a firebasio.DB instance
            category: a primary key in db
            wireStartDate, wireEndDate: isoformat earliest/latest publication
                dates for stories
                
        Output: Story records are updated with image records and reposted to
            the database.  

        Returns: None
        """
        # signal.signal(signal.SIGINT, log_utilities.signal_handler)
        
        if self.bucket_tool.bucket.name != WIRE_BUCKET:
            warning = 'Warning: Initialized bucket {} is not {}.'.format(
                self.bucket_tool.bucket.name, WIRE_BUCKET)
            self.logger.warning(warning)
            print(warning)

        stories = db.grab_stories(category=category,
                                  startDate=wireStartDate,
                                  endDate=wireEndDate)
        for s in stories:
            try:
                if self._check_for_images(s.record['core_locations']):
                    continue
            except KeyError:
                continue
            try: 
                image_records = await self.pull_for_story(s)
            except Exception as e:
                self.logger.exception('Pulling for story {}\n'.format(s.idx))
        print('complete')
        return

    def _check_for_images(self, core_locations):
        """Check whether images have been posted to core_locations."""
        for data in core_locations.values():
            if 'images' in data.keys():
                return True
        return False


class GeoJSONHandler(GrabberHandler):
    """Descendant class to pull images for geojsons in a FeatureCollection.

    Descendant method:
        async pull_for_geojson: Pull images for geojsons in a FeatureCollection.
    """
    def __init__(self, bucket_name, specs_filename=None, **image_specs):

        super().__init__(bucket_name,
                         specs_filename=specs_filename,
                         **image_specs)

    async def pull_for_geojson(self, features_filename):
        """Pull images for geojsons in a FeatureCollection.

        Argument:
            features_fname: name of file containing GeoJSON FeatureCollection

        Output: Adds image records to the FeatureCollection and writes it
            to file.
            
        Returns: A json dump of the FeatureCollection.
        """

        # signal.signal(signal.SIGINT, log_utilities.signal_handler)

        with open(features_filename, 'r') as f:
            geojsons = json.load(f)

        for feature in geojsons['Features']:
            if 'properties' not in feature.keys():
                feature.update({'properties': {}})
            if 'images' not in feature['properties'].keys():
                feature['properties'].update({'images': []})
            try:
                polygon = geometry.asShape(feature['geometry'])
                bbox = geometry.box(*polygon.bounds)
                records = await self.pull(bbox, **feature['properties'])
            except Exception as e:
                self.logger.exception('Pulling for bbox {}\n'.format(
                    bbox.bounds))
                records = []
            feature['properties']['images'] += records
        
        output_fname = features_filename.split('.json')[0] + '-images.json'
        with open(output_fname, 'w') as f:
            json.dump(geojsons, f, indent=4)
        print('complete')
        return json.dumps(geojsons)
