"""Classes for high-level management of image-grabbing processes.

--- --- 
WIP:  Written to run in a Flask web app, the async routines here
have no event loops assigned a priori. To run any async method, simply edit
this code to decorate it with @loop, or at runtime, create a scheduled version
of the function by passing it through loop() explicitly.
E.g. to pull for a bbox: 

> g = GrabberHandler(bucket_name=bucket_name, specs_filename='specs.json',
                     **more_image_specs)
> looped = loop(g.pull)
> records = looped(bbox, **override_image_specs)

Running from the interpreter, however, raises some issues with clean
shutdown on KeyboardInterrupt.  See loop() below.  
--- ---

Usage:

To pull for a GeoJSON FeatureCollection:
> g = GeoJSONGrabber(bucket_name=bucket_name, specs_filename='specs.json',
                      **more_image_specs)
> updated_feature_collection = g.pull_for_geojson(features_filename)

(The FeatureCollection itself may contain image_specs which will override those
initialized.)  

To pull for the news wire:
> sg = StoryGrabber(bucket_name=WIRE_BUCKET, specs_filename='specs.json', **more_image_specs)
> sg.pull_for_wire()

To pull for a single DBItem story:
> sg = StoryGrabber(bucket_name=bucket_name, specs_filename='specs.json',
                    **more_image_specs)
> records = sg.pull_for_story(story, **override_image_specs)

To pull for a shapely bbox:
> g = GrabberHandler(bucket_name=bucket_name, specs_filename='specs.json',
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
    "N_images": 1  # Number of images to pull for each bbox
    "write_styles": [  # Defined in postprocessing.color
        "matte",       
        "contrast",
        "dra",
        "desert"
    ],
    "landcover_indices": [],
    "thumbnails": false,
    "file_header": ""
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

from digital_globe import dg_grabber
from landsat import landsat_grabber
from planet_labs import planet_grabber
from utilities import cloud_storage
from utilities import firebaseio
from utilities import log_utilities
from utilities.geobox import geobox
from utilities.geobox import conversions


PROVIDER_CLASSES = {
    'digital_globe': dg_grabber.DGImageGrabber,
    'planet': planet_grabber.PlanetGrabber,
    'landsat': landsat_grabber.LandsatThumbnails
}

# For staging, en route to bucket
STAGING_DIR = os.path.join(os.path.dirname(__file__), 'tmp-staging')

# News wire
STORY_SEEDS = firebaseio.DB(firebaseio.FIREBASE_URL)
DB_CATEGORY = '/WTL'

WIRE_START_DATE = (datetime.date.today()-datetime.timedelta(days=3)).isoformat()
WIRE_END_DATE = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()

WIRE_BUCKET = 'newswire-images'

DEFAULT_BUCKET = 'bespoke-images'

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
        bucket_tool: class instance to access Google Cloud storage bucket
        logger: a Python logging.getLogger instance
        image_specs: dict of catalog and image specs
    """
    
    def __init__(self,
                 bucket_name=DEFAULT_BUCKET,
                 providers=['planet'],
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

        if not os.path.exists(staging_dir):
            os.makedirs(staging_dir)
        try: 
            self.bucket_tool = cloud_storage.BucketTool(bucket_name)
        except Exception as e:
            print('Bucket name not recognized: {}'.format(repr(e)))
            raise

        self.logger = log_utilities.get_stream_logger(log_dest)

        with open(specs_filename, 'r') as f:
            self.image_specs = json.load(f)
        self.image_specs.update(image_specs)
        self.image_specs.update({'file_header': os.path.join(staging_dir, '')})

    async def pull(self, bbox, **image_specs):
        """Pull images for bbox.

        Arguments:
            bbox: a shapely box
            image_specs: to override values in self.image_specs

        Output:  Images written posted to cloud storage bucket.

        Returns: List of image records (including bucket urls to images).
        """
        specs = self.image_specs.copy()
        specs.update(image_specs)
        grab_tasks = []
        
        for provider, grabber_class in self.provider_classes.items():
            grabber = grabber_class(**specs)
            print('Pulling for bbox {}.\n'.format(conforming_bbox.bounds))
            scenes = grabber.prep_scenes(conforming_bbox)

            grab_tasks += [
                asyncio.ensure_future(
                    grabber.grab_scene(conforming_bbox, scene))
                for scene in scenes
            ]

        recs_written = []
        for task in asyncio.as_completed(grab_tasks):
            print('Task: {}'.format(task))
            try: 
                written = await task
                print('Task returned.  Uploading.')
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
            record = await grabber.grab_by_id(bbox, catalogID, item_type)
            urls = self._upload(record.pop('paths'))
            record.update({'urls': urls})
        except Exception:
            self.logger.exception('Pulling for ID {}\n'.format(catalogID))
            record = {}
            
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

class GeoJSONHandler(GrabberHandler):
    """Descendant class to pull images for geojsons in a FeatureCollection.

    Descendant method:
        async pull_for_geojson: Pull images for geojsons in a FeatureCollection.
    """
    def __init__(self, **specs):
        super().__init__(**specs)

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

        for feature in geojsons['features']:
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
