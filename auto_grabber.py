"""Classes for automated pulling of imagery.

Inputs are bounding boxes, dates, and numbers of images, which condition
imagery source tried.  

The imagery sources are fixed via PROVIDERS.

The wire database is fixed in variables STORY_SEEDS and DB_CATEGORY.


Class PullForWire: Class to pull images for stories on the news wire.

Usage:

# WIP

Image specs determine additional image parameters and are passed through
auto_grabber.py to individual (Digital Globe, Planet, etc.) grabbers,
where their particular use is defined.  As of writing,
default_image_specs.json contains:
{
    "clouds": 10,
    "min_intersect": 0.9,
    "startDate": "2008-09-01T00:00:00.0000Z",
    "endDate": null,
    "bbox_rescaling": 2,
    "min_size": 0.5,
    "max_size": 10,
    "N_images": 1
}

Outputs:

If bucket_name is specified images are uploaded to the corresponding cloud
storage bucket; otherwise they are saved locally to disk. Typically,
the remote paths (i.e. urls) or local paths are returned by image pulling
functions.

If there is a bucket, in the case of pull_for_geojson, the urls are added as
'properties' with key 'images' to the geojson features and a new
FeatureCollection is written to geojsonfile-images.json.  For
pull_for_story and pull_for_wire, the story is updated with the urls, which is
then reposted to the database.
"""

import datetime
import json
import os
import signal
import sys

import numpy as np
from shapely import geometry

import config
import cloud_storage
from digital_globe import dg_grabber
from geobox import geobox
from geobox import conversions
sys.path.append('story-seeds/')
import firebaseio
import log_utilities

PROVIDERS = {
    'digital_globe': {
        'grabber': dg_grabber.DGImageGrabber(),
        'write_styles': ['DRA'] #['GeoTiff']
    },
    'planet': {
        'grabber': None # WIP
    }
}

# Default image specs:
with open('default_image_specs.json', 'r') as f:
    DEFAULT_IMAGE_SPECS = json.load(f)

# For staging (en route to bucket) or local storage of image files:
IMAGE_DIR = os.path.join(os.path.dirname(__file__),
                         'Images' + datetime.date.today().isoformat())

# News wire
STORY_SEEDS = firebaseio.DB(config.FIREBASE_URL)
DB_CATEGORY = '/WTL'

WIRE_START_DATE = (datetime.date.today()-datetime.timedelta(days=3)).isoformat()
WIRE_END_DATE = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()

WIRE_BUCKET = 'newswire-images'


class AutoGrabber(object):
    """Class to pull images.

    Public Methods:
        pull: pull images for boundingbox

    Attributes:
        image_grabbers: functions to pull images (from modules in this repo)
        image_dir: directory for local staging or storage of images
        image_specs: dict of catalog search and image size specs
        bucket_tool: class instance to access Google Cloud storage bucket
            (default None, for local storage of images)
    """
    
    def __init__(self,
                 providers=PROVIDERS,
                 image_dir=IMAGE_DIR,
                 bucket_name=None,
                 **image_specs):
        self.providers = providers
        self.image_dir = image_dir
        if not os.path.exists(self.image_dir):
            os.makedirs(self.image_dir)
        if bucket_name:
            try: 
                self.bucket_tool = cloud_storage.BucketTool(bucket_name)
            except Exception as e:
                print('Bucket name not recognized: {}'.format(repr(e)))
                raise
        else:
            self.bucket_tool = None
        self.image_specs = DEFAULT_IMAGE_SPECS
        self.image_specs.update(image_specs)

    def pull(self, bbox, **image_specs):
        """Pull images for bbox (and optionally, post to bucket).

        Arguments:
            bbox: a shapely box
            image_specs: to override values in self.image_specs

        Output:
            Images written to self.image_dir or posted to cloud storage bucket.

        Returns: List of local or remote paths to images.
        """
        specs = self.image_specs.copy()
        specs.update(image_specs)
        bbox, providers = self._enforce_size_specs(bbox)

        local_paths = []
        while len(local_paths) < specs['N_images'] and providers:
            provider = providers.pop()
            grabber = provider['grabber']
            print('Pulling for bbox {}.\n'.format(bbox.bounds))
            paths = grabber(bbox,
                            N_images=specs['N_images'],
                            write_styles=provider['write_styles'],
                            file_header=os.path.join(self.image_dir, ''))[-1]
            local_paths += paths
        print('Pulled {} image(s).\n'.format(len(local_paths)))
        
        if self.bucket_tool:
            urls = []
            for path in local_paths:
                try:
                    url = self.bucket_tool.upload_blob(path,
                                                       os.path.split(path)[1])
                except Exception as e:
                    print('Bucket error for {}: {}\n'.format(path, repr(e)))
                    self.logger.exception('Bucket error for {}'.format(path))
                os.remove(path)        
                urls.append(url)
            return urls
        
        return local_paths

    def pull_for_story(self, story, **image_specs):
        """Pull images for a DBItem story."""
        
        print('Story: {}\n'.format(story.idx))
        try:
            core_locations = story.record['core_locations']
        except KeyError:
            print('No locations found.\n')
            return []
        image_paths = []
        for data in core_locations.values():
            bbox = geometry.box(data['boundingbox'])
            image_paths += self.pull(bbox, **image_specs)
        if image_paths and self.bucket_tool:
            story.record.update({'images': image_paths})
            record = story.put_item()
            if not record:
                self.logger.error('Error posting paths to DB: {}\n'.format(
                    image_paths))
        return image_paths

    def _enforce_size_specs(self, bbox):
        """Resize bbox if necesssary to make dimensions conform
        to self.size_specs.

        Argument: shapely box

        Returns: shapely box
        """
        min_size = self.image_specs['min_size']
        max_size = self.image_specs['max_size']
        delx, dely = geobox.get_side_distances(bbox)
        delx *= self.image_specs['bbox_rescaling']
        dely *= self.image_specs['bbox_rescaling']
        if delx < min_size:
            delx = min_size
        elif delx > max_size:
                delx = max_size
        if dely < min_size:
             dely = min_size
        elif dely > max_size:
            dely = max_size
        lon, lat = bbox.centroid.x, bbox.centroid.y
        deltalat = conversions.latitude_from_dist(dely)
        deltalon = conversions.longitude_from_dist(delx, lat) 
        bbox = geobox.make_bbox(lat, lon, deltalat, deltalon)

        # WIP: providers to be ordered according to bbox size
        return bbox, [self.providers['digital_globe']]

        
class BulkGrabber(AutoGrabber):

    def __init__(self, bucket_name=None, **image_specs):

        super().__init__(bucket_name=bucket_name, **image_specs)
        log_dir = os.path.join(os.path.dirname(__file__), 'AutoException_logs')
        log_filename = 'Auto' + datetime.date.today().isoformat() + '.log'
        self.logger = log_utilities.build_logger(log_dir,
                                                 log_filename,
                                                 logger_name='auto_grabber')

    def pull_for_geojson(self, features_filename):
        """Pull images for geojsons in a FeatureCollection.

        Argument:
            features_fname: name of file containing GeoJSON FeatureCollection

        Output:
            Adds local or remote paths to images to the FeatureCollection and
            writes it to file; returns a json dump of the FeatureCollection.
        """

        signal.signal(signal.SIGINT, log_utilities.signal_handler)

        with open(features_filename, 'r') as f:
            geojsons = json.load(f)
        for feature in geojsons['Features']:
            if 'properties' not in feature.keys():
                feature.update({'properties': {}})
            try:
                polygon = geometry.asShape(feature['geometry'])
                bbox = geometry.box(*polygon.bounds)
                paths = self.pull(bbox, **feature['properties'])
            except Exception as e:
                print('Pulling images: {}\n'.format(repr(e)))
                self.logger.exception('Pulling for bbox {}\n'.format(
                    bbox.bounds))
                paths = []
            feature['properties'].update({'images': paths})
        
        output_fname = features_filename.split('.json')[0] + '-images.json'
        with open(output_fname, 'w') as f:
            json.dump(geojsons, f)
        print('complete')
        return json.dumps(geojsons)

    def pull_for_wire(self,
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
                
        Output:
            If a cloud storage bucket is given, story records are updated
            with remote paths to images, and stories are reposted to the
            database.  
        
        Returns: list of lists of local or remote paths to images
        """
        signal.signal(signal.SIGINT, log_utilities.signal_handler)

        # error handling in case the wrong bucket is initialized??
        # how to limit file size if pulling to disk?  

        stories = db.grab_stories(category=category,
                                  startDate=wireStartDate,
                                  endDate=wireEndDate)
        stories = [s for s in stories if ('core_locations' in s.record.keys()
                                          and 'images' not in s.record.keys())]
        written_images = []
        for s in stories:
            paths = self.pull_for_story(s)
            written_images.append(paths)
        print('complete')
        return written_images
