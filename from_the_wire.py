""" Class to pull imagery for stories from the news wire.

The wire database is fixed in variables STORY_SEEDS and DB_CATEGORY.
The imagery source is fixed (for now, single source only) via
    IMAGE_GRABBER.

Class PullForWire: Class to pull images for stories on the news wire.

Usage with defaults: 
> pfw = PullForWire()
> pfw.pull_for_db()

For a single story, given as a firebaseio.DBItem instance story:
> pfw.pull_for_story(story)

Primary output: images written to disk.
"""

import datetime
import logging
from logging import handlers
import os
import signal
import sys

import numpy as np
from shapely import geometry

import config 
from digital_globe import dg_grabber
from geobox import geobox
from geobox import conversions
sys.path.append('story-seeds/')
import firebaseio

STORY_SEEDS = firebaseio.DB(config.FIREBASE_URL)
DB_CATEGORY = '/WTL'
IMAGE_GRABBER = dg_grabber.DGImageGrabber()

IMAGE_DIR = 'WTLImages' + datetime.date.today().isoformat()
EXCEPTIONS_DIR = 'FTWexception_logs'
LOGFILE = 'FTW.log'

# See the particular image_grabber for additional parameters
IMAGE_SIZE_SPECS = {
    'bbox_rescaling': 2, # linear scaling of image relative to object bbox
    'min_size': .5, # minimum linear size for image in km
    'max_size': 10 # maximum linear size for image in km - TODO: add other image grabbers for larger scales
}

# WIP:  N_images should eventually vary with story content (change vs.
# static illustration); DG write_styles supported are 'DRA' and/or 'Raw'.
IMAGE_STYLE_SPECS = {
    'N_images': 1,
    'write_styles': ['DRA']
}


class PullForWire(object):
    """Class to pull images for stories on the news wire.

    Public Methods:
        pull_for_db: pull images for all stories from given start date
            for given database
        pull_for_story: pull images for all OSM records in story

    Attributes:
        image_grabber: function to pull images for an input bbox;
            specifies implicitly an image source and image specs
        size_specs: dict specifying relationships between object bbox
            and image size.
        style_specs: dict for image number and processing specs
        image_dir: directory to write images
        logger: logging.getLogger instance
    """
    
    def __init__(self,
                 image_grabber=IMAGE_GRABBER,
                 image_dir=IMAGE_DIR,
                 **specs):
        self.image_grabber = image_grabber
        self.image_dir = image_dir
        if not os.path.exists(image_dir):
            os.makedirs(image_dir)
        self.size_specs = IMAGE_SIZE_SPECS.copy()
        self.style_specs = IMAGE_STYLE_SPECS.copy()
        for k,v in specs.items():
            if k in self.size_specs.keys():
                self.size_specs.update({k:v})
            if k in self.style_specs.keys():
                self.style_specs.update({k:v})
        self.logger = _build_logger()

    def pull_for_db(self,
                    db=STORY_SEEDS,
                    category=DB_CATEGORY,
                    startDate=datetime.date.today().isoformat()):
        """Pull images for all stories in database from startDate.

        Arguments:
            db: a firebasio.DB instance
            category: a primary key in db
            startDate: isoformat earliest publication date for stories

        Output: images written to disk
        
        Returns: list of image records
        """

        written_images = []
        signal.signal(signal.SIGINT, _signal_handler)
        
        stories = db.grab_stories(category=category, startDate=startDate)
        stories = [s for s in stories
                   if 'core_locations' in s.record.keys()]
        for s in stories:
            print('Pulling images for: {}\n'.format(s.idx))
            image_records = self.pull_for_story(s)
            written_images.append(image_records)

        print('complete')
        return written_images

    def pull_for_story(self, story):
        """Pull images for all locations with bounding boxes in story.

        Argument:  A firebaseio.DBItem instance

        Output:  Images written to disk

        Returns: list of image records
        """
        image_records = []
        for loc_name, loc_data in story.record['core_locations'].items():
            if 'osm' in loc_data.keys():
                for n, osm in enumerate(loc_data['osm']):
                    bbox = geobox.osm_to_shapely_box(osm['boundingbox'])
                    bbox = self._enforce_size_specs(bbox)
                    print('Location: {}\n'.format(loc_name))
                    print('Bbox size: {:.2f} km x {:.2f} km\n'.format(
                        *geobox.get_side_distances(bbox)))
                    try:
                        imgs, recs = self.image_grabber(
                            bbox,
                            N_images=self.style_specs['N_images'],
                            write_styles=self.style_specs['write_styles'],
                            file_header=os.path.join(
                                self.image_dir,
                                loc_name+str(n+1)+'-'))
                        image_records.append(recs)
                    except Exception as e:
                        self.logger.error(
                            'Story {}, Loc {}\n'.format(
                                story.idx, loc_name),
                            exc_info=True)
        return image_records

    def _enforce_size_specs(self, bbox):
        """Resize bbox if necesssary to make dimensions conform
        to self.size_specs.

        Argument: shapely box

        Returns: shapely box
        """
        min_size = self.size_specs['min_size']
        max_size = self.size_specs['max_size']
        delx, dely = geobox.get_side_distances(bbox)
        delx *= self.size_specs['bbox_rescaling']
        dely *= self.size_specs['bbox_rescaling']
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
        return bbox


def _build_logger(directory=EXCEPTIONS_DIR, logfile=LOGFILE):
    logger = logging.getLogger(__name__)
    if not os.path.exists(directory):
        os.makedirs(directory)
    trfh = handlers.TimedRotatingFileHandler(
        os.path.join(directory, logfile), when='D')
    logger.addHandler(trfh)
    return logger

def _signal_handler(*args):
    print('KeyboardInterrupt: Writing logs before exiting...')
    logging.shutdown()
    sys.exit(0)

if __name__ == '__main__':
    try:
        startDate = sys.argv[1]
    except IndexError:
        print("Using today's date as default startDate for " +
            "pulling stories.")
        print('Optionally, you may specify a date: ' +
              'python from_the_wire.py 2018-03-28')
        startDate = datetime.date.today().isoformat()
    puller = PullForWire()
    puller.pull_for_db(startDate=startDate)
