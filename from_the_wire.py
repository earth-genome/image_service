# Routines to pull imagery for stories from the news wire.

import datetime
import os
import signal
import sys

import numpy as np
from shapely import geometry

sys.path.append('../')
sys.path.append('../story-seeds')
import config  # story-seeds config
from digital_globe import dg_grabber
import firebaseio
from geobox import geobox
from logger import log_exceptions

STORY_SEEDS = firebaseio.DB(config.FIREBASE_URL)
DB_CATEGORY = '/WTL'
IMAGE_GRABBER = dg_grabber.DGImageGrabber()

IMAGE_DIR = 'WTLImages' + datetime.date.today().isoformat()
EXCEPTION_DIR = 'FTWexception_logs'

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
    """
    
    def __init__(self,
                 image_grabber=IMAGE_GRABBER,
                 size_specs=IMAGE_SIZE_SPECS,
                 style_specs=IMAGE_STYLE_SPECS,
                 image_dir=IMAGE_DIR):
        self.image_grabber = image_grabber
        self.size_specs = size_specs
        self.style_specs = style_specs
        self.image_dir = image_dir
        if not os.path.exists(image_dir):
            os.makedirs(image_dir)

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
        
        Returns: list of image filenames
        """

        written_images = []
        except_log = ''
    
        def signal_handler(*args):
            print('KeyboardInterrupt: Writing logs before exiting...')
            log_exceptions(except_log, directory=EXCEPTION_DIR)
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        stories = db.grab_stories(category=category, startDate=startDate)
        stories = [s for s in stories
                   if 'core_locations' in s.record.keys()]
        for s in stories:
            print('Pulling images for: {}\n'.format(s.idx))
            image_records, log = self.pull_for_story(s)
            written_images.append(image_records)
            except_log += log

        log_exceptions(except_log, directory=EXCEPTION_DIR)
        print('complete')
        return written_images

    def pull_for_story(self, story):
        """Pull images for all locations with bounding boxes in story.

        Argument:  A firebaseio.DBItem instance

        Output:  Images written to disk

        """
        image_records = []
        except_log = ''
        for loc_name, loc_data in story.record['core_locations'].items():
            if 'osm' in loc_data.keys():
                for n, osm in enumerate(loc_data['osm']):
                    bbox = geobox.osm_to_shapely_box(osm['boundingbox'])
                    bbox = self._enforce_size_specs(bbox)
                    print('Location: {}\n'.format(loc_name))
                    print('Bbox size: {:.2f} km x {:.2f} km\n'.format(
                        *geobox.get_side_distances(bbox)))
                    try:
                        # TODO: eventually image_grabber should throw
                        # or log exceptions
                        imgs, recs = self.image_grabber(
                            bbox,
                            N_images=self.style_specs['N_images'],
                            write_styles=self.style_specs['write_styles'],
                            file_header=os.path.join(
                                self.image_dir,
                                loc_name+str(n+1)+'-'))
                        image_records.append(recs)
                    except Exception as e:
                        except_log += 'Story {}, Location {}\n'.format(
                            story.idx, loc_name)
                        except_log += 'Exception: {}'.format(repr(e))
        return image_records, except_log

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
        deltalat = geobox.latitude_from_dist(dely)
        deltalon = geobox.longitude_from_dist(delx, lat) 
        bbox = geobox.make_bbox(lat, lon, deltalat, deltalon)
        return bbox
    

