"""Routines to grab Landsat thumbnails.

This module differs from others in the package in that it pulls from the
earthrise-assets web app instead of a provider directly. Nonetheless,
and at some cost in internal logic of the code, it mirrors key structures
of the other modules so it can be managed via wrappers.py,
and ultimately, the earthrise-imagery web app. 

Class LandsatThumbnails: A class to pull and color correct images.

Usage with default specs:
> bbox = geobox.bbox_from_scale(-122.42, 37.77, 40.0)
> lt = LandsatThumbnails()
> lt(bbox)

"""

import asyncio
import datetime
import io
import json
import os

import aiohttp
import dateutil
import numpy as np
import skimage

from geobox import geobox
from postprocessing import color

DEFAULT_SPECS_FILE = os.path.join(os.path.dirname(__file__),
                                  'default_specs.json')

class LandsatThumbnails(object):
    """Pull Landsat thumbnails from the earthrise-assets web app.

    Attributes:
        app_url: base url for earthrise-assets web app
        specs: dict of catalog and image specs 
    
    Method:
        grab_scene: Retrieve and color correct the images.
    """
    def __init__(self,
                 app_url='http://earthrise-assets.herokuapp.com/nasa/image',
                 specs_filename=DEFAULT_SPECS_FILE,
                 default_styles_override=['landsat'],
                 **specs):

        self.app_url = app_url
        with open(specs_filename, 'r') as f:
            self.specs = json.load(f)
        self.specs['write_styles'] = default_styles_override
        self.specs.update(specs)

    def __call__(self, bbox):
        """Scheduling wrapper for async execution of pull()."""
        loop = asyncio.get_event_loop()
        recs_written = loop.run_until_complete(self.pull(bbox))
        return recs_written

    async def pull(self, bbox):
        """Grab the most recent available images consistent with specs.
    
        Argument: bbox: a shapely box
            
        Returns: List of records of written images
        """
        scenes = self.prep_scenes(bbox)
        grab_tasks = [
            asyncio.ensure_future(self.grab_scene(bbox, scene))
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
    
    def prep_scenes(self, *args):
        """Prepare a list of dates defining the scenes.
        
        On earthrise-assets, a Landsat scene is a 90-day composite
            defined by an endDate. *args is a placeholder for compatibility
            with other modules in this package.

        Returns: list of isoformat dates
        """
        if self.specs['endDate']:
            enddates = [self.specs['endDate']]
        else:
            enddates = [datetime.date.today().isoformat()]

        while len(enddates) < self.specs['N_images']:
            earlier = (dateutil.parser.parse(enddates[-1]) -
                       datetime.timedelta(days=self.specs['skip_days']))
            enddates.append(earlier.date().isoformat())

        return enddates
    
    async def grab_scene(self, bbox, enddate):
        """Retrieve and reprocess scene assets.

        Arguments:
            bbox: a shapely box
            enddate: an isoformat date

        Returns: dict record, including 'paths' to images
        """
        path, record = await self._retrieve(bbox, enddate)
        output_paths = self.color_process(path)
        record.update({'paths': output_paths})
        return record

    # For compatability with earthrise-imagery web app and grabber_handlers:
    async def grab_by_id(self, *args, **kwargs):
        return {}
        
    def search_id(self, *args, **kwargs):
        return self.search_latlon_clean()
        
    def search_latlon_clean(self, *args, **kwargs):
        return 'For Landsat only the pull method is available.'
    
    async def _retrieve(self, bbox, enddate):
        """Pull image from the web app.

        Returns: path to image and scene record
        """
        payload = {
            'lat': '{:.4f}'.format(bbox.centroid.y),
            'lon': '{:.4f}'.format(bbox.centroid.x),
            # The scale parameter accepted by earthrise-assets is a float
            # in range [0, 2.8], which corresponds roughly (or possibly
            # exactly?) to the number of hundreds of km of the box side.
            'scale': '{:.2f}'.format(
                np.mean(geobox.get_side_distances(bbox))/100),
            'end': enddate
        }
        path = (self.specs['file_header'] +
                ''.join(k+v for k,v in payload.items()) + '.tif')
                
        async with aiohttp.ClientSession() as session:
            async with session.get(self.app_url,
                                   params=payload,
                                   allow_redirects=True) as response:
                record = await response.json(content_type=None)
                img_url = record.pop('url')
            async with session.get(img_url) as img_response:
                bin_img = await img_response.read()

        # Save via skimage to get a 3-band PNG
        img = skimage.io.imread(io.BytesIO(bin_img))
        skimage.io.imsave(path, img)

        return path, record

    def color_process(self, path):
        """Correct color, producing mutliple versions of the image.

        Returns: Paths to color-corrected images.
        """
        output_paths = []
        styles = [style.lower() for style in self.specs['write_styles']
                  if style in color.STYLES]

        for style in styles:
            outpath = color.ColorCorrect(style=style)(path)
            output_paths.append(outpath)

        if self.specs['thumbnails']:
            os.remove(path)
        else:
            output_paths.append(path)
        return output_paths
    
