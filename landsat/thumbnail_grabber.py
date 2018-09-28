"""Routines to grab Landsat thumbnails from the earthrise-assets
web app and upload to Google cloud storage.

Class ThumbnailGrabber:  Pull Landsat thumbnails from a web app and upload to
cloud storage.

The routine handles incoming http requests asynchronously, mostly to conform
with story_seeds/thumbnails/request_thumbnails.py.  It must be
scheduled in an event loop and passed an aiohttp.ClientSession().
A convenience wrapper to do this is main():
> loop = asycnio.get_event_loop()
> loop.run_until_complete(main(lat, lon))

"""

import aiohttp
from datetime import date
from datetime import timedelta
import io
import json
import os

import requests
import skimage.io

from utilities import cloud_storage
from postprocessing import color

CATALOG_PARAMS = {
    'landsat_scale': .4,
    'days': 90
}
WEBAPP_URL = 'http://earthrise-assets.herokuapp.com/nasa/image'
STAGING_DIR = os.path.join(os.path.dirname(__file__), 'tmp-imgs')
BUCKET_TOOL = cloud_storage.BucketTool('landsat-thumbnails')

class ThumbnailGrabber(object):
    """Pull Landsat thumbnails from a web app and upload to cloud
    storage.

    Attributes:
        base_url: web app url base
        staging_dir: directory for temporary writing of images
        postprocessor: function to color correct image (or None)
        bucket_tool: class instance to access Google Cloud storage bucket
        logger: logging.GetLogger() instance (or None)
    
    Method:
        source_and_post: pull thumbnails and post to cloud storage
    """
    def __init__(
        self,
        base_url=WEBAPP_URL,
        staging_dir=STAGING_DIR,
        postprocessor=color.ColorCorrect(
            cut_frac=.75, gamma=.75, return_ubyte=False).enhance_contrast,
        bucket_tool=BUCKET_TOOL,
        logger=None):
        
        self.base_url = base_url
        if not os.path.exists(staging_dir):
            os.makedirs(staging_dir)
        self.staging_dir = staging_dir
        self.postprocessor = postprocessor
        self.bucket_tool = bucket_tool
        self.logger = logger
        
    async def __call__(self, session, lat, lon, N_images=4,
                       params=CATALOG_PARAMS):
        """Pull thumbnails and post to cloud storage

        Arguments:
            session: aiohttp.ClientSession() instance
            lat/lon: float latitude and longitude
            N_images: Number of images to pull, in time increments
                specified by params['days']
            params: Dict containing time increment (key: 'days')
                and image distance scale (key: 'landsat_scale')

        Returns: list of urls to thumbnails on cloud storage 
        """
        thumbnail_urls = []
        payload = {
            'lat': '{:.4f}'.format(lat),
            'lon': '{:.4f}'.format(lon),
            'scale': '{:.1f}'.format(params['landsat_scale'])
        }    
        for n in range(N_images):
            endDate = date.today() - timedelta(days=n*params['days'])
            startDate = (date.today() -
                         timedelta(days=(n+1)*params['days']))
            payload.update({
                'begin': startDate.isoformat(),
                'end': endDate.isoformat()
            })
            try: 
                img_path = await self._save_image(session, payload)
                url = self.bucket_tool.upload_blob(img_path,
                                         os.path.split(img_path)[1])
                thumbnail_urls.append(url)
                os.remove(img_path)
            except Exception as e:
                if self.logger is None:
                    print(repr(e))
                    print('Continuing...')
                else:
                    self.logger.exception(e)
        return thumbnail_urls

    async def _save_image(self, session, payload):
        """Pull image from web app, postprocess, and save to staging_dir.

        Arguments:
            payload: dict containing, at minimum, scene lat, lon, scale.

        Returns: path to image
        """
        filename = ''.join(k+v for k,v in payload.items()) + '.png'
        path = os.path.join(self.staging_dir, filename)
        async with session.get(self.base_url,
                         params=payload,
                         allow_redirects=True) as response:
            img_data = await response.json(content_type=None)
            img_url = img_data['url']
        async with session.get(img_url) as img_response:
            bin_img = await img_response.read()
        if self.postprocessor:
            img = skimage.io.imread(io.BytesIO(bin_img))
            corrected = self.postprocessor(img)
            skimage.io.imsave(path, corrected)
        else:
            with open(path, 'wb') as f:
                f.write(bin_img)
        return path

# Session handling wrapper. To call within an asyncio event loop.
async def main(lat, lon):
    async with aiohttp.ClientSession() as session:
        grabber = ThumbnailGrabber()
        thumbnail_urls = await grabber(session, lat, lon)
        return thumbnail_urls
