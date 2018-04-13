"""Routines to grab Landsat thumbnails from the earthrise-assets
web app and upload to Google cloud storage.

Class ThumbnailGrabber:  Pull Landsat thumbnails from a web app and upload to cloud storage.

Usage with default parameters:
> grabber = ThumbnailGrabber()
> grabber.source_and_post(lat, lon)

"""

from datetime import date
from datetime import timedelta
import io
import json
import os

import matplotlib.pyplot as plt
import requests

from cloud_storage import Bucketer
from postprocessing.color_correct import ColorCorrect

CATALOG_PARAMS = {
    'landsat_scale': .4,
    'days': 90
}
WEBAPP_URL = 'http://earthrise-assets.herokuapp.com/nasa/image'
STAGING_DIR = os.path.join(os.path.dirname(__file__), 'tmp-imgs')

class ThumbnailGrabber(object):
    """Pull Landsat thumbnails from a web app and upload to cloud
    storage.

    Attributes:
        base_url: web app url base
        staging_dir: directory for temporary writing of images
        postprocessor: function to color correct image (or None)
        bucket: Google Cloud storage bucket
        logger: logging.GetLogger() instance (or None)
    
    Method:
        source_and_post: pull thumbnails and post to cloud storage
    """
    def __init__(self,
                 base_url=WEBAPP_URL,
                 staging_dir=STAGING_DIR,
                 postprocessor=ColorCorrect().brightness_and_contrast,
                 bucket=Bucketer('landsat-thumbnails'),
                 logger=None):
        self.base_url = base_url
        if not os.path.exists(staging_dir):
            os.makedirs(staging_dir)
        self.staging_dir = staging_dir
        self.postprocessor = postprocessor
        self.bucket = bucket
        self.logger = logger
        
    def source_and_post(self, lat, lon, N_images=4,
                        params=CATALOG_PARAMS):
        """Pull thumbnails and post to cloud storage

        Arguments:
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
                img_path = self._save_image(payload)
                url = self.bucket.upload_blob(img_path,
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

    def _save_image(self, payload):
        """Pull image from web app, postprocess, and save to staging_dir.

        Arguments:
            payload: dict containing, at minimum, scene lat, lon, scale.

        Returns: path to image
        """
        filename = ''.join(k+v for k,v in payload.items()) + '.png'
        path = os.path.join(self.staging_dir, filename)
        res = requests.get(self.base_url,
                           params=payload,
                           allow_redirects=True)
        img_url = json.loads(res.text)['url']
        res = requests.get(img_url)
        if self.postprocessor:
            img = plt.imread(io.BytesIO(res.content))
            corrected = self.postprocessor(img)
            plt.imsave(path, corrected)
        else:
            with open(path, 'wb') as f:
                f.write(res.content)
        return path
