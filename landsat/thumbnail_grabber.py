"""Routines to grab Landsat thumbnails from the earthrise-assets
web app and upload to Google cloud storage.

Class ThumbnailGrabber:  Pull Landsat thumbnails from a web app and upload to cloud storage.

Usage with default parameters:
> grabber = ThumbnailGrabber()
> grabber.source_and_post(lat, lon)

"""

from datetime import date
from datetime import timedelta
import json
import os

from google.cloud import storage
import requests

CATALOG_PARAMS = {
    'landsat_scale': .4,
    'days': 90
}
WEBAPP_URL = 'http://earthrise-assets.herokuapp.com/nasa/image'
GOOGLE_CLOUD_PROJECT = 'good-locations'
BUCKET_NAME = 'landsat-thumbnails'

class ThumbnailGrabber(object):
    """Pull Landsat thumbnails from a web app and upload to cloud
    storage.

    Attributes:
        base_url: web app url base
        storage_client: cloud storage client instance
        bucket: bucket within cloud storage
        image_dir: local image for temporary writing of images
        logger: logging.GetLogger() instance (or None)
    
    Method:
        source_and_post: pull thumbnails and post to cloud storage
    """
    def __init__(self,
                 base_url=WEBAPP_URL,
                 storage_project=GOOGLE_CLOUD_PROJECT,
                 bucket_name=BUCKET_NAME,
                 logger=None):
        self.base_url = base_url
        self.storage_client = storage.Client(project=storage_project)
        self.bucket = self.storage_client.get_bucket(bucket_name)
        self.image_dir = os.path.join(os.path.dirname(__file__),
                                      'tmp-imgs')
        if not os.path.exists(self.image_dir):
            os.makedirs(self.image_dir)
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
                img_path = save_image(self.base_url,
                                      payload,
                                      self.image_dir)
                thumbnail_url = upload_blob(self.bucket,
                                       img_path,
                                       os.path.split(img_path)[1])
                thumbnail_urls.append(thumbnail_url)
                os.remove(img_path)
            except Exception as e:
                if self.logger is None:
                    print(repr(e))
                    print('Continuing...')
                else:
                    self.logger.exception(e)
        return thumbnail_urls

def save_image(base_url, payload, image_dir):
    """Pull image from web app and save locally.

    Arguments:
        base: base url for source web app
        payload: dict containing, at minimum, scene lat, lon, scale.
        image_dir:  path to local directory to save image

    Returns: local path to image
    """
    filename = ''.join(k+v for k,v in payload.items()) + '.png'
    path = os.path.join(image_dir, filename)
    res = requests.get(base_url,
                       params=payload,
                       allow_redirects=True)
    img_url = json.loads(res.text)['url']
    res = requests.get(img_url)
    with open(path, 'wb') as f:
        f.write(res.content)
    return path

def upload_blob(bucket, source_file_name, destination_blob_name):
    """Uploads a file to the bucket.

    Arguments:
        bucket: cloud storage bucket
        source_file_name: local path to file to upload
        destination_blob_name: filename in remote bucket

    Returns: url to remote file
    """
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    blob.make_public()
    return blob.public_url

