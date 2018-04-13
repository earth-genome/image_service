"""Class to upload files to Google Cloud storage."""

from google.cloud import storage

class Bucketer(object):
    """Upload files Google Cloud storage.

    Attributes:
        bucket: Cloud storage bucket
        
    Methods:
        upload_blob: Upload a file to the bucket
    """
    
    def __init__(self, bucket_name, project='good-locations'):
        client = storage.Client(project=project)
        self.bucket = client.get_bucket(bucket_name)

    def upload_blob(self, source_file_name, destination_blob_name):
        """Uploads a file to the bucket.

        Arguments:
            source_file_name: local path to file to upload
            destination_blob_name: filename in remote bucket

        Returns: url to remote file
        """
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        blob.make_public()
        return blob.public_url
