"""Command line wrapper to pull images for a GeoJSON FeatureCollection

The main function call goes to GeoJSONGrabber.pull_for_geojson() in the
grabber module.

Usage:
> python pull_for_geojson.py geojsonfile.json cloud-storage-bucket-name
    [-s image_specs.json] [-N N_images] [-h]

"""

import argparse
import json
import sys

sys.path.append('../')
from grab_imagery import grabber_handlers

with open('default_specs.json', 'r') as f:
    DEFAULT_SPECS = json.load(f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pull images for geojsons.'
    )
    parser.add_argument(
        'features_filename',
        type=str,
        help='Name of .json file containing GeoJSON FeatureCollection.'
    )
    parser.add_argument(
        'bucket_name',
        type=str,
        help='Name of existing cloud storage bucket.'
    )
    parser.add_argument(
        '-s', '--specs_filename',
        type=str,
        help=('Json-formatted file containing image specs. ' +
              'Format and defaults are specified in default_specs.json.')
    )
    parser.add_argument(
        '-N', '--N_images',
        type=int,
        default=DEFAULT_SPECS['N_images'],
        help=('Number of images to pull, default: {}'.format(
            DEFAULT_SPECS['N_images']))
    )
    kwargs = vars(parser.parse_args())
    features_filename = kwargs.pop('features_filename')
    bucket_name = kwargs.pop('bucket_name')
    grabber = grabber_handlers.GeoJSONHandler(bucket_name, **kwargs)
    grabber.pull_for_geojson(features_filename)

            

