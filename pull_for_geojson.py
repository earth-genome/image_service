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

import grabber_handlers

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
        '-b', '--bucket_name',
        type=str,
        default=grabber_handlers.DEFAULT_BUCKET,
        help='Name of existing cloud storage bucket, default: {}'.format(
            grabber_handlers.DEFAULT_BUCKET)
    )
    parser.add_argument(
        '-p', '--provider',
        type=str,
        help='From {}; if none specified, both with be used.'.format(
            list(grabber_handlers.PROVIDER_CLASSES.keys()))
    )
    parser.add_argument(
        '-s', '--specs_filename',
        type=str,
        default='default_specs.json',
        help='Json-formatted file containing image specs, defautl: {}'.format(
             'default_specs.json')
    )
    parser.add_argument(
        '-N', '--N_images',
        type=int,
        default=DEFAULT_SPECS['N_images'],
        help='Number of images to pull, default: {}'.format(
            DEFAULT_SPECS['N_images'])
    )
    kwargs = vars(parser.parse_args())
    features_filename = kwargs.pop('features_filename')
    provider = kwargs.pop('provider')
    if provider:
        kwargs['providers'] = [provider]
    else:
        kwargs['providers'] = list(grabber_handlers.PROVIDER_CLASSES.keys())
    grabber = grabber_handlers.GeoJSONHandler(**kwargs)
    puller = grabber_handlers.loop(grabber.pull_for_geojson)
    puller(features_filename)
    

            

