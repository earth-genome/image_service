"""Command line wrapper to pull images for a GeoJSON FeatureCollection

The main function call goes to BulkGrabber.pull_for_geojson() in the
auto_grabber module.

Usage:
> python pull_for_geojson.py geojsonfile.json [-b bucket_name]
    [-s image_specs.json] [-N N_images] [-h]

"""

import argparse

import auto_grabber

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pull images for geojsons.'
    )
    parser.add_argument(
        'features_filename',
        type=str,
        help='File containing GeoJSON FeatureCollection {}'.format(
            'filename.json')
    )
    parser.add_argument(
        '-s', '--image_specs_filename',
        type=str,
        help=('Json-formatted file containing image specs. ' +
              'Format and defaults are specified in auto_grabber.py.')
    )
    parser.add_argument(
        '-N', '--N_images',
        type=int,
        default=auto_grabber.DEFAULT_IMAGE_SPECS['N_images'],
        help=('Number of images to pull, default: {}'.format(
            auto_grabber.DEFAULT_IMAGE_SPECS['N_images']))
    )
    parser.add_argument(
        '-b', '--bucket_name',
        type=str,
        help=('Cloud storage bucket name. If not given, images are stored ' +
              'locally, as specified in auto_grabber.py.')
    )
    kwargs = vars(parser.parse_args())
    features_filename = kwargs.pop('features_filename')
    grabber = auto_grabber.BulkGrabber(**kwargs)
    grabber.pull_for_geojson(features_filename)

            

