"""Command line wrapper to pull images for the new wire.

The main function call goes to BulkGrabber.pull_for_wire() in the
auto_grabber module.

Usage:
> python pull_for_wire.py 
    [-s image_specs.json] [-N N_images] [-h]

More options can be accessed through the BulkGrabber class.
"""

import argparse

import auto_grabber

WIRE_BUCKET = 'newswire-images'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pull images for stories from the wire.'
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
        help=('Number of images to pull per scene, default: {}'.format(
            auto_grabber.DEFAULT_IMAGE_SPECS['N_images']))
    )
    kwargs = vars(parser.parse_args())
    grabber = auto_grabber.BulkGrabber(bucket_name=WIRE_BUCKET, **kwargs)
    grabber.pull_for_wire()



