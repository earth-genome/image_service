"""Command line wrapper to pull images for the new wire.

The main function call goes to StoryGrabber.pull_for_wire() in the
auto_grabber module.

Usage:
> python pull_for_wire.py [-s image_specs.json] [-N N_images] [-h]

More options can be accessed through the StoryGrabber class.
"""

import argparse
import json
import sys

sys.path.append('../')
from grab_imagery import grabber_handlers

WIRE_BUCKET = 'newswire-images'

with open('default_story_specs.json', 'r') as f:
    DEFAULT_SPECS = json.load(f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pull images for stories from the wire.'
    )
    parser.add_argument(
        '-s', '--specs_filename',
        type=str,
        help=('Json-formatted file containing image specs. ' +
              'Format and defaults are specified in default_story_specs.json.')
    )
    parser.add_argument(
        '-N', '--N_images',
        type=int,
        default=DEFAULT_SPECS['N_images'],
        help=('Number of images to pull per scene, default: {}'.format(
            DEFAULT_SPECS['N_images']))
    )
    kwargs = vars(parser.parse_args())
    grabber = grabber_handlers.StoryHandler(WIRE_BUCKET, **kwargs)
    grabber.pull_for_wire()



