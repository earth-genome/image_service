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

import grabber_handlers

with open('default_story_specs.json', 'r') as f:
    DEFAULT_SPECS = json.load(f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pull images for stories from the wire.'
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
        default='default_story_specs.json',
        help=('Json-formatted file containing image specs, defautl: {}'.format(
             'default_story_specs.json'))
    )
    parser.add_argument(
        '-N', '--N_images',
        type=int,
        default=DEFAULT_SPECS['N_images'],
        help=('Number of images to pull per scene, default: {}'.format(
            DEFAULT_SPECS['N_images']))
    )
    kwargs = vars(parser.parse_args())
    provider = kwargs.pop('provider')
    if provider:
        kwargs['providers'] = [provider]
    else:
        kwargs['providers'] = list(grabber_handlers.PROVIDER_CLASSES.keys())
    grabber = grabber_handlers.StoryHandler(**kwargs)
    puller = grabber_handlers.loop(grabber.pull_for_wire)
    puller()



