"""Command line wrapper to pull images for a GeoJSON FeatureCollection


Usage:
> python pull_for_geojson.py digital_globe geojsonfile.json 
    [-b bucket-name] [-s image_specs.json] [-N N_images] [-h]

[Or planet in place of digital_globe.]

"""

import argparse
import asyncio
import json
import sys

import shapely

import dg_grabber
import grabber
import planet_grabber

GRABBERS = {
    'digital_globe': dg_grabber.DGImageGrabber,
    'planet': planet_grabber.PlanetGrabber
}

DEFAULT_BUCKET = 'soccer-fields'

async def pull_for_geojson(image_grabber, filename):
    """Pull images for features in a FeatureCollection.

    Arguments:
        grabber: An instance of one of the GRABBERS above
        filename: name of file containing GeoJSON FeatureCollection

    Output: Adds image records to the FeatureCollection and writes it
            to file.
    Returns: A json dump of the FeatureCollection.
    """
    with open(filename, 'r') as f:
        geojson = json.load(f)

    tasks = [pull_for_feature(image_grabber, f) for f in geojson['features']]
    new_features = await asyncio.gather(*tasks, return_exceptions=True)
    geojson['features'] = new_features
        
    outfile = filename.split('.json')[0] + '-images.json'
    with open(outfile, 'w') as f:
        json.dump(geojson, f, indent=4)
    return outfile

async def pull_for_feature(image_grabber, feature):
    """Pull images for a geojson feature."""
    if 'properties' not in feature:
        feature.update({'properties': {}})
    if 'images' not in feature['properties']:
        feature['properties'].update({'images': []})
    polygon = shapely.geometry.asShape(feature['geometry'])
    bbox = shapely.geometry.box(*polygon.bounds)
    records = await image_grabber.pull(bbox)
    feature['properties']['images'] += records
    return feature

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pull images for geojsons.'
    )
    parser.add_argument(
        'provider',
        type=str,
        choices=list(GRABBERS),
        help='An image provider, from {}.'.format(list(GRABBERS))
    )
    parser.add_argument(
        'features_filename',
        type=str,
        help='Name of file containing a GeoJSON FeatureCollection.'
    )
    parser.add_argument(
        '-b', '--bucket',
        type=str,
        default=DEFAULT_BUCKET,
        help='Name of existing cloud storage bucket, default: {}'.format(
            DEFAULT_BUCKET)
    )
    parser.add_argument(
        '-s', '--specs_filename',
        type=str,
        help=('Json-formatted file containing image specs. Ref. '
              'default_specs.json for formatting and options.')
    )
    parser.add_argument(
        '-N', '--N_images',
        type=int,
        default=1,
        help='Number of images to pull, default 1.'
    )
    kwargs = vars(parser.parse_args())
    provider = kwargs.pop('provider')
    features_filename = kwargs.pop('features_filename')
    kwargs = {k:v for k,v in kwargs.items() if v is not None}
    image_grabber = GRABBERS[provider](**kwargs)
    looped = grabber.loop(pull_for_geojson)
    outfile = looped(image_grabber, features_filename)
    print('Links to images are written in {}'.format(outfile))

            

