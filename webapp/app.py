"""A Flask web app to search and pull satellite imagery.

Image search and story record retrieval are handled by the web app directly;
image pulling is pushed to a Redis queue and handled by the worker process
in worker.py.
"""

from datetime import datetime
import json
import os
import sys

from flask import Flask, request
from flask_restful import inputs
import numpy as np
from rq import Queue

from grab_imagery import firebaseio
from grab_imagery.geobox import geobox
from grab_imagery.grabber_handlers import PROVIDER_CLASSES
from grab_imagery.postprocessing import color
from grab_imagery.postprocessing import landcover
import puller_wrappers
import worker

q = Queue('default', connection=worker.connection, default_timeout=3600)
tnq = Queue('thumbnails', connection=worker.connection, default_timeout=900)
app = Flask(__name__)

# For Planet imagery:
KNOWN_ASSET_TYPES = ['analytic', 'ortho_visual', 'visual']
KNOWN_ITEM_TYPES = ['PSScene3Band', 'PSScene4Band', 'PSOrthoTile',
                    'REOrthoTile', 'SkySatScene']

# For Digital Globe:
KNOWN_IMAGE_SOURCES = ['WORLDVIEW02', 'WORLDVIEW03_VNIR', 'GEOEYE01',
                      'QUICKBIRD02', 'IKONOS']

# for help messaging
EXAMPLE_ARGS = ('provider=digital_globe' +
                '&lat=36.2553&lon=-112.6980' +
                '&start=2017-01-01&end=2018-01-01&clouds=10&N=1')

ARGUMENTS = {
    'provider': 'One of {}'.format(set(PROVIDER_CLASSES.keys())),
    'lat, lon': 'Decimal lat, lon',
    'scale': 'Side length of image in kilometers (float)',
    'start, end': 'Dates in format YYYY-MM-DD',
    'N': 'Integer number of images',
    'skip': 'Minimum number of days between images if N > 1',
    'clouds': 'Integer percentage cloud cover in range [0, 100]',
    'min_intersect': 'Float in range [0, 1.0]',
    'write_styles': 'One or more of {}'.format(
        set(color.STYLES.keys()).difference(landcover.INDICES.keys())),
    'indices': 'One or more of {}'.format(
        set(landcover.INDICES.keys())),
    'pansharp_scale': 'For DG: max scale for pansharpened images (in km)',
    'image_source': 'For DG: one or more of {}'.format(
        set(KNOWN_IMAGE_SOURCES)),
    'item_types': 'For Planet: one or more of {}'.format(
        set(KNOWN_ITEM_TYPES)),
    'asset_types': 'For Planet: one or more of {}'.format(
        set(KNOWN_ASSET_TYPES)),
    'bucket_name': 'One of our Google cloud-storage buckets',
    'thumbnails': 'True/False'
}
            


@app.route('/')
def help():
    welcome = ('This web app provides functionality from the following ' + 
        'endpoints, each of which takes required and optional arguments. ' +
        'Hit one of these urls to see specific argument formatting.')
    msg = {
        'Welcome': welcome,
        'Search for available images based on lat, lon':
            ''.join((request.url, 'search?')),
        'Retrieve record for a known catalog ID':
            ''.join((request.url, 'search-by-id?')),
        'Pull images based on lat, lon, and scale':
            ''.join((request.url, 'pull?')),
        'Pull image for a known catalogID':
            ''.join((request.url, 'pull-by-id?')),
        'Retrieve a story record from the WTL database':
            ''.join((request.url, 'retrieve-story?')),
        'Pull images for a story in the WTL database':
            ''.join((request.url, 'pull-for-story?')),
        'Retrieve links to images uploaded to Google Cloud storage':
            ''.join((request.url, 'links?'))
    }
    return json.dumps(msg)

@app.route('/search')
def search():
    """Search image availability for give lat, lon."""

    notes = ('Provider, lat, lon, N (number of records) are required.')
    msg = _help_msg(request.base_url, EXAMPLE_ARGS, notes)

    try:
        provider = _parse_provider(request.args)
        lat, lon, _ = _parse_geoloc(request.args)
        specs = _parse_specs(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)
    try:
        N_records = specs['N_images']
    except KeyError:
        return json.dumps(msg)

    grabber = PROVIDER_CLASSES[provider](**specs)
    records = grabber.search_latlon_clean(lat, lon, N_records=N_records)
    return json.dumps(records)

@app.route('/search-by-id')
def search_by_id():
    """Retrieve catalog record for input catalogID."""
    notes = ('Provider and id are required; ' +
             'for Planet the associated item_type also is required.')
    msg = _help_msg(
        request.base_url,
        ('provider=planet&id=1425880_1056820_2018-05-14_0f18' +
        '&item_type=PSOrthoTile'),
        notes)

    try: 
        provider, catalogID, item_type = _parse_catalog_keys(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)

    grabber = PROVIDER_CLASSES[provider]()
    record = grabber.search_id(catalogID, item_type)
    return json.dumps(record)
    
@app.route('/pull')
def pull():
    """Pull images given lat, lon, and scale."""

    notes = {
        'Required arguments': 'Provider, lat, lon, scale; give scale in km.'
    }
    notes.update({'Possible arguments': ARGUMENTS})
    
    msg = _help_msg(
        request.base_url,
        EXAMPLE_ARGS + '&scale=3.0&min_intersect=.9', notes)

    try:
        provider = _parse_provider(request.args)
        lat, lon, scale = _parse_geoloc(request.args)
        specs = _parse_specs(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)
    if not scale:
        return json.dumps(msg)

    bbox = geobox.bbox_from_scale(lat, lon, scale)
    kwargs = dict({'providers': [provider]}, **specs)
    db_key = datetime.now().strftime('%Y%m%d%H%M%S%f')
    puller_wrappers.connection.set(db_key, json.dumps('In progress.'))

    if 'thumbnails' in specs.keys() and specs['thumbnails']:
        job = tnq.enqueue_call(
            func=puller_wrappers.pull,
            args=(db_key, bbox),
            kwargs=kwargs)
        try:
            print('As dict: {}'.format(job['args']))
        except:
            pass
        try:
            print('As attribute: {}'.format(job.args))
        except:
            pass
    else:
        job = q.enqueue_call(
            func=puller_wrappers.pull,
            args=(db_key, bbox),
            kwargs=kwargs)

    guide = _pulling_guide(request.url_root, db_key, bbox.bounds, **kwargs)
    return json.dumps(guide)

@app.route('/pull-by-id')
def pull_by_id():
    """Pull an image for a known catalogID."""
    notes = ('All of the above arguments are required, except item_type ' +
             'when the provider is digital_globe.')
    msg = _help_msg(
        request.base_url,
        ('provider=planet&id=1425880_1056820_2018-05-14_0f18' +
        '&lat=-121.529&lon=38.455&scale=4.0&item_type=PSOrthoTile'),
        notes)

    try:
        provider, catalogID, item_type = _parse_catalog_keys(request.args)
        lat, lon, scale = _parse_geoloc(request.args)
        specs = _parse_specs(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)
    if not scale:
        return json.dumps(msg)

    bbox = geobox.bbox_from_scale(lat, lon, scale)
    kwargs = dict({'providers': [provider]}, **specs)
    db_key = datetime.now().strftime('%Y%m%d%H%M%S%f')
    puller_wrappers.connection.set(db_key, json.dumps('In progress.'))
    
    job = q.enqueue_call(
        func=puller_wrappers.pull_by_id,
        args=(db_key, bbox, catalogID, item_type),
        kwargs=kwargs)

    kwargs.update({
        'catalogID': catalogID,
        'item_type': item_type
    })
    guide = _pulling_guide(request.url_root, db_key, bbox.bounds, **kwargs)
    return json.dumps(guide)
    
@app.route('/retrieve-story')
def retrieve_story():
    """Retrieve a story record from the WTL database."""

    msg = _help_msg(request.base_url,
                    'idx=Index of the story in the database', '')
    try:
        story  = _parse_index(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)
        
    return json.dumps({story.idx: story.record})

@app.route('/pull-for-story')
def pull_for_story():
    """Pull images for a story in the WTL database."""
    
    msg = _help_msg(request.base_url,
                    'idx=Index of the story in the database&N=3', '')

    try:
        story = _parse_index(request.args)
        specs = _parse_specs(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)

    db_key = datetime.now().strftime('%Y%m%d%H%M%S%f')
    puller_wrappers.connection.set(db_key, json.dumps('In progress.'))

    job = q.enqueue_call(
        func=puller_wrappers.pull_for_story,
        args=(db_key, story),
        kwargs=specs,
        timeout=7200)

    guide = _pulling_guide(request.url_root, db_key, story.idx, **specs)
    return json.dumps(guide)

@app.route('/links')
def get_links():
    """Retrieve links posted to Redis at specified key."""
    msg = _help_msg(
            request.base_url,
            'key=20180809150109299437',
            'Key is hashed by time of request, format %Y%m%d%H%M%S%f.')
    
    key = request.args.get('key')
    if not key:
        return json.dumps(msg)

    return puller_wrappers.connection.get(key)

# Argument parsing functions

def _parse_provider(args):
    """Parse url arguments for provider."""
    provider = args.get('provider')
    if not provider or provider not in PROVIDER_CLASSES.keys():
        raise ValueError('A provider is required. Supported providers ' +
                         'are {}'.format(list(PROVIDER_CLASSES.keys())))
    return provider

def _parse_geoloc(args):
    """Parse url arguments for lat, lon, scale."""
    lat = args.get('lat', type=float)
    lon = args.get('lon', type=float)
    if not lat or not lon:
        raise ValueError('Lat, lon are required.') 
    scale = args.get('scale', type=float)

    return lat, lon, scale

def _parse_specs(args):
    """Parse url arguments for image pulling specs."""  
    specs = {
        'startDate': args.get('start'),
        'endDate': args.get('end'),
        'N_images': args.get('N', type=int),
        'skip_days': args.get('skip', type=int),
        'clouds': args.get('clouds', type=int),
        'min_intersect': args.get('min_intersect', type=float),
        'write_styles': args.getlist('write_styles'),
        'landcover_indices': args.getlist('indices'),
        'pansharp_scale': args.get('pansharp_scale', type=float),
        'image_source': args.getlist('image_sources'),
        'item_types': args.getlist('item_types'),
        'asset_types': args.getlist('asset_types'),
        'bucket_name': args.get('bucket_name'),
        'thumbnails': args.get('thumbnails', type=inputs.boolean)
    }
    if not set(specs['asset_types']) <= set(KNOWN_ASSET_TYPES):
        raise ValueError('Supported asset_types are {} '.format(
            KNOWN_ASSET_TYPES) + '(applicable to Planet only)')
    if not set(specs['item_types']) <= set(KNOWN_ITEM_TYPES):
        raise ValueError('Supported item_types are {} '.format(
            KNOWN_ITEM_TYPES) + '(applicable to Planet only)')
    if not set(specs['image_source']) <= set(KNOWN_IMAGE_SOURCES):
        raise ValueError('Supported image_sources are {} '.format(
            KNOWN_IMAGE_SOURCES) + '(applicable to DG only)')
    if not set(specs['write_styles']) <= set(color.STYLES.keys()):
        raise ValueError('Supported write_styles are {}'.format(
            list(color.STYLES.keys())))
    if not set(specs['landcover_indices']) <= set(landcover.INDICES.keys()):
        raise ValueError('Supported indices are {}'.format(
            list(landcover.INDICES.keys())))
    
    # override defaults to ensure availability of NIR band in this case:
    if specs['landcover_indices'] and not specs['item_types']:
        specs['item_types'] = ['PSScene4Band', 'PSOrthoTile', 'REOrthoTile']
    
    specs = {k:v for k,v in specs.items() if v is not None and v != []}
    return specs



def _parse_index(args):
    """Parse url arguments for story index.

    Returns: The DBItem story.
    """
    STORY_SEEDS = firebaseio.DB(firebaseio.FIREBASE_URL)
    DB_CATEGORY = '/WTL'

    idx = args.get('idx')
    if not idx:
        raise ValueError('A story index is required.')
    
    record = STORY_SEEDS.get(DB_CATEGORY, idx)
    if not record:
        raise ValueError('Story not found.')
    
    story = firebaseio.DBItem(DB_CATEGORY, idx, record)
    return story

def _parse_catalog_keys(args):
    """Parse provider, catalogID, and item_type"""
    provider = _parse_provider(args)
    catalogID = args.get('id')
    item_type = args.get('item_type')
    if not catalogID:
        raise ValueError('Catalog id is required.')
    if provider == 'planet' and not item_type:
        raise ValueError('For Planet an item_type is required.')
    if item_type and item_type not in KNOWN_ITEM_TYPES:
        raise ValueError('Supported item_types are {} '.format(
            KNOWN_ITEM_TYPES) + '(applicable to Planet only)')
    return provider, catalogID, item_type

# Help messaging

def _help_msg(base_url, url_args, notes):
    msg = {
        'Usage': '{}?{}'.format(base_url, url_args),
        'Notes': notes
    }
    return msg

def _pulling_guide(url_root, db_key, target, **specs):
    hope = ('On completion images will be uploaded to Google cloud ' +
        'storage, with links printed to stdout. Depending on the size of ' +
        'the scene requested, this could take from a few minutes to one hour.')
    guide = {
        'Pulling for': target,
        'Specs': specs,
        'Hope': hope,
        'Follow': '$ heroku logs --tail -a earthrise-imagery',
        'Links': '{}links?key={}'.format(url_root, db_key)
    }
    return guide

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
