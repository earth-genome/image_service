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
import puller_wrappers
import worker

q = Queue('default', connection=worker.connection)
tnq = Queue('thumbnails', connection=worker.connection)
app = Flask(__name__)

# for help messaging
EXAMPLE_ARGS = ('provider=digital_globe' +
                '&lat=36.2553&lon=-112.6980' +
                '&start=2017-01-01&end=2018-01-01&clouds=10&N=1')

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
            ''.join((request.url, 'search-id?')),
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

    notes = ('Provider, lat, lon, scale are required; give scale in km.')
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
            kwargs=kwargs,
            timeout=3600)
    else:
        job = q.enqueue_call(
            func=puller_wrappers.pull,
            args=(db_key, bbox),
            kwargs=kwargs,
            timeout=3600)

    guide = _pulling_guide(request.url_root, db_key, bbox.bounds, **kwargs)
    return json.dumps(guide)

@app.route('/pull-by-id')
def pull_by_id():
    """Pull an image for a known catalogID."""
    notes = ('All arguments but the last are required; ' + 
             'if the provider is Planet then item_type is also required.')
    msg = _help_msg(
        request.base_url,
        (EXAMPLE_ARGS.split('&start')[0] +
         '&scale=3.0&id=103001006B8F9000&item_type=visual'),
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
        kwargs=kwargs,
        timeout=3600)

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
    # For Planet imagery only:
    KNOWN_ASSET_TYPES = ['analytic', 'ortho_visual', 'visual']
    
    specs = {
        'startDate': args.get('start'),
        'endDate': args.get('end'),
        'clouds': args.get('clouds', type=int),
        'N_images': args.get('N', type=int),
        'min_intersect': args.get('min_intersect', type=float),
    # The following are special-purpose and excluded from help messaging:
        'pansharp_scale': args.get('pansharp_scale', type=float),
        'asset_types': args.getlist('asset_types'),
        'write_styles': args.getlist('write_styles'),
        'bucket_name': args.get('bucket_name'),
        'thumbnails': args.get('thumbnails', type=inputs.boolean)
    }
    if not set(specs['asset_types']) <= set(KNOWN_ASSET_TYPES):
        raise ValueError('Supported asset_types are {} '.format(
            KNOWN_ASSET_TYPES) + '(applicable to Planet only)')
    if not set(specs['write_styles']) <= set(color.STYLES.keys()):
        raise ValueError('Supported write_styles are {}'.format(
            list(color.STYLES.keys())))
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
