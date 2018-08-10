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
import numpy as np
from rq import Queue

from grab_imagery import firebaseio
from grab_imagery.geobox import geobox
from grab_imagery.grabber_handlers import PROVIDER_CLASSES
import puller_wrappers
import worker

q = Queue(connection=worker.connection)
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

    notes = ('Provider, lat, lon are required.')
    msg = _help_msg(request.base_url, EXAMPLE_ARGS, notes)

    try:
        provider, lat, lon, _, specs = _parse_geoloc(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)

    grabber = PROVIDER_CLASSES[provider](**specs)
    records = grabber.search_latlon_clean(
        lat, lon, N_records=specs['N_images'])
    return json.dumps(records)

@app.route('/search-id')
def search_id():
    """Retrieve catalog record for input catalogID."""
    notes = ('Provider and id are required; ' +
             'for Planet the associated item_type also is required.')
    msg = _help_msg(
        request.base_url,
        ('provider=planet&id=1425880_1056820_2018-05-14_0f18' +
        '&item_type=PSScene3Band'),
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
        provider, lat, lon, scale, specs = _parse_geoloc(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)
    if not scale:
        return json.dumps(msg)

    specs.update({'providers': [provider]})
    bbox = geobox.bbox_from_scale(lat, lon, scale)
    db_key = datetime.now().strftime('%Y%m%d%H%M%S%f')
    puller_wrappers.connection.set(db_key, 'In progress.')
    
    job = q.enqueue_call(
        func=puller_wrappers.pull,
        args=(db_key, bbox),
        kwargs=specs,
        timeout=3600)

    guide = _pulling_guide(request.url_root, db_key, bbox.bounds, **specs)
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
        _, lat, lon, scale, _ = _parse_geoloc(request.args)
        provider, catalogID, item_type = _parse_catalog_keys(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)
    if not scale:
        return json.dumps(msg)

    specs = {'providers':[provider], 'N_images':1}
    bbox = geobox.bbox_from_scale(lat, lon, scale)
    db_key = datetime.now().strftime('%Y%m%d%H%M%S%f')
    puller_wrappers.connection.set(db_key, 'In progress.')
    
    job = q.enqueue_call(
        func=puller_wrappers.pull_by_id,
        args=(db_key, bbox, catalogID, item_type),
        kwargs=specs,
        timeout=3600)

    specs.update({
        'catalogID': catalogID,
        'item_type': item_type
    })
    guide = _pulling_guide(request.url_root, db_key, bbox.bounds, **specs)
    return json.dumps(guide)
    
@app.route('/retrieve-story')
def retrieve_story():
    """Retrieve a story record from the WTL database."""

    msg = _help_msg(request.base_url,
                    'idx=Index of the story in the database', '')
    try:
        story, _  = _parse_index(request.args)
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
        story, N_images  = _parse_index(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return json.dumps(msg)

    db_key = datetime.now().strftime('%Y%m%d%H%M%S%f')
    puller_wrappers.connection.set(db_key, 'In progress.')

    job = q.enqueue_call(
        func=puller_wrappers.pull_for_story,
        args=(db_key, story),
        kwargs={'N_images': N_images},
        timeout=7200)

    guide = _pulling_guide(request.url_root, db_key, story.idx,
                           N_images=N_images)
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
    #return worker.connection.get(key).decode('utf-8')
    
def _parse_geoloc(args):
    """Parse url arguments for requests based on lat, lon."""

    provider = _parse_provider(args)
    
    lat = args.get('lat', type=float)
    lon = args.get('lon', type=float)
    if not lat or not lon:
        raise ValueError('Lat, lon are required.') 
    scale = args.get('scale', type=float)

    specs = {
        'startDate': args.get('start'),
        'endDate': args.get('end'),
        'clouds': args.get('clouds', default=10, type=int),
        'N_images': args.get('N', default=3, type=int),
        'min_intersect': args.get('min_intersect', default=.9, type=float)
    }
        
    return provider, lat, lon, scale, specs

def _parse_index(args):
    """Parse url arguments for requests based on story index

    Returns: The DBItem story and N_images.
    """
    STORY_SEEDS = firebaseio.DB(firebaseio.FIREBASE_URL)
    DB_CATEGORY = '/WTL'

    idx = args.get('idx')
    N_images = int(args.get('N', default=3))
    if not idx:
        raise ValueError('A story index is required.')
    
    record = STORY_SEEDS.get(DB_CATEGORY, idx)
    if not record:
        raise ValueError('Story not found.')
    
    story = firebaseio.DBItem(DB_CATEGORY, idx, record)
    return story, N_images

def _parse_catalog_keys(args):
    """Parse catalogID and item_type"""
    provider = _parse_provider(args)
    catalogID = args.get('id')
    item_type = args.get('item_type')
    if not catalogID:
        raise ValueError('Catalog id is required.')
    if provider == 'planet' and not item_type:
        raise ValueError('For Planet an item_type is required.')
    return provider, catalogID, item_type

def _parse_provider(args):
    """Parse provider from url arguments."""
    provider = args.get('provider')
    if not provider or provider not in PROVIDER_CLASSES.keys():
        raise ValueError('A provider is required. Supported providers ' +
                         'are {}'.format(list(PROVIDER_CLASSES.keys())))
    return provider
        
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
        'Links to uploaded images': '{}links?key={}'.format(url_root, db_key)
    }
    return guide

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
