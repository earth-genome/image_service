
import os
import sys
import json
import datetime

import numpy as np
import matplotlib.pyplot as plt

from quart import Quart, request

sys.path.append('grab_imagery/story-seeds/')
from grab_imagery import auto_grabber
from grab_imagery.auto_grabber import PROVIDER_CLASSES
from grab_imagery import firebaseio

# newswire
DB_CATEGORY = '/WTL'
WIRE_BUCKET = 'newswire-images'

# bespoke imagery
BUCKET = 'bespoke-images'

# for help messaging
EXAMPLE_ARGS = ('provider=digital_globe' + 
                '&lat=37.7749&lon=-122.4194' +
                '&start=2018-01-01&end=2018-05-02&clouds=10&N=3')


app = Quart(__name__)

@app.route('/')
def help():
    msg = ('This web app provides functionality from the following ' + 
           'endpoints, each of which takes additional required and '
           'optional arguments:<br><br>')

    msg += ('Search for available images based on lat, lon:'
            '&emsp;{}<br>'.format(''.join((request.url, 'search?'))))
    msg += ('Retrieve record for a known catalog ID:'
            '&emsp;{}<br>'.format(''.join((request.url, 'search-id?'))))
    msg += ('Pull images based on lat, lon, and scale:'
            '&emsp;{}<br>'.format(''.join((request.url, 'pull?'))))
    msg += ('Pull image for a known catalogID:'
            '&emsp;{}<br>'.format(''.join((request.url, 'pull-by-id?'))))
    msg += ('Retrieve a story record from the WTL database:'
            '&emsp;{}<br>'.format(''.join((request.url, 'retrieve-story?'))))
    msg += ('Pull images for a story in the WTL database:'
            '&emsp;{}<br>'.format(''.join((request.url, 'pull-for-story?'))))
        
    msg += '<br>Hit one of the above urls for specific argument formatting.'

    return msg

@app.route('/search')
async def search():
    """Search image availability for give lat, lon."""

    notes = ('(Provider, lat, lon are required.)')
    msg = _help_msg(request.base_url, EXAMPLE_ARGS, notes)

    try:
        provider, lat, lon, _, specs = _parse_geoloc(request.args)
    except ValueError as e:
        return '{}<br>{}<br>'.format(repr(e), msg)

    grabber = PROVIDER_CLASSES[provider](**specs)
    records = grabber.search_latlon_clean(
        lat, lon, N_records=specs['N_images'])
    return json.dumps(records)

@app.route('/search-id')
def search_id():
    """Retrieve catalog record for input catalogID."""
    notes = ('(Provider and id are required; ' +
             'for Planet the associated item_type also is required.)')
    msg = _help_msg(
        request.base_url,
        ('provider=planet&id=1425880_1056820_2018-05-14_0f18' +
        '&item_type=PSScene3Band'),
        notes)

    try: 
        provider, catalogID, item_type = _parse_catalog_keys(request.args)
    except ValueError as e:
        return '{}<br>{}<br>'.format(repr(e), msg)

    grabber = PROVIDER_CLASSES[provider]()
    record = grabber.search_id(catalogID, item_type)
    return json.dumps(record)

    
@app.route('/pull')
async def pull():
    """Pull images given lat, lon, and scale."""

    notes = ('(Provider, lat, lon, scale are required; give scale in km.)')
    msg = _help_msg(request.base_url, EXAMPLE_ARGS + '&scale=1.0', notes)

    try:
        provider, lat, lon, scale, specs = _parse_geoloc(request.args)
    except ValueError as e:
        return '{}<br>{}<br>'.format(repr(e), msg)
    if not scale:
        return 'Scale (in km) is required.<br>{}'.format(msg)

    specs.update({'providers': [provider]})
    bbox = auto_grabber.geobox.bbox_from_scale(lat, lon, scale)
    grabber = auto_grabber.AutoGrabber(BUCKET, **specs)
    records = await grabber.pull(bbox)
    return json.dumps(records)

@app.route('/pull-by-id')
async def pull_by_id():
    """Pull an image for a known catalogID."""
    notes = ('(All arguments but the last are required; ' + 
             'if the provider is Planet then item_type is also required.)')
    msg = _help_msg(
        request.base_url,
        (EXAMPLE_ARGS.split('&start')[0] +
         '&scale=1.0&id=103001007D4FDC00&item_type=visual'),
        notes)

    try:
        _, lat, lon, scale, _ = _parse_geoloc(request.args)
        provider, catalogID, item_type = _parse_catalog_keys(request.args)
    except ValueError as e:
        return '{}<br>{}<br>'.format(repr(e), msg)
    if not scale:
        return 'Scale (in km) is required.<br>{}'.format(msg)
    
    bbox = auto_grabber.geobox.bbox_from_scale(lat, lon, scale)
    grabber = auto_grabber.AutoGrabber(
        BUCKET, providers=[provider], N_images=1)
    record = await grabber.pull_by_id(provider, bbox, catalogID, item_type)
    return json.dumps(record)
    
@app.route('/retrieve-story')
def retrieve_story():
    """Retrieve a story record from the WTL database."""

    msg = _help_msg(request.base_url,
                    'idx=Index of the story in the database', '')
    try:
        story, _  = _parse_index(request.args)
    except ValueError as e:
        return '{}<br>{}<br>'.format(repr(e), msg)
        
    return json.dumps({story.idx: story.record})

@app.route('/pull-for-story')
async def pull_for_story():
    """Pull images for a story in the WTL database."""
    
    msg = _help_msg(request.base_url,
                    'idx=Index of the story in the database&N=3', '')

    try:
        story, N_images  = _parse_index(request.args)
    except ValueError as e:
        return '{}<br>{}<br>'.format(repr(e), msg)

    grabber = auto_grabber.AutoGrabber(WIRE_BUCKET, N_images=N_images)
    records = await grabber.pull_for_story(story)

    return json.dumps(records)

# WIP: Bug in Quart 0.5.0 breaks type handling in request.args.get()
# As of 5/22/18, bug is fixed, but 0.5.0 PyPI release is from 4/14.
# Clean parsing routines after bug fix is propagated in new release.

def _parse_geoloc(args):
    """Parse url arguments for requests based on lat, lon."""

    provider = _parse_provider(args)
    
    lat = args.get('lat')#, type=float)
    lon = args.get('lon')#, type=float)
    if not lat or not lon:
        raise ValueError('Lat, lon are required.')
    else: # to clean
        lat, lon = float(lat), float(lon)  
    scale = args.get('scale')#, type=float)
    if scale:   # to clean
        scale = float(scale)

    specs = {
        'startDate': args.get('start'),
        'endDate': args.get('end'),
        'clouds': int(args.get('clouds', default=10)),# type=int),
        'N_images': int(args.get('N', default=3))#, type=int)
    }
        
    return provider, lat, lon, scale, specs

def _parse_index(args):
    """Parse url arguments for requests based on story index

    Returns: The DBItem story and N_images.
    """
    STORY_SEEDS = firebaseio.DB(firebaseio.FIREBASE_URL)

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
        raise ValueError('A provider is required; supported providers ' +
                         'are {}'.format(PROVIDER_CLASSES.keys()))
    return provider
    
def _help_msg(url_base, url_args, notes):
    return '<br>Usage: {}?{}<br>{}'.format(url_base, url_args, notes)

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
