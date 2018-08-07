"""A Flask web app to search and pull satellite imagery."""

import os
import sys
import json
import datetime

from flask import Flask, request
import numpy as np
from rq import Queue

from grab_imagery import firebaseio
from grab_imagery.geobox import geobox
from grab_imagery.grabber_handlers import PROVIDER_CLASSES
import puller_wrappers
from worker import connection

# newswire
DB_CATEGORY = '/WTL'

# for help messaging
EXAMPLE_ARGS = ('provider=digital_globe' + 
                '&lat=37.7749&lon=-122.4194' +
                '&start=2018-01-01&end=2018-05-02&clouds=10&N=3')

app = Flask(__name__)
q = Queue(connection=connection)

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
def search():
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
def pull():
    """Pull images given lat, lon, and scale."""

    notes = ('(Provider, lat, lon, scale are required; give scale in km.)')
    msg = _help_msg(
        request.base_url,
        EXAMPLE_ARGS + '&scale=1.0&min_intersect=.9', notes)

    try:
        provider, lat, lon, scale, specs = _parse_geoloc(request.args)
    except ValueError as e:
        return '{}<br>{}<br>'.format(repr(e), msg)
    if not scale:
        return 'Scale (in km) is required.<br>{}'.format(msg)

    specs.update({'providers': [provider]})
    bbox = geobox.bbox_from_scale(lat, lon, scale)
    
    job = q.enqueue_call(
        func=puller_wrappers.pull,
        args=(bbox,),
        kwargs=specs,
        timeout=3600)
    
    return _pulling_msg(bbox.bounds, **specs)

@app.route('/pull-by-id')
def pull_by_id():
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

    specs = {'providers':[provider], 'N_images':1}
    bbox = geobox.bbox_from_scale(lat, lon, scale)

    job = q.enqueue_call(
        func=puller_wrappers.pull_by_id,
        args=(bbox, catalogID, item_type),
        kwargs=specs,
        timeout=3600)

    specs.update({
        'catalogID': catalogID,
        'item_type': item_type
    })
    return _pulling_msg(bbox.bounds, **specs)
    
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
def pull_for_story():
    """Pull images for a story in the WTL database."""
    
    msg = _help_msg(request.base_url,
                    'idx=Index of the story in the database&N=3', '')

    try:
        story, N_images  = _parse_index(request.args)
    except ValueError as e:
        return '{}<br>{}<br>'.format(repr(e), msg)

    job = q.enqueue_call(
        func=puller_wrappers.pull_for_story,
        args=(story,),
        kwargs={'N_images': N_images},
        timeout=3600)

    return _pulling_msg(story.idx, N_images=N_images)

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

def _pulling_msg(target, **specs):
    msg = '<br>Pulling for: {}<br><br>'.format(target)
    msg += 'Specs: {}<br><br>'.format(specs)
    msg += ('On completion images will be uploaded to Google cloud ' +
        'storage, with links posted stdout. This ' +
        'could take up to one hour. Try:<br><br>$ heroku logs --tail')
    return msg

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
