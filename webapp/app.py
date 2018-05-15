
import os
import sys
import json
import datetime

import numpy as np
import matplotlib.pyplot as plt

from flask import Flask, request, make_response

sys.path.append('grab_imagery/story-seeds/')
from grab_imagery import auto_grabber
from grab_imagery import firebaseio
from grab_imagery.digital_globe import dg_grabber
from grab_imagery.planet_labs import planet_grabber

PROVIDERS = ['digital_globe', 'planet']

# newswire
DB_CATEGORY = '/WTL'
WIRE_BUCKET = 'newswire-images'

# bespoke imagery
BUCKET = 'bespoke-images'

# for help messaging
EXAMPLE_ARGS = ('provider=digital_globe' + 
                '&lat=37.7749&lon=-122.4194' +
                '&start=2018-01-01&end=2018-05-02&clouds=10&N=3')


app = Flask(__name__)

@app.route('/')
def help():
    msg = ('This web app provides functionality from the following ' + 
           'endpoints, each of which takes additional required and '
           'optional arguments:<br><br>')

    msg += ('Search for available images based on lat, lon:'
            '&emsp;{}<br>'.format(''.join((request.url, 'search?'))))
    msg += ('Pull images based on lat, lon, and scale:'
            '&emsp;{}<br>'.format(''.join((request.url, 'pull?'))))
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

    if provider == 'digital_globe':
        grabber = dg_grabber.DGImageGrabber(**specs)
    elif provider == 'planet':
        grabber = planet_grabber.PlanetGrabber(**specs)
        
    records = grabber.search_latlon_clean(lat, lon,
                                          N_records=specs['N_images'])
    return json.dumps(records)
    
@app.route('/pull')
def pull():
    """Pull images given lat, lon, and scale."""

    notes = ('(Provider, lat, lon, scale are required; give scale in km.)')
    msg = _help_msg(request.base_url, EXAMPLE_ARGS + '&scale=1.0', notes)

    try:
        provider, lat, lon, scale, specs = _parse_geoloc(request.args)
    except ValueError as e:
        return '{}<br>{}<br>'.format(repr(e), msg)

    if not scale:
        return 'Scale (in km) is required.<br>{}'.format(msg)

    specs.update({'bbox_rescaling': 1})
    specs.update({'provider_names': [provider]})
    bbox = auto_grabber.geobox.square_bbox_from_scale(lat, lon, scale)
    grabber = auto_grabber.AutoGrabber(BUCKET, **specs)
    records = grabber.pull(bbox)
    return json.dumps(records)

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

    grabber = auto_grabber.AutoGrabber(WIRE_BUCKET, N_images=N_images)
    records = grabber.pull_for_story(story)

    return json.dumps(records)

def _parse_geoloc(args):
    """Parse url arguments for requests based on lat, lon."""
    
    lat = args.get('lat', type=float)
    lon = args.get('lon', type=float)
    if not lat or not lon:
        raise ValueError('Lat, lon are required.')
    scale = args.get('scale', type=float)

    provider = args.get('provider', type=str)
    if not provider or provider not in PROVIDERS:
        raise ValueError('A provider is required; supported providers ' +
                         'are {}'.format(PROVIDERS))
    
    specs = {
        'startDate': args.get('start', type=str),
        'endDate': args.get('end', type=str),
        'clouds': args.get('clouds', default=10, type=int),
        'N_images': args.get('N', default=2, type=int)
    }
        
    return provider, lat, lon, scale, specs

def _parse_index(args):
    """Parse url arguments for requests based on story index

    Returns: The DBItem story and N_images.
    """
    STORY_SEEDS = firebaseio.DB(firebaseio.FIREBASE_URL)

    idx = args.get('idx', type=str)
    N_images = args.get('N', default=3, type=int)
    if not idx:
        raise ValueError('A story index is required.')
    
    record = STORY_SEEDS.get(DB_CATEGORY, idx)
    if not record:
        raise ValueError('Story not found.')
    
    story = firebaseio.DBItem(DB_CATEGORY, idx, record)
    return story, N_images
    
    
def _help_msg(url_base, url_args, notes):
    return '<br>Usage: {}?{}<br>{}'.format(url_base, url_args, notes)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
