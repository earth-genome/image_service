"""A Flask web app to search and pull satellite imagery.

Image search is handled by the web app directly; image pulling is pushed to a 
Redis queue and handled by the worker process in worker.py.
"""

from datetime import datetime
import json
import os
import sys

from flask import Flask, jsonify, request
from flask_restful import inputs
import numpy as np
from rq import Queue

from geobox import geobox
from grabbers.dg import KNOWN_IMAGE_SOURCES
from grabbers.planet_grabber import KNOWN_ITEM_TYPES, KNOWN_ASSET_TYPES
from postprocessing.color import STYLES
from postprocessing.landcover import INDICES
import wrappers
from wrappers import PROVIDER_CLASSES
import worker

q = Queue('default', connection=worker.connection, default_timeout=3600)
tnq = Queue('thumbnails', connection=worker.connection, default_timeout=900)
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False


# for help messaging
EXAMPLE_ARGS = ('provider=digital_globe' +
                '&lat=37.8268&lon=-122.4227' +
                '&start=2017-01-01&end=2018-01-01&clouds=10&N=1')

ARGUMENTS = {
    'provider': 'One of {}'.format(set(PROVIDER_CLASSES)),
    'lat, lon': 'Decimal lat, lon',
    'scale': 'Side length of image in kilometers (float)',
    'start, end': 'Dates in format YYYY-MM-DD',
    'N': 'Integer number of images',
    'skip': 'Minimum number of days between images if N > 1',
    'clouds': 'Integer percentage cloud cover in range [0, 100]',
    'min_intersect': 'Float in range [0, 1.0]',
    'write_styles': 'One or more of {}'.format(set(STYLES)),
    'indices': 'One or more of {}'.format(set(INDICES)),
    'pansharpen': 'For DG: True/False',
    'image_source': 'For DG: one or more of {}'.format(
        set(KNOWN_IMAGE_SOURCES)),
    'item_types': 'For Planet: one or more of {}'.format(set(KNOWN_ITEM_TYPES)),
    'asset_type': 'For Planet: one of {}'.format(set(KNOWN_ASSET_TYPES)),
    'bucket_name': 'One of our Google cloud-storage buckets',
    'thumbnails': 'True/False'
}
            


@app.route('/')
def welcome():
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
        'Retrieve links to images uploaded to Google Cloud storage':
            ''.join((request.url, 'links?'))
    }
    return jsonify(msg)

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
        return jsonify(msg), 400
    try:
        max_records = specs['N_images']
    except KeyError:
        return jsonify(msg), 400

    # Override default specs to search all available assets:
    if not specs.get('item_types'):
        specs['item_types'] = KNOWN_ITEM_TYPES.copy()
        try: 
            specs['item_types'].remove('PSScene4Band')
        except ValueError:
            pass
    if not specs.get('image_source'):
        specs['image_source'] = KNOWN_IMAGE_SOURCES.copy()

    try:
        grabber = PROVIDER_CLASSES[provider](**specs)
        records = grabber.search_latlon_clean(lat, lon, max_records=max_records)
    except Exception as e:
        msg['Exception'] = repr(e)
        return jsonify(msg), 400
    
    return jsonify(records), 200

@app.route('/search-by-id')
def search_by_id():
    """Retrieve catalog record for input catalogID."""
    notes = ('Provider and id are required. For Planet an item_type ' +
             'from {} is also required.'.format(set(KNOWN_ITEM_TYPES)))
    msg = _help_msg(
        request.base_url,
        ('provider=planet&id=1425880_1056820_2018-05-14_0f18' +
        '&item_type=PSOrthoTile'),
        notes)

    try: 
        provider, catalogID, item_type = _parse_catalog_keys(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return jsonify(msg), 400

    try:
        grabber = PROVIDER_CLASSES[provider]()
        record = grabber.search_id_clean(catalogID, item_type)
    except Exception as e:
        msg['Exception'] = repr(e)
        return jsonify(msg), 400
    
    return jsonify(record), 200
    
@app.route('/pull')
def pull():
    """Pull images given lat, lon, and scale."""

    notes = {
        'Required arguments': 'Provider, lat, lon, scale; give scale in km.'
    }
    notes.update({'Allowed arguments': ARGUMENTS})
    msg = _help_msg(
        request.base_url, EXAMPLE_ARGS + '&scale=.75&pansharpen=True', notes)

    try:
        provider = _parse_provider(request.args)
        lat, lon, scale = _parse_geoloc(request.args)
        specs = _parse_specs(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return jsonify(msg), 400
    if not scale:
        return jsonify(msg), 400

    bbox = geobox.bbox_from_scale(lat, lon, scale)
    db_key = datetime.now().strftime('%Y%m%d%H%M%S%f')
    wrappers.connection.set(db_key, json.dumps('In progress.'))

    if specs.get('thumbnails'):
        job = tnq.enqueue_call(
            func=wrappers.pull,
            args=(db_key, provider, bbox),
            kwargs=specs)
    else:
        job = q.enqueue_call(
            func=wrappers.pull,
            args=(db_key, provider, bbox),
            kwargs=specs)

    guide = _pulling_guide(
        request.url_root, db_key, provider, bbox.bounds, **specs)
    return jsonify(guide), 200

@app.route('/pull-by-id')
def pull_by_id():
    """Pull an image for a known catalogID."""
    notes = ('All of the above arguments are required. For Planet an ' +
             'item_type from {} is also required.'.format(
                 set(KNOWN_ITEM_TYPES)))
    msg = _help_msg(
        request.base_url,
        EXAMPLE_ARGS.split('&start')[0] + '&id=103001006B8F9000&scale=.75',
        notes)

    try:
        provider, catalogID, item_type = _parse_catalog_keys(request.args)
        lat, lon, scale = _parse_geoloc(request.args)
        specs = _parse_specs(request.args)
    except ValueError as e:
        msg['Exception'] = repr(e)
        return jsonify(msg), 400
    if not scale:
        return jsonify(msg), 400

    bbox = geobox.bbox_from_scale(lat, lon, scale)
    db_key = datetime.now().strftime('%Y%m%d%H%M%S%f')
    wrappers.connection.set(db_key, json.dumps('In progress.'))

    job = q.enqueue_call(
        func=wrappers.pull_by_id,
        args=(db_key, provider, bbox, catalogID, item_type),
        kwargs=specs)

    specs.update({'catalogID': catalogID, 'item_type': item_type})
    guide = _pulling_guide(
        request.url_root, db_key, provider, bbox.bounds, **specs)
    return jsonify(guide), 200

@app.route('/links')
def get_links():
    """Retrieve links posted to Redis at specified key."""
    msg = _help_msg(
            request.base_url,
            'key=20180809150109299437',
            'Key is hashed by time of request, format %Y%m%d%H%M%S%f.')
    
    key = request.args.get('key')
    if not key:
        return jsonify(msg), 400

    return wrappers.connection.get(key), 200

# Argument parsing functions

def _parse_provider(args):
    """Parse url arguments for provider."""
    provider = args.get('provider')
    if not provider or provider not in PROVIDER_CLASSES:
        raise ValueError('A provider is required. Supported providers ' +
                         'are {}'.format(set(PROVIDER_CLASSES)))
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
        'pansharpen': args.get('pansharpen', type=inputs.boolean),
        'image_source': args.getlist('image_source'),
        'item_types': args.getlist('item_types'),
        'asset_type': args.get('asset_type'),
        'bucket_name': args.get('bucket_name'),
        'thumbnails': args.get('thumbnails', type=inputs.boolean)
    }
    if specs['asset_type'] and specs['asset_type'] not in KNOWN_ASSET_TYPES:
        raise ValueError('Supported asset_types are {} '.format(
            KNOWN_ASSET_TYPES) + '(applicable to Planet only)')
    if not set(specs['item_types']) <= set(KNOWN_ITEM_TYPES):
        raise ValueError('Supported item_types are {} '.format(
            KNOWN_ITEM_TYPES) + '(applicable to Planet only)')
    if not set(specs['image_source']) <= set(KNOWN_IMAGE_SOURCES):
        raise ValueError('Supported image_sources are {} '.format(
            KNOWN_IMAGE_SOURCES) + '(applicable to DG only)')
    if not set(specs['write_styles']) <= set(STYLES):
        raise ValueError('Supported write_styles are {}'.format(set(STYLES)))
    if not set(specs['landcover_indices']) <= set(INDICES):
        raise ValueError('Supported indices are {}'.format(INDICES))
    
    specs = {k:v for k,v in specs.items() if v is not None and v != []}
    return specs

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

def _pulling_guide(url_root, db_key, provider, bounds, **specs):
    guide = {
        'Provider': provider,
        'Geographic bounds': bounds,
        'Specs': specs,
        'A message of hope': ('On completion images will be uploaded to '
            'Google cloud storage. This could take several minutes.'),
        'Follow': '$ heroku logs --tail -a earthrise-imagery',
        'Links': '{}links?key={}'.format(url_root, db_key)
    }
    return guide

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
