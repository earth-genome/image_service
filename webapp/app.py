
import os
import sys
import json
import datetime

import numpy as np
import matplotlib.pyplot as plt

from flask import Flask, request

sys.path.append('grab_imagery/story-seeds/')
from grab_imagery import auto_grabber
from grab_imagery.digital_globe import dg_grabber

app = Flask(__name__)

@app.route('/search')
def search():
    usage_msg = ('Usage: http://earthrise-assets.herokuapp.com/search?' +
                 'provider=digital_globe' +  
                 '&lat=37.7749&lon=-122.4194' +
                 '&start=2018-01-01&end=2018-05-02' +
                 '&clouds=10&N=5\nLat, lon are required.')
        
    args = request.args
    lat = args.get('lat', type=float)
    lon = args.get('lon', type=float)
    if not lat or not lon:
        raise ValueError('Lat, lon are required.\n{}'.format(usage_msg))

    specs = {
        'startDate': args.get('start', type=str),
        'endDate': args.get('end', type=str),
        'clouds': args.get('clouds', default=10, type=int),
    }

    N_records = args.get('N', default=10, type=int)
    provider = args.get('provider', type=str)

    if provider == 'digital_globe':
        grabber = dg_grabber.DGImageGrabber(**specs)
        records = grabber.search_latlon_clean(lat, lon, N_records=N_records)
    elif provider == 'planet':
        return 'Just kidding! Planet imagery coming soon.'
    else:
        raise ValueError('Supported providers are {}.\n{}'.format(
            list(auto_grabber.PROVIDERS.keys()), usage_msg))
    return json.dumps(records)
    
@app.route('/pull')
def pull():
    usage_msg = ('Usage: http://earthrise-assets.herokuapp.com/pull?' +
                 'provider=digital_globe' + 
                 '&lat=37.7749&lon=-122.4194&scale=1.0' +
                 '&start=2018-01-01&end=2018-05-02' +
                 '&clouds=10&N=1\n' +
                 'Lat, lon, and scale are required; give scale in km.')
        
    args = request.args
    lat = args.get('lat', type=float)
    lon = args.get('lon', type=float)
    scale = args.get('scale', type=float)
    if not lat or not lon or not scale:
        raise ValueError('Lat, lon, scale are required.\n{}'.format(
            usage_msg))
    
    specs = {
        'startDate': args.get('start', type=str),
        'endDate': args.get('end', type=str),
        'clouds': args.get('clouds', default=10, type=int),
        'N_images': args.get('N', default=1, type=int)
    }
        
    provider = args.get('provider', type=str)
    if provider:
        specs.update({'provider_names': [provider]})
        # Temporary, until Planet is supported:
        if provider != 'digital_globe':
            raise ValueError('Currently only supporting digital_globe.\n' +
                             '{}'.format(usage_msg))
        
    bbox = auto_grabber.geobox.square_bbox_from_scale(lat, lon, scale)
    grabber = auto_grabber.AutoGrabber('bespoke-images', **specs)
    records = grabber.pull(bbox)
    return json.dumps(records)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
