import os
import sys
import math
import json
import datetime
import requests
import numpy as np
import matplotlib.pyplot as plt

from flask import Flask, request
from geobox import geobox
from shapely import wkt

import gbdxtools


# import dg_grabber



app = Flask(__name__)


@app.route('/dg/assets')
def newswire():


    lon = float(request.args.get('lon'))
    lat = float(request.args.get('lat'))
    start = str(request.args.get('start'))
    end = str(request.args.get('end'))

	bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
	g = dg_grabber.DGImageGrabber(startDate=start, endDate=end)
	_, _, records = g(bbox, N_images=10, write_styles=None)

	return json.dumps(records[0])


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')