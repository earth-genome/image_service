
"""Download an image from the DG catalog given lat/lon and scale.

Inputs:
    latitude, longitude,
    scale (size of image (width or height) in km)

Usage:
    python grab_by_latlon.py 38.8977 -77.0365 -s 3.5
    Outputs: Dynamical-range-adjusted RGB PNG image

Image source (incl. sensor and pansharpening) determined automatically;
parameters other than clouds and start/end dates conform to defaults
set in dg_grabber.py  For more flexibility, see/use dg_grabber.py.

For command line syntax with additional supported options:
python grab_draft.py -h

"""

import argparse
import sys

sys.path.append('../')
from digital_globe import dg_grabber
from geobox import geobox

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Grab an image from GBDX.'
    )
    parser.add_argument(
        'lat',
        type=float,
        help='Latitude: {}'.format('38.8977')
    )
    parser.add_argument(
        'lon',
        type=float,
        help='Longitude: {}'.format('-77.0365')
    ) 
    parser.add_argument(
        '-s', '--scale',
        type=float,
        default=1.0,
        help='Approx. image size (width or height) in km, ' +
            'default: {}'.format(1.0)
    )
    parser.add_argument(
        '-N', '--N_images',
        type=int,
        default=2,
        help='Number of images to pull, default: {}'.format(2)
    )
    parser.add_argument(
        '-c', '--clouds',
        type=int,
        default=dg_grabber.DEFAULT_SPECS['clouds'],
        help='Maximum percentage cloud cover, default: {}'.format(
            dg_grabber.DEFAULT_SPECS['clouds'])
    )
    parser.add_argument(
        '-sd', '--startDate',
        type=str,
        default=dg_grabber.DEFAULT_SPECS['startDate'],
        help='Isoformat start date for image search, default: {}'.format(
            dg_grabber.DEFAULT_SPECS['startDate'])
    )
    parser.add_argument(
        '-ed', '--endDate',
        type=str,
        default=dg_grabber.DEFAULT_SPECS['endDate'],
        help='Isoformat end date for image search, default: {}'.format(
            dg_grabber.DEFAULT_SPECS['endDate'])
    )
    parser.add_argument(
        '-f', '--file_header',
        type=str,
        default='',
        help='Short prefix for output image filenames.'
    )
    args = vars(parser.parse_args())
    lat = args.pop('lat')
    lon = args.pop('lon')
    scale = args.pop('scale')
    N_images = args.pop('N_images')
    file_header = args.pop('file_header')
    bbox = geobox.bbox_from_scale(lat, lon, scale)
    grabber = dg_grabber.DGImageGrabber(**args)
    records = grabber(bbox, N_images=N_images, write_styles=['DRA'],
                      file_header=file_header)
    if len(records) == 0:
        print('Try expanding the date range, change scale to change ' +
               'sensor, or access dg_grabber.py for more options.')
    else:
        for r in records:
            print(r)
        

