"""Download an image from the DG catalog given lat/lon and scale.

Inputs:
    latitude, longitude,
    scale (half size of image (width or height) in km

Usage:
    python grab_by_latlon.py 38.8977 -77.0365 -s .5
    Outputs: PNG image

For command line syntax with additional options:
python grab_draft.py -h

"""

import sys
import matplotlib.pyplot as plt
import argparse

import dg_grabber

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
        help='Approx. image size (half width or height) in km, ' +
            'e.g.: {}'.format(1.0)
    )
    parser.add_argument(
        '-c', '--clouds',
        type=int,
        default=10,
        help='Maximum percentage cloud cover, e.g.: {}'.format(10)
    )
    parser.add_argument(
        '-sd', '--startDate',
        type=str,
        default='2016-01-01T09:51:36.0000Z',
        help='Isoformat start date for image search: {}'.format(
            '2016-01-01T09:51:36.0000Z')
    )
    parser.add_argument(
        '-ed', '--endDate',
        type=str,
        default=None,
        help='Isoformat end date for image search: {}'.format(
            '2018-01-01T09:51:36.0000Z')
    )
    args = vars(parser.parse_args())
    lat = args.pop('lat')
    lon = args.pop('lon')
    grabber = dg_grabber.DGImageGrabber(lat, lon, **args)
    record, img = grabber()
    if img is None:
        print ('No image found. Try expanding the date range or ' +
               'change the scale to access a different image source.')
        sys.exit(1)
    aoi = img.aoi(bbox=grabber.bbox)
    rgb = aoi.rgb()  # alternately, for raw multispectrum: raw = aoi.read()
    outfile = (record['identifier'] + '_' +
                record['properties']['timestamp'] +
                '_lat{:.4f}lon{:.4f}size{:.2f}km'.format(
                grabber.lat, grabber.lon, grabber.scale) + '.png')
    print 'Record:\n{}'.format(record)
    print '\nSaving to {}'.format(outfile)
    plt.imsave(outfile, rgb)

