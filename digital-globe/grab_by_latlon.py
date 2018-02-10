"""Download an image from the DG catalog given lat/lon and scale.

Inputs:
    latitude, longitude,
    scale (size of image (width or height) in km)

Usage:
    python grab_by_latlon.py 38.8977 -77.0365 -s 3.5
    Outputs: PNG image

Parameters assumed: offNadirAngle=None, acomp=True, proj='EPSG:4326'
Image source (incl. sensor and pansharpening) determined by default.
For more flexibility, see/use dg_grabber.py.

For command line syntax with additional supported option:
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
        default='2012-01-01T09:51:36.0000Z',
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
    scale = args.pop('scale')
    bbox = dg_grabber.make_bbox(lat, lon,
                              dg_grabber.latitude_from_dist(scale),
                              dg_grabber.longitude_from_dist(scale,lat))
    grabber = dg_grabber.DGImageGrabber(bbox, **args)
    imgs, records = grabber()
    if len(imgs) == 0:
        print ('Try expanding the date range, change scale to change ' +
               'sensor, or access dg_grabber.py for more options.')
        sys.exit(1)
    rgbs = [img.rgb() for img in imgs]  # alt. could do: raw = img.read()
    for rgb, rec in zip(rgbs, records):
        outfile = (rec['identifier'] + '_' + rec['properties']['timestamp'] +
                '_lat{:.4f}lon{:.4f}size{:.2f}km'.format(lat, lon, scale) +
                '.png')
        print 'Record:\n{}'.format(rec)
        print '\nSaving to {}\n'.format(outfile)
        plt.imsave(outfile, rgb)

