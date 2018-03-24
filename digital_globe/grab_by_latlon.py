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
        help='Approx. image size (width or height) in km, ' +
            'e.g.: {}'.format(1.0)
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
        default=10,
        help='Maximum percentage cloud cover, default: {}'.format(10)
    )
    parser.add_argument(
        '-sd', '--startDate',
        type=str,
        default='2012-01-01T09:51:36.0000Z',
        help='Isoformat start date for image search, default: {}'.format(
            '2012-01-01T09:51:36.0000Z')
    )
    parser.add_argument(
        '-ed', '--endDate',
        type=str,
        default=None,
        help='Isoformat end date for image search (default None).'
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
    bbox = dg_grabber.bbox_from_scale(lat, lon, scale) 
    grabber = dg_grabber.DGImageGrabber(**args)
    imgs, records = grabber(bbox, N_images=N_images, write_styles=['DRA'],
                            file_header=file_header)
    if len(imgs) == 0:
        print('Try expanding the date range, change scale to change ' +
               'sensor, or access dg_grabber.py for more options.')
    else:
        for r in records:
            print(r)
        

