""" Routine to download an image from the DG Catalog given a bounding box
and Catalog ID.

This is a legacy routine (coded prior to dg_grabber.py) but still useful
for downloading after extracting a Catalog ID and bounding box from
Jupyter @ notebooks.geobigdata.io.

Ref: http://gbdxtools.readthedocs.io/en/latest/image_classes.html

Inputs: CatalogID, bounding box, flag option (@a) for atmospheric compensation,
    flag option (@p) to pansharpen imagery.
    Note: flag is set with @ in place of - to allow input of negative
    numbers.
    Note: Pansharpening implies a four-fold increase in resolution and
    corresponding increase in tile downloads.

Output: Dynamical-range-adjusted RGB PNG image file.

Usage: python grab_by_id_bbox.py CatalogID  bounding box [@a] [@p]

Ex: python grab_by_id_bbox.py 1040010034CDD100 151.269378, -33.898346, 151.286458, -33.886092 @p
"""
import numpy as np
import argparse
import skimage.io
from gbdxtools import CatalogImage

def grab_rgb(catalog_id, bbox, pansharpen=True, acomp=False):
    """Grab an image from GBDX CatalogImage. Return the RGB."""
    img = CatalogImage(
        catalog_id, 
        bbox=bbox, 
        pansharpen=pansharpen,
        acomp=acomp
    )   
    return img.rgb()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prefix_chars='@',
        description='Grab a pansharpened image from GBDX.')
    parser.add_argument('catalog_id',
        type=str,
        help='GBDX Catalog ID: {}'.format('1040010034CDD100')
    )
    parser.add_argument('bounding_box',
        type=str,
        nargs='+',
        help='Four coordinates of a bounding box: {}'.format(
            '151.269378, -33.898346, 151.286458, -33.886092')
    )
    parser.add_argument(
        '@p', '@@pansharpen',
        dest='pansharpen',
        action='store_true',
        help='Flag (True if set / no value required.)'
    )
    parser.add_argument(
        '@a', '@@acomp',
        dest='acomp',
        action='store_true',
        help='Flag (True if set / no value required.)'
    )
    args = parser.parse_args()
    bbox = [float(i.split(',')[0]) for i in args.bounding_box]
    outfile = args.catalog_id + '_LL{:.6f}_{:.6f}'.format(*bbox[:2])
    rgb = grab_rgb(
        args.catalog_id, 
        bbox=bbox, 
        pansharpen=args.pansharpen,
        acomp=args.acomp
    )
    skimage.io.imsave(outfile + '.png', rgb)
   
