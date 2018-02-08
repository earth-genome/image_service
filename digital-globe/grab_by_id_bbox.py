""" Routine to download an image from the DG Catalog given a bounding box
and Catalog ID.

Ref: http://gbdxtools.readthedocs.io/en/latest/image_classes.html

Image specs (default):  Atmosphere corrected, RGB bands

Inputs: CatalogID, bounding box, flag option (@e) to equalize histograms,
    flag option (@p) to pansharpen imagery.
    Note: flag is set with @ in place of - to allow input of negative
    numbers.
    Note: Pansharpening implies a four-fold increase in resolution and
    corresponding increase in tile downloads.

Output: PNG file(s). (With @e flag, both original and equalized are saved.)

Usage: python grab_by_id_bbox.py CatalogID  bounding box [@e] [@p]

Ex: python pansharp.py 1040010034CDD100 151.269378, -33.898346, 151.286458, -33.886092 @e 
"""
import numpy as np
import argparse
import cv2
import skimage
import matplotlib.pyplot as plt
from gbdxtools import CatalogImage

def grab_rgb(catalog_id, bbox, pansharpen=True, acomp=True):
    """Grab an image from GBDX CatalogImage. Return the RGB."""
    img = CatalogImage(
        catalog_id, 
        bbox=bbox, 
        pansharpen=pansharpen,
        acomp=acomp
    )   
    return img.rgb()

    
def equalize_histogram(img):
    """Equalize histogram of input image."""
    if img.dtype is not np.dtype('uint8'):
        img = skimage.img_as_ubyte(img)
    hsv = cv2.cvtColor(img, cv2.COLOR_RGB2HSV)
    hsv[:,:,2] = cv2.equalizeHist(hsv[:,:,2])
    equalized = cv2.cvtColor(hsv, cv2.COLOR_HSV2RGB)
    return equalized

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
        '@e', '@@equalize_histogram',
        dest='equalize',
        action='store_true',
        help='Flag (True if set / no value required.)'
    )
    parser.add_argument(
        '@p', '@@pansharpen',
        dest='pansharpen',
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
        acomp=True
    )
    plt.imsave(outfile + '.png', rgb)
    if args.equalize:
        rgb = equalize_histogram(rgb)
        plt.imsave(outfile+'-equi.png', rgb)
   
