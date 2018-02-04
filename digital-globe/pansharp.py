""" Routine to download an image from the DG Catalog given a bounding box
and Catalog ID.

Ref: http://gbdxtools.readthedocs.io/en/latest/image_classes.html

Image specs:  Pansharpened, atmosphere corrected, RGB bands

Inputs: CatalogID, bounding box, flag option to equalize histograms
    Note: flag is set with @ in place of - to allow input of negative numbers

Output: PNG file.

Usage: python pansharp_grab.py CatalogID  bounding box [@e]

Ex: python pansharp_grab.py 1040010034CDD100 151.269378, -33.898346, 151.286458, -33.886092 @e
"""

import argparse
import matplotlib.pyplot as plt
from gbdxtools import CatalogImage

# TODO, using cv2 tools
def equilize_histogram(img):
    return

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
        dest='equilize',
        action='store_true',
        help='Flag (True if set / no value required.)'
    )
    args = parser.parse_args()
    bbox = [float(i.split(',')[0]) for i in args.bounding_box]
    outfile = args.catalog_id + '_LL{:.6f}_{:.6f}'.format(*bbox[:2])

    img = CatalogImage(
        args.catalog_id, 
        bbox=bbox, 
        pansharpen=True,
        acomp=True
    )
    
    rgb = img.rgb()
    
    if args.equilize:
        rgb = equilize_histogram(rgb)
        outfile = outfile + '-equi'
    
    plt.imsave(outfile + '.png', rgb)
   