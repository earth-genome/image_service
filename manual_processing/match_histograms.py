"""Command-line wrapper for the rio-hist routine to match image histograms.

Usage: python match_histograms.py source_img.tif ref_img.tif

Image types .png, .jpg are also supported.  It is assumed that pixel
values in the two images have the same range, be it (0,255), (0., 1.), etc.  

This routine operates on R, G, B bands in succession and does not offer
matching in other color spaces as does rio hist.  

"""

import argparse
import sys

import numpy as np
from rio_hist import match
import skimage.io

IMAGE_EXTS = ('tif', 'tiff', 'jpg', 'png')

def parse_filename(filename):
    """Extract a prefix and extension from filename."""
    splits = filename.split('.')
    ext = splits[-1]
    if ext not in IMAGE_EXTS:
        sys.exit('Supported image files end in one of {}'.format(IMAGE_EXTS))
    prefix = '.'.join(splits[:-1])
    return prefix, ext

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Match image histograms')
    parser.add_argument(
        'src_filename',
        type=str,
        help='Filename of image to modify.  Formats: {}'.format(IMAGE_EXTS)
    )
    parser.add_argument(
        'ref_filename',
        type=str,
        help='Filename of image with histogram to match, also {}'.format(
            IMAGE_EXTS)
    )
    args = vars(parser.parse_args())

    src_prefix, src_ext = parse_filename(args['src_filename'])
    _, ref_ext = parse_filename(args['ref_filename'])
    
    src = skimage.io.imread(args['src_filename'])
    ref = skimage.io.imread(args['ref_filename'])
    if src.dtype != ref.dtype:
        sys.exit('Dtypes {}, {} do not match.'.format(src.dtype, ref.dtype))

    matched = src.copy()
    for band in range(3):
        matched.T[band] = match.histogram_match(src.T[band], ref.T[band])

    skimage.io.imsave(src_prefix+'-matched.'+src_ext, matched)
        

