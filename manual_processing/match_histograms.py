"""Command-line wrapper for the rio-hist routine to match image histograms.

Usage: python match_histograms.py source_img.tif ref_img.tif

It is assumed that pixel values in the two images have the same range, 
be it (0,255), (0., 1.), etc.  

This routine operates on R, G, B bands in succession and does not offer
matching in other color spaces as does rio hist.  

"""

import argparse

import rasterio
from rio_hist import match

def parse_filename(filename):
    """Extract a prefix and extension from filename."""
    splits = filename.split('.')
    ext = splits[-1]
    prefix = '.'.join(splits[:-1])
    return prefix, ext

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Match image histograms')
    parser.add_argument(
        'src_filename',
        type=str,
        help='Filename of image to modify.'
    )
    parser.add_argument(
        'ref_filename',
        type=str,
        help='Filename of image with histogram to match.'
    )
    args = parser.parse_args()

    with rasterio.open(args.src_filename) as f:
        src = f.read()
        profile = f.profile.copy()
    with rasterio.open(args.ref_filename) as f:
        ref = f.read()
    assert src.dtype == ref.dtype
    assert len(src) == len(ref)
            
    matched = src.copy()
    for band in range(len(src)):
        matched[band] = match.histogram_match(src[band], ref[band])

    src_prefix, src_ext = parse_filename(args.src_filename)
    with rasterio.open(src_prefix+'-matched.'+src_ext, 'w', **profile) as f:
        f.write(matched)

        

