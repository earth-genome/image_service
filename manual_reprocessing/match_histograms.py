"""Command-line wrapper for the rio-hist routine to match image histograms.

Usage: 
$ python match_histograms.py source_img.tif ref_img.tif [-nd nodata_value]

For more:
$ python match_histograms.py -h

It is assumed that pixel values in the two images have the same range, 
be it (0,255), (0., 1.), etc. This is lightly enforced by asserting that 
the two image arrays have the same dtypes. Due to a bug in masked array 
handling with rio_hist, only integer dtypes are allowed if a nodata_value is
given.

This routine operates on R, G, B bands in succession and does not offer
matching in other color spaces as does rio hist.  

"""

import argparse

import numpy as np
import rasterio
from rio_hist import match

def parse_filename(filename):
    """Extract a prefix and extension from filename."""
    splits = filename.split('.')
    ext = splits[-1]
    prefix = '.'.join(splits[:-1])
    return prefix, ext

def mask_image(img, mask_val):
    """Build a masked array for an image.

    The mask applies to points where all bands at a given (row, col) equal 
    the mask_val, e.g. [R, G, B] = [0, 0, 0]. Fourth bands are likely alpha 
    bands and are excluded from this check.

    Due to a bug exposed in an interaction between np.unique sort and the 
    masked array default fill value, matching fails for masked uint8/16 
    images. A work-around is to cast the masked image to type int32, so that
    the default fill value (999999) is not modded out to 8 or 16 bits. 
    
    stackoverflow.com/questions/42360616/unexpected-numpy-unique-behavior

    Therefore this masking will corrupt non-integer-type images.

    Arguments: 
        img: An array of shape (bands, rows, cols)
        mask_val: Integer value to mask

    Raises TypeError: For non-integer type images.

    Returns: Numpy masked array
    """
    if not np.issubdtype(img.dtype, np.integer):
        raise TypeError('When supplying a nodata value, images must be integer'
                        'type. See notes in match_histograms.py for details.')

    mask_vals = [mask_val for _ in range(len(img[:3]))]
    mask_coords = np.all(img[:3].T == mask_vals, axis=-1).T
    mask = np.array([mask_coords for _ in range(len(img))])
    return np.ma.masked_array(img.astype('int32'), mask=mask)

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
    parser.add_argument(
        '-nd', '--nodata_val',
        type=int,
        help='Integer no data value to (optionally) exclude from match.'
    )
    args = parser.parse_args()

    with rasterio.open(args.src_filename) as f:
        src = f.read()
        profile = f.profile.copy()
    with rasterio.open(args.ref_filename) as f:
        ref = f.read()
    if src.dtype != ref.dtype:
        raise TypeError('Dtypes of the images do not match.')
    if len(src) != len(ref):
        raise ValueError('Images do not have the same number of bands.')
            
    matched = []
    if args.nodata_val is None:
        for band in range(len(src)):
            mband = match.histogram_match(src[band], ref[band])
            matched.append(mband)
    else:
        masked_src = mask_image(src, args.nodata_val)
        masked_ref = mask_image(ref, args.nodata_val)
        for band in range(len(src)):
            mband = match.histogram_match(masked_src[band], masked_ref[band])
            mband[masked_src.mask[band]] = args.nodata_val
            matched.append(mband)

    if len(matched) == 3:
        profile.update({'photometric': 'RGB'})

    src_prefix, src_ext = parse_filename(args.src_filename)
    with rasterio.open(src_prefix+'-matched.'+src_ext, 'w', **profile) as f:
        f.write(np.asarray(matched, dtype=src.dtype))

        

