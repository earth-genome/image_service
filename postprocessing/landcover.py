"""Band algebra for common remote sensing indices.

For usage,
$ python landcover.py -h

"""


import argparse
import sys

import skimage.io

INDICES = ['ndvi', 'ndwi']

def ndvi(img):
    """Compute ndvi on four-band img."""
    b, g, r, nir = img.T
    ndvi = (nir - r)/(nir + r)
    return ndvi.T

def ndwi(img):
    b, g, r, nir = img.T
    ndwi = (g - nir)/(g + nir)
    return ndwi.T
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Compute common remote sensing indices.'
    )
    parser.add_argument(
        'filename',
        type=str,
        help='Filename for a 4-band tif image, bands ordered B-G-R-NIR.'
    )
    parser.add_argument(
        'index_name',
        type=str,
        help='Index type from {}'.format(INDICES)
    )
    args = parser.parse_args()
    img = skimage.io.imread(args.filename).astype('float32')
    if args.index_name.lower() == 'ndvi':
        index = ndvi(img)
    elif args.index_name.lower() == 'ndwi':
        index = ndwi(img)
    else:
        sys.exit('Supported indices: {}'.format(('ndvi', 'ndwi')))
    outfile = args.filename.split('.tif')[0] + '-' + args.index_name + '.png'
    skimage.io.imsave(outfile, index)
        
