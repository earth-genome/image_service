"""Band algebra for common remote sensing indices.

For usage,
$ python landcover.py -h

"""


import argparse
import sys

import rasterio

INDICES = ['ndvi', 'ndwi']

def compute_index(path, index):
    """Compute a landcover index on a four-band GeoTiff.

    Arguments: 
        path: Path to a GeoTiff with bands ordered R-G-B-NIR
        index: One of the available INDICES above
    
    Returns: Path to a grayscale GeoTiff
    """
    with rasterio.open(path) as f:
        img = f.read().astype('float32')
        profile = f.profile.copy()
    red, green, blue, nir = img

    if index == 'ndvi':
        computed = (nir - red)/(nir + red)
    elif index == 'ndwi':
        computed = (green - nir)/(green + nir)
    else:
        raise ValueError('Landcover index not recognized.')

    profile.update({'count': 1, 'dtype': rasterio.float32})
    outfile = path.split('.tif')[0] + index + '.tif'
    with rasterio.open(outfile, 'w', **profile) as f:
        f.write(computed, 1)
    return outfile

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Compute common remote sensing indices.'
    )
    parser.add_argument(
        'filename',
        type=str,
        help='Filename for a 4-band tif image, bands ordered R-G-B-NIR.'
    )
    parser.add_argument(
        'index_name',
        type=str,
        choices=INDICES,
        help='Index type from {}'.format(INDICES)
    )
    args = parser.parse_args()
    compute_index(args.filename, args.index_name)
        
