
import sys

import numpy as np
import rasterio

def write_alpha(path):
    """Write a nodata mask as an alpha band of an image.

    Note: This routine assumes a uint8 dtype. Otherwise the mask would need
        to be converted in type and rescaled before being stacked as a 
        fourth image band.

    Returns: Path to the 4-band image.
    """
    with rasterio.open(path) as f:
        img = f.read()
        msk = f.read_masks(2)
        prof = f.profile

    stack = np.vstack([img, msk.reshape(1, *msk.shape)]) 
    prof.update({'nodata': None, 'count': 4, 'dtype': 'uint8'}) 

    splits = path.split('.')
    base = '.'.join(splits[:-1])
    ext = splits[-1]
    outpath = base + 'alphamsk.' + ext
    
    with rasterio.open(outpath, 'w', **prof) as f:
        f.write(stack)

if __name__ == '__main__':
    write_alpha(sys.argv[1])
    
