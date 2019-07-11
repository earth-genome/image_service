"""Wrapper to restore georeferencing headers to an image file.

We lose georeferencing when editing an image in Photoshop. 
This routine restores headers from a georeferenced version of the scene. 

Usage: python restore_georef.py image_file.tif reference_geotiff.tif

Output: A file image_file-georef.tif

While color and spatial resolution may vary between the two, it is assumed 
that the geographical scene covered by image_file and reference_geotiff
are identical.

"""
import argparse

import rasterio

def transfer_georef(image_file, geotiff):
    """Apply georeferencing from geotiff to image_file."""
    with rasterio.open(geotiff) as f:
        ref_profile = f.profile

    with rasterio.open(image_file) as f:
        profile = f.profile
        img = f.read()

    profile.update({k:v for k,v in ref_profile.items()
                    if k in ['driver', 'crs', 'transform']})
    if profile['count'] == 3:
        profile.update({'photometric':  'RGB'})

    outpath = '.'.join(args.image_file.split('.')[:-1]) + '-georef.tif'
    with rasterio.open(outpath, 'w', **profile) as f:
        f.write(img)

    return outpath

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'image_file',
        help='Image file to receive georeferencing.')
    parser.add_argument(
        'geotiff',
        help='Geotiff to provide georeferencing.')
    args = parser.parse_args()

    outpath = transfer_georef(**vars(args))
    print('Wrote {}'.format(outpath))
