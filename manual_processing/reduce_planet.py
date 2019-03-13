"""Routine to mosaic, crop and color correct geotiff tiles downloaded from 
the Planet Explorer website, or more generally, to mosaic and crop geotiffs.

First, assemble all tiles (*.tif files) in a single tile_dir, with no 
extraneous files ending in .tif.

Usage for Planet analytic: 
$ python reduce_planet.py 3 2 1 -d tile_dir -s base [-g footprint.geojson] 
    [-b 8] -o outfile.tif

3 2 1 indicate R-G-B band orderings.  

For Planet Analytic: R-G-B-NIR are bands 3 2 1 4
For Planet Visual or other visual image: R-G-B are bands 1 2 3

For more details see:
$ python reduce_planet.py -h

Output is one or two files: outfile.tif (raw image) and 
outfilevisbase.tif (color corrected to style 'base').  

For Planet Visual, do not supply a color correction (-s) option.
To re-mosaic tiles already reduced, again do not supply a -s option. 

"""

import argparse
from inspect import getsourcefile
import glob
import os
import subprocess
import sys

import numpy as np
import rasterio

current_dir = os.path.dirname(os.path.abspath(getsourcefile(lambda:0)))
parent_dir = os.path.join(current_dir, '..')
sys.path.insert(1, parent_dir)

from manual_processing import reduce_landsat
from postprocessing import color

ALLOWED_BIT_DEPTHS = (8, 16)

def vrt_merge(files, outfile, srcnodata=0):
    """Build a virtual mosaic from input files.""" 
    vrtfile = outfile.split('.tif')[0] + '.vrt'
    commands = ['gdalbuildvrt', '-srcnodata', str(srcnodata), vrtfile, *files]
    subprocess.call(commands)
    return vrtfile

def resolve(vrtfile, bandlist=[], bounds=[]):
    """Convert vrtfile to tif while resolving bands and geographic bounds.

    Arguments:
        vrtfile: A file output by gdalbuildvrt
        bandlist: Ordered list of output bands
        bounds: List of geographic corner coordinates (shapely format)

    Outputs a geotiff; returns the filename.
    """
    tiffile = vrtfile.split('.vrt')[0] + '.tif'
    commands = ['gdal_translate', vrtfile, tiffile, '-co', 'COMPRESS=LZW']
    
    if bandlist:
        dressed_bands = np.asarray([('-b', str(b)) for b in bandlist])
        commands += [*dressed_bands.flatten()]
        if len(bandlist) == 3:
            commands += ['-colorinterp', 'red,green,blue']
            
    if bounds:
        gdal_bounds = [str(bounds[n]) for n in (0, 3, 2, 1)]
        commands += ['-projwin_srs', 'EPSG:4326', '-projwin', *gdal_bounds]

    subprocess.call(commands)
    os.remove(vrtfile)
    return tiffile

def get_bit_depth(image_files):
    """Determine and check consistency of input bit depth."""
    dtypes = []
    for image_file in image_files:
        with rasterio.open(image_file) as f:
            dtypes.append(f.profile['dtype'])
            
    input_bit_depth = np.iinfo(next(iter(dtypes))).bits
    if not len(set(dtypes)) <= 1 or input_bit_depth not in ALLOWED_BIT_DEPTHS:
        
        raise ValueError('Input dtypes must all match, either Unit8 or Unit16. '
                         'Dtypes: {}'.format(list(zip(image_files, dtypes))))
    
    return input_bit_depth

def change_bit_depth(image_file, input_bit_depth, output_bit_depth):
    """Rewrite the image file to a different bit depth."""
    assert in_bit_depth in ALLOWED_BIT_DEPTHS
    assert out_bit_depth in ALLOWED_BIT_DEPTHS

    tmp_file = image_file + '-tmp'
    os.rename(image_file, tmp_file)
    
    img_type = 'UInt16' if out_bit_depth == 16 else 'Byte'
    commands = [
        'gdal_translate', tmp_file, image_file,
        '-ot', img_type, '-scale', '0', str(2**input_bit_depth - 1),
        '0', str(2**output_bit_depth - 1)
    ]
    subprocess.call(commands)
    os.remove(tmp_file)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'bandlist',
        type=str,
        nargs='+',
        help=('Band numbers to assemble in R-G-B(-NIR) order. '
              'For Planet Analytic: 3 2 1 or 3 2 1 4. '
              'For Planet Visual or other RGB input: 1 2 3.')
    )
    parser.add_argument(
        '-g', '--geojson',
        type=str,
        help='Geojson file expressing area of interest for optional crop.'
    )
    parser.add_argument(
        '-s', '--color_style',
        type=str,
        choices=list(color.STYLES.keys()),
        help='Optional color correction style.'
    )
    parser.add_argument(
        '-d', '--image_dir',
        type=str,
        default='',
        help='Directory containing image files. Defaults to pwd.'
    )
    parser.add_argument(
        '-b', '--bit_depth',
        type=int,
        choices=ALLOWED_BIT_DEPTHS,
        help=('Bit depth of output image. Defaults to input bit depth.')
    )
    req_group = parser.add_argument_group(title='required flags')
    req_group.add_argument(
        '-o', '--outfile',
        type=str,
        required=True,
        help='Name of base output file, e.g. outfile.tif.'
    )
    args = parser.parse_args()

    bounds = reduce_landsat.get_bounds(args.geojson) if args.geojson else []

    # N.B. for gdalbuildvrt, if there is spatial overlap between files, 
    # content is fetched from files that appear later in the list.
    image_files = glob.glob(os.path.join(args.image_dir, '*.tif'))
    image_files.sort()
    
    input_bit_depth = get_bit_depth(image_files)
    if args.bit_depth:
        if args.bit_depth not in ALLOWED_BIT_DEPTHS:
            raise ValueError('Invalid output bit depth: {}.'.format(bit_depth))
        elif args.bit_depth == input_bit_depth:
            args.bit_depth = None

    vrtfile = vrt_merge(image_files, args.outfile)
    tiffile = resolve(vrtfile, bandlist=args.bandlist, bounds=bounds)
    
    if args.color_style:
        color.ColorCorrect(cores=1, style=args.color_style)(tiffile)
        
    if args.bit_depth:
        change_bit_depth(tiffile, input_bit_depth, args.bit_depth)
    

