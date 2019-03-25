"""Routine to mosaic, crop and color correct geotiff tiles.

In the example that follows, we composite a scene from 
analytic tiles downloaded from the Planet Explorer website.

First, assemble all tiles (*.tif files) in a single tile_dir, with no 
extraneous files ending in .tif.

Usage for Planet analytic: 
$ python reduce_tiles.py 3 2 1 -d tile_dir -s base [-g footprint.geojson] 
    [-b 8] -o /path/to/outfile.tif

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
sys.path.insert(1, os.path.dirname(current_dir))

from manual_processing import reduce_landsat
from postprocessing import color

ALLOWED_BIT_DEPTHS = (8, 16)

def build_image(paths, outpath='./reduced.tif', **kwargs):
    """Produce one or more images from raw satellite image tiles.

    Arguments: 
        paths: List of paths to input geotiffs
        outpath: Path to the basic output image 
        
        kwargs (optional):
            geojson: A geojson feature or feature collection expressing
                area for crop
            bandlist: List of integer band numbers to assemble in R-G-B-(NIR)
                order
            bit_depth: Integer, new output bit depth 
            color_style: One of color.STYLES.keys()
    
    Returns: Paths to image file(s) written to disk
    """
    input_bit_depth = get_bit_depth(paths)
    bit_depth = kwargs.get('bit_depth')
    if bit_depth and bit_depth not in ALLOWED_BIT_DEPTHS:
        raise ValueError('Invalid output bit depth: {}.'.format(bit_depth))
    
    vrtfile = vrt_merge(paths, outpath)
    tiffile = resolve(vrtfile, **kwargs)
    outpaths = [tiffile]

    style = kwargs.get('color_style')
    if style:
        outpaths.append(color.ColorCorrect(cores=1, style=style)(tiffile))

    if bit_depth and bit_depth != input_bit_depth:
        for f in outpaths:
            change_bit_depth(f, input_bit_depth, bit_depth)
        
    return outpaths

def vrt_merge(paths, outpath, srcnodata=0):
    """Build a virtual mosaic from input paths.""" 
    vrtfile = outpath.split('.tif')[0] + '.vrt'
    commands = ['gdalbuildvrt', '-srcnodata', str(srcnodata), vrtfile, *paths]
    subprocess.call(commands)
    return vrtfile

def resolve(vrtfile, **kwargs):
    """Convert vrtfile to tif while resolving bands and geographic bounds.

    Arguments:
        vrtfile: A file output by gdalbuildvrt
        kwargs (optional):
            bandlist: Ordered list of output bands
            geojson: A geojson feature or feature collection expressing
                a crop region

    Outputs a geotiff; returns the filename.
    """
    tiffile = vrtfile.split('.vrt')[0] + '.tif'
    commands = ['gdal_translate', vrtfile, tiffile, '-co', 'COMPRESS=LZW']

    bandlist = kwargs.get('bandlist')
    if bandlist:
        dressed_bands = np.asarray([('-b', str(b)) for b in bandlist])
        commands += [*dressed_bands.flatten()]
        if len(bandlist) == 3:
            commands += ['-colorinterp', 'red,green,blue']

    geojson = kwargs.get('geojson')
    if geojson: 
        bounds = reduce_landsat.get_bounds(geojson)
        gdal_bounds = [str(bounds[n]) for n in (0, 3, 2, 1)]
        commands += ['-projwin_srs', 'EPSG:4326', '-projwin', *gdal_bounds]

    subprocess.call(commands)
    os.remove(vrtfile)
    return tiffile

def get_bit_depth(paths):
    """Determine and check consistency of input bit depth."""
    dtypes = []
    for path in paths:
        with rasterio.open(path) as f:
            dtypes.append(f.profile['dtype'])
            
    input_bit_depth = np.iinfo(next(iter(dtypes))).bits
    if not len(set(dtypes)) <= 1 or input_bit_depth not in ALLOWED_BIT_DEPTHS:
        
        raise ValueError('Input dtypes must all match, either Unit8 or Unit16. '
                         'Dtypes: {}'.format(list(zip(paths, dtypes))))
    
    return input_bit_depth

def change_bit_depth(path, input_bit_depth, output_bit_depth):
    """Rewrite the image at path to a different bit depth."""
    assert input_bit_depth in ALLOWED_BIT_DEPTHS
    assert output_bit_depth in ALLOWED_BIT_DEPTHS

    tmp_file = path + '-tmp'
    os.rename(path, tmp_file)
    
    img_type = 'UInt16' if output_bit_depth == 16 else 'Byte'
    commands = [
        'gdal_translate', tmp_file, path,
        '-ot', img_type, '-scale', '0', str(2**input_bit_depth - 1),
        '0', str(2**output_bit_depth - 1)
    ]
    subprocess.call(commands)
    os.remove(tmp_file)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'bandlist',
        type=int,
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
        '-d', '--tile_dir',
        type=str,
        default='',
        help='Directory containing image tiles. Defaults to pwd.'
    )
    parser.add_argument(
        '-b', '--bit_depth',
        type=int,
        choices=ALLOWED_BIT_DEPTHS,
        help=('Bit depth of output image. Defaults to input bit depth.')
    )
    req_group = parser.add_argument_group(title='required flags')
    req_group.add_argument(
        '-o', '--outpath',
        type=str,
        required=True,
        help='Path to basic output file, e.g. ./outfile.tif.'
    )
    args = parser.parse_args()

    # N.B. for gdalbuildvrt, if there is spatial overlap between files, 
    # content is fetched from files that appear later in the list.
    tiles = glob.glob(os.path.join(args.tile_dir, '*.tif'))
    tiles.sort()
    
    outpaths = build_image(tiles, **vars(args))
    print('Wrote {}'.format(outpaths))
