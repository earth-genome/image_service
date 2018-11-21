"""Routines to process Landsat Surface Reflectance tiles. 

Ordered from https://earthexplorer.usgs.gov, scenes are delivered as tar
files containing each band as a separte TIF, with filenames of form

LC08_L1TP_037034_20170309_20180125_01_T1_sr_band2.tif

For Landsat8, R-G-B images are built from bands 4-3-2. For Landsat5, R-G-B 
are built from bands 3-2-1.  

Usage: Untar everything into a folder. Multiple scenes are fine, as the 
program will untangle them. The only restriction is that all band files
for a scene must share a common prefix, with filename of form prefixband?.tif.

Then: 

$ python reduce_landsat.py 4 3 2 -g footprint.geojson -d image_dir -wp 3500

For help:
$ python reduce_landsat.py -h

The band ordering 4 3 2 or 3 2 1 is required. The scenes will be
cropped to the footprint, if given. The image_dir defaults to pwd if not
specified. 

The -wp flag sets the white point of the output images. The Landsat
histograms are confined to a small part of the possible 16-bit
range. In light testing, 3500 (max 2**16 - 1 = 65535) seems reasonable
default white point for a linear rescaling of the histogram. Adjust
this if the output image is overly dark or overly saturated.

The routine outputs one 8-bit TIF for each processed scene.

"""

import argparse
import glob
import json
import os
import subprocess

from shapely import geometry

WHITE_PT = 3500
BIT_DEPTH = 8

def partition(bandfiles):
    """Partition input filenames according to shared prefix.

    Returns: dict of prefixes and filenames
    """
    prefixes = set([f.split('band')[0] for f in bandfiles])
    partition = {p:[f for f in bandfiles if p in f] for p in prefixes}
    return partition

def combine_bands(prefix, rgbfiles):
    """Assemble R-G-B image bands into GDAL .vrt file."""
    combined = prefix + '.vrt'
    commands = ['gdalbuildvrt', '-separate', combined, *rgbfiles]
    subprocess.call(commands)
    return combined

def crop_and_rescale(vrtfile, bounds, **kwargs):
    """Crop virtual image and linearly rescale the image histogram.

    Arguments: 
        vrtfile: A GDAL .vrt image file
        bounds: lat/lon coordinates, ordered [minx, miny, maxx, maxy], or []
        **kwargs including:
            white_point: The 16-bit image value that should be reset to white
            bit_depth: bit-depth for output image, either 8 or 16

    Output: A geotiff

    Returns: Geotiff filename
    """
    tiffile = vrtfile.split('.vrt')[0] + '.tif'
    commands = [
        'gdal_translate', vrtfile, tiffile,
        '-co', 'COMPRESS=LZW',
        '-colorinterp', 'red,green,blue']
    if bounds:
        gdal_bounds = [str(bounds[n]) for n in (0, 3, 2, 1)]
        commands += ['-projwin_srs', 'EPSG:4326', '-projwin', *gdal_bounds]
    if kwargs['bit_depth'] == 8:
        commands += [
            '-ot', 'Byte',
            '-scale', '0', str(kwargs['white_point']), '0', '255'
        ]
    elif kwargs['bit_depth'] == 16:
        commands += [
            '-ot', 'UInt16',
            '-scale', '0', str(kwargs['white_point']), '0', '65535'
        ]
    else:
        raise ValueError('Invalid output bit depth: {}.'.format(
            kwargs['bit_depth']))
    subprocess.call(commands)
    return tiffile

def extract_geom(geojson):
    """Find the first available geometry in the geojson."""
    if geojson['type'] == 'FeatureCollection':
        features = geojson['features']
        geom = features[0]['geometry']
        if len(features) > 1:
            print('Proceeding with first available geometry: {}'.format(geom))
    elif geojson['type'] == 'Feature':
        geom = geojson['geometry']
    else:
        raise TypeError('GeoJSON type {} not recognized.'.format(
            geojson['type']))
    return geom

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process Landsat surface reflectance tiles. Routine ' +
            'will attempt to process all files in pwd with filenames ' +
            'of form *band?.tif.'
    )
    parser.add_argument(
        'bandlist',
        type=str,
        nargs='+',
        help='Band numbers to assemble in R-G-B order. E.g. 4 3 2 ' +
            'for Landsat 8 or 3 2 1 for Landsat 5.')
    parser.add_argument(
        '-g', '--geojson',
        type=str,
        help='Geojson file expressing area of interest for optional crop.'
    )
    parser.add_argument(
        '-wp', '--white_point',
        type=int,
        default=WHITE_PT,
        help='16-bit image value to be reset to white. Default: {}'.format(
            WHITE_PT)
    )
    parser.add_argument(
        '-d', '--image_dir',
        type=str,
        default='',
        help='Directory containing image band files. Defaults to pwd.'
    )
    parser.add_argument(
        '-b', '--bit_depth',
        type=int,
        default=BIT_DEPTH,
        help='Bit-depth of output image, either 8 or 16. Default: {}'.format(
            BIT_DEPTH)
    )
    args = parser.parse_args()

    if args.geojson:
        with open(args.geojson, 'r') as f:
            collection = json.load(f)
        geom = extract_geom(collection)
        bounds = geometry.asShape(geom).bounds
    else:
        bounds = []

    bandfiles = glob.glob(os.path.join(args.image_dir,'*band?.tif'))
    grouped = partition(bandfiles)
    geotiffs = []
    for prefix, files in grouped.items():
        rgbfiles = [prefix + 'band{}.tif'.format(b) for b in args.bandlist]
        rgbfiles = [f for f in rgbfiles if f in files]
        if len(rgbfiles) != 3: 
            print('Incomplete R-G-B set for file prefix {}'.format(prefix))
            continue
        vrtfile = combine_bands(prefix, rgbfiles)
        outfile = crop_and_rescale(vrtfile, bounds, **vars(args))
        os.remove(vrtfile)
        geotiffs.append(outfile)
    print('Files written: {}'.format(geotiffs))

    
