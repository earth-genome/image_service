
import argparse
import glob
import json
import os
import subprocess

from shapely import geometry

# In light testing, this seems to be a reasonable and common 16-bit value
# (max 2**16 - 1 = 65535) to set as white point in a linear rescaling
# of the Landsat image histogram. Can be overriden with command line flag -wp.
WHITE = 3500

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

def crop_and_rescale(vrtfile, bounds, whitepoint):
    """Crop virtual image and linearly rescale the image histogram.

    Arguments: 
        vrtfile: A GDAL .vrt image file
        bounds: lat/lon coordinates [minx, miny, maxx, maxy], or []
        whitepoint: The 16-bit image value that should be reset to white

    Output: A geotiff

    Returns: Geotiff filename
    """
    tiffile = vrtfile.split('.vrt')[0] + '.tif'
    commands = [
        'gdal_translate', vrtfile, tiffile,
        '-co', 'COMPRESS=LZW',
        '-colorinterp', 'red,green,blue',
        '-ot', 'Byte', '-scale', '0', str(whitepoint), '0', '255']
    if bounds:
        gdal_bounds = [str(bounds[n]) for n in (0, 3, 2, 1)]
        commands += ['-projwin_srs', 'EPSG:4326', '-projwin', *gdal_bounds]
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
        default=WHITE,
        help='16-bit image value to be reset to white. Default: {}'.format(
            WHITE)
    )
    args = parser.parse_args()

    if args.geojson:
        with open(args.geojson, 'r') as f:
            collection = json.load(f)
        geom = extract_geom(collection)
        bounds = geometry.asShape(geom).bounds
    else:
        bounds = []

    bandfiles = glob.glob('*band?.tif')
    grouped = partition(bandfiles)
    geotiffs = []
    for prefix, files in grouped.items():
        rgbfiles = [prefix + 'band{}.tif'.format(b) for b in args.bandlist]
        rgbfiles = [f for f in rgbfiles if f in files]
        if len(rgbfiles) != 3: 
            print('Incomplete R-G-B set for file prefix {}'.format(prefix))
            continue
        vrtfile = combine_bands(prefix, rgbfiles)
        outfile = crop_and_rescale(vrtfile, bounds, args.white_point)
        os.remove(vrtfile)
        geotiffs.append(outfile)
    print(geotiffs)

    
