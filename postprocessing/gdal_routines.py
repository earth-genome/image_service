
import os
import subprocess

import numpy as np

from geobox import geobox

def reproject(filename, epsg_code, clean=True):
    """Reproject GeoTiff to a common projection.

    Arguments:
        filename: GeoTiff filename
        epsg_code: integer EPSG code (e.g. 4326)
        clean: True/False to delete the input file. 

    Output: Writes a GeoTiff.

    Returns: New GeoTiff filename
    """
    targetname = filename.split('.tif')[0] + '-reproj.tif'
    commands = [
        'gdalwarp',
        '-t_srs', 'EPSG:'+str(epsg_code),
        '-r', 'bilinear',
        '-co', 'COMPRESS=LZW',
        filename, targetname]
    subprocess.call(commands)
    if clean:
        os.rename(targetname, filename)
        return filename
    else:
        return targetname

def crop_and_reband(filename, bbox, output_bands, clean=True):
    """Crop GeoTiff to boundingbox and set output_bands.

    Arguments:
        filename: GeoTiff filename
        bbox: A shapely box.
        output_bands: A list of bands by number (indexed from 1)
        clean: True/False to delete the input file. 

    Output: Writes a GeoTiff.

    Returns: New GeoTiff filename.
    """
    tags = ('_bbox{:.4f}_{:.4f}_{:.4f}_{:.4f}'.format(*bbox.bounds))
    targetname = filename.split('.tif')[0] + tags + '.tif'
    bounds = geobox.shapely_to_gdal_box(bbox)
    dressed_bands = np.asarray([('-b', str(b)) for b in output_bands])
    commands = [
        'gdal_translate',
        '-projwin_srs', 'EPSG:4326',
        '-projwin', *[str(b) for b in bounds],
        *dressed_bands.flatten(),
        '-co', 'COMPRESS=LZW',
        filename, targetname
    ]
    subprocess.call(commands)
    if clean:
        os.remove(filename)
    return targetname


def merge(filenames, clean=True):
    """Merge input GeoTiffs.

    Arguments:
        filenames: GeoTiff filenames
        clean: True/False to delete the input file. 

    Output: Writes a GeoTiff

    Returns: New GeoTiff filename (based on the first input GeoTiff name) 
    """
    targetname = filenames[0].split('.tif')[0] + '-merged.tif'
    # To keep this out of memory, create the target file with gdal_merge
    # but then copy data with gdalwarp:
    commands = [
        'gdal_merge.py',
        '-createonly',
        '-co', 'COMPRESS=LZW',
        '-of', 'GTiff',
        '-o', targetname,
        *filenames
    ]
    subprocess.call(commands)
    commands = [
        'gdalwarp',
        '--config', 'GDAL_CACHEMAX', '1000', '-wm', '1000',
        *filenames, targetname]
    subprocess.call(commands)
    if clean:
        for filename in filenames:
            os.remove(filename)
    return targetname
