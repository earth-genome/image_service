
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

def crop(filename, bbox, clean=True):
    """Crop GeoTiff to bounding box.

    Arguments:
        filename: GeoTiff filename
        bbox: A shapely box.
        clean: True/False to delete the input file. 

    Output: Writes a GeoTiff.

    Returns: New GeoTiff filename.
    """
    tags = ('_bbox{:.4f}_{:.4f}_{:.4f}_{:.4f}'.format(*bbox.bounds))
    targetname = filename.split('.tif')[0] + tags + '.tif'
    bounds = geobox.shapely_to_gdal_box(bbox)
    commands = [
        'gdal_translate',
        '-projwin_srs', 'EPSG:4326',
        '-projwin', *[str(b) for b in bounds],
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
    commands = [
        'gdalwarp',
        '--config', 'GDAL_CACHEMAX', '1000', '-wm', '1000',
        '-r', 'bilinear',
        '-co', 'COMPRESS=LZW',
        *filenames, targetname]
    subprocess.call(commands)
    if clean:
        for filename in filenames:
            os.remove(filename)
    return targetname

# If an alpha band is kept through crop and merge, gdalwarp will use it to
# correctly handle no-data values in merging partial scenes.
# Thus, reband() should be reserved for a final step in processing:

def reband(filename, output_bands, clean=True):
    """Return GeoTiff with output_bands.

    Arguments:
        filename: GeoTiff filename
        output_bands: A list of bands by number (indexed from 1)
        clean: True/False to delete the input file. 

    Output: Writes a GeoTiff.

    Returns: New GeoTiff filename.
    """
    targetname = filename.split('.tif')[0] + '-RGB.tif'
    dressed_bands = np.asarray([('-b', str(b)) for b in output_bands])
    commands = [
        'gdal_translate',
        *dressed_bands.flatten(),
        '-co', 'COMPRESS=LZW',
        filename, targetname
    ]
    subprocess.call(commands)
    if clean:
        os.rename(targetname, filename)
        return filename
    else:
        return targetname
