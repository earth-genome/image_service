
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

# Calls to gdalwarp are expensive, so combine crop and reband into one call.
# In limited testing, this halves runtime as compared to factored
# crop and reband functions. 

def crop_and_reband(filename, bbox, output_bands, clean=True):
    """Crop GeoTiff to bounding box and return a GeoTiff in output_bands.

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
    if len(output_bands) == 3:
        commands += ['-co', 'photometric=RGB']
    subprocess.call(commands)
    if clean:
        os.remove(filename)
    return targetname

# For reasons I don't understand, if GeoTiffs are rebanded, and sometimes
# (with Planet PSOrthoTile assets, but not PSScene3Band or REOrthoTile)
# for cropped-only GeoTiffs, gdalwarp does not correctly identify
# 0 as the nodata value, which gdal_translate inputs during crop.
# At some risk of reducing the general applicability of the merge routine,
# this can be handled by specifying srcnodata explicitly:

def merge(filenames, srcnodata=0, clean=True):
    """Merge input GeoTiffs.

    Arguments:
        filenames: GeoTiff filenames
        srcnodata: 
        clean: True/False to delete the input file. 

    Output: Writes a GeoTiff

    Returns: New GeoTiff filename (based on the first input GeoTiff name) 
    """
    targetname = filenames[0].split('.tif')[0] + '-merged.tif'
    commands = [
        'gdalwarp',
        '--config', 'GDAL_CACHEMAX', '1000', '-wm', '1000',
        '-r', 'bilinear',
        '-srcnodata', str(srcnodata),
        '-co', 'COMPRESS=LZW',
        *filenames, targetname]
    subprocess.call(commands)
    if clean:
        for filename in filenames:
            os.remove(filename)
    return targetname

# Separate crop and reband routines:
 
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

def reband(filename, output_bands, clean=True):
    """Return GeoTiff with output_bands.

    Arguments:
        filename: GeoTiff filename
        output_bands: A list of bands by number (indexed from 1)
        clean: True/False to delete the input file. 

    Output: Writes a GeoTiff.

    Returns: New GeoTiff filename.
    """
    targetname = filename.split('.tif')[0] + '-reband.tif'
    dressed_bands = np.asarray([('-b', str(b)) for b in output_bands])
    commands = [
        'gdal_translate',
        *dressed_bands.flatten(),
        '-co', 'COMPRESS=LZW',
        '--config', 'GDAL_PAM_ENABLED', 'NO',
        filename, targetname
    ]
    if len(output_bands) == 3:
        commands += ['-co', 'photometric=RGB']
    subprocess.call(commands)
    if clean:
        os.rename(targetname, filename)
        return filename
    else:
        return targetname
