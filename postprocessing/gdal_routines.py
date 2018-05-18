
import os
import subprocess

import numpy as np

from geobox import geobox

def crop(geotiff_fname, bbox, clean=True):
    """Crop GeoTiff to boundingbox.

    Arguments:
        geotiff_fname: GeoTiff filename
        bbox: A shapely box.

    Output: Writes a new GeoTiff; if clean, deletes the input file.

    Returns: New GeoTiff filename.
    """
    tags = ('_bbox{:.4f}_{:.4f}_{:.4f}_{:.4f}'.format(*bbox.bounds))
    outname = geotiff_fname.split('.tif')[0] + tags + '.tif'
    bounds = geobox.shapely_to_gdal_box(bbox)
    commands = [
        'gdal_translate',
        '-projwin_srs', 'EPSG:4326',
        '-projwin', *[str(b) for b in bounds],
        geotiff_fname, outname
    ]
    subprocess.call(commands)
    if clean:
        os.remove(geotiff_fname)
    return outname

def reband(geotiff_fname, output_bands, clean=True):
    """Write a new GeoTiff with only selected output_bands.

    Arguments:
        geotiff_fname: GeoTiff filename
        output_bands: A list of bands by number (indexed from 1) 

    Output: Writes an LZW-compressed GeoTiff; if clean, deletes the input file.

    Returns: New GeoTiff filename
    """
    outname = geotiff_fname.split('.tif')[0] + 'LZW.tif'
    dressed_bands = np.asarray([('-b', str(b)) for b in output_bands])
    commands = [
        'gdal_translate',
        '-co', 'COMPRESS=LZW',
        *dressed_bands.flatten(),
        geotiff_fname, outname
    ]
    subprocess.call(commands)
    if clean:
        os.remove(geotiff_fname)
    return outname

    
