"""Tools for bulk download of Sentinel-2 imagery from its s3 bucket.

Depends on installed command line tools: 
   aws cli, and optionally, a configured AWS profile; and
   rio (rasterio).

External functions: download, jp2_to_geotiff, mask_merge_cog
"""

import os
import subprocess

import _env
import cog
from georeferencing import mask

AWS_GRAB = 'aws s3 cp s3://sentinel-s2-{level}/tiles/{utm_zone}/{lat_band}/{grid_square}/{year}/{month}/{day}/0/R10m/TCI.jp2 {outpath} --request-payer'

def download(sentinel_level, zones, dates, redownload=False,
             dest_dir=os.path.join(_env.base_dir, 'tmp'), aws_profile=None):
    """Download Sentinel-2 TCI imagery.

    Arguments: 
        sentinel_level: 'l1c' or 'l2a'
        zones: list of UTM grid zones of form '19NLJ'
        dates: list of sensing dates in isoformat, 'YYYYMMDD'
        redownload: bool: Force redownload of image even if path exists.
        aws_profile: An AWS profile for a logged-in user
    """
    outpaths = []
    for date in dates:
        payload = {
            'level': sentinel_level.lower(),
            'year': date[:4],
            'month': date[4:6].lstrip('0'),
            'day': date[6:].lstrip('0')
        }
        for zone in zones:
            outpath = os.path.join(dest_dir, '{}{}TCI_{}{}.jp2'.format(
                sentinel_level, '-'.join(dates), zone, date))
            payload.update({
                'utm_zone': zone[:2],
                'lat_band': zone[2:3],
                'grid_square': zone[3:]
            })
            if not os.path.exists(outpath) or redownload:
                commands = AWS_GRAB.format(outpath=outpath, **payload).split()
                if aws_profile:
                    commands += ['--profile', aws_profile]
                subprocess.call(commands)
            outpaths.append(outpath)
    return outpaths

def jp2_to_geotiff(jp2, tile_size=None, overwrite=False, clean=False):
    """Convert a JPEG2000 into a tiled GeoTiff.

    Arguments: 
        jp2: Path to a jp2 file
        tile_size: Tile size (e.g. 256 or 512) if geotiff is to be tiled;
            if None, a striped geotiff will be written instead
        overwrite: bool: To replace an existing .tif file with the same 
            prefix as the jp2.

    Returns: Path to the geotiff
    """
    geotiff = jp2.split('.jp2')[0] + '.tif'
    if tile_size:
        commands = ('rio convert --co tiled=yes '
                    '--co BLOCKXSIZE={} --co BLOCKYSIZE={} '
                    '{} {}'.format(tile_size, tile_size, jp2, geotiff)).split()
    else:
        commands = ('rio convert {} {}'.format(jp2, geotiff)).split()
    if os.path.exists(geotiff):
        if overwrite:
            os.remove(geotiff)
        else:
            return geotiff
    subprocess.call(commands)
    if clean:
        os.remove(jp2)
    return geotiff

def mask_merge_cog(jp2s, tile_size=512, srcnodata=0, geojson_mask=None,
                   clean=False, **kwargs):
    """Mask, merge, and cog a list of Sentinel jp2s.

    Arguments:
        jp2s: List of paths to jp2 files
        tile_size: Intermediate geotiff tile_size
        srcnodata: An override nodata value for the source imagery
        geojson_mask: Path to a GeoJSON to apply as mask 
        clean: bool: To delete input and intermediate files after processing
        Optional **kwargs to pass to cog.build_local

    Returns: Path to a COG
    """
    geotiffs = [jp2_to_geotiff(jp2, tile_size=tile_size, clean=clean)
                    for jp2 in jp2s]
    if geojson_mask:
        geotiffs = [mask.mask(g, geojson_mask, nodata=srcnodata)
                        for g in geotiffs]
    cogged = cog.build_local(geotiffs, srcnodata=srcnodata, clean=clean,
                             tile_size=tile_size, **kwargs)
    return cogged
