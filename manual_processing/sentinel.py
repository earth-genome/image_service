"""Tools for bulk download of Sentinel-2 imagery from its s3 bucket.

In addition to packages in requirements.txt, this module depends on:
    A default configured AWS cli profile;
    Sen2Cor, aliased to a bash function Sen2Cor that takes a SAFE directory 
       as sole argument.

External functions: 
    download, download_and_Sen2Cor, jp2_to_geotiff, mask_merge_cog
"""

import os
import shutil
import subprocess

import sentinelhub

import _env
import cog
from georeferencing import mask

AWS_GRAB = 'aws s3 cp s3://sentinel-s2-{level}/tiles/{utm_zone}/{lat_band}/{grid_square}/{year}/{month}/{day}/{aws_idx}/R10m/TCI.jp2 {outpath} --request-payer'

DEST_DIR = os.path.join(_env.base_dir, 'tmp')
if not os.path.exists(DEST_DIR):
    os.mkdir(DEST_DIR)

def download(level, date, zones, aws_idx=0, redownload=False,
             dest_dir=DEST_DIR):
    """Download Sentinel-2 TCI imagery.

    Arguments: 
        level: Sentinel processing level, 'l1c' or 'l2a'
        date: sensing date in isoformat, 'YYYY-MM-DD'
        zones: list of UTM grid zones of form '19NLJ'
        aws_idx: The last number in the s3 file path for the relevant tile(s).
        redownload: bool: Force redownload of image even if path exists.
        dest_dir: Path to directory to write images.

    Returns: List of paths to downloaded images.
    """
    outpaths = []
    payload = {'level': level.lower(), 'aws_idx': aws_idx}
    payload.update({k:v.split('0') for k,v in
                        zip(['year', 'month', 'day'], date.split('-'))})
    for zone in zones:
        outpath = os.path.join(dest_dir, 'Sentinel_{}TCI{}_{}.jp2'.format(
            level, date, zone))
        payload.update({
            'utm_zone': zone[:2],
            'lat_band': zone[2:3],
            'grid_square': zone[3:],
        })
        if not os.path.exists(outpath) or redownload:
            commands = AWS_GRAB.format(outpath=outpath, **payload).split()
            subprocess.call(commands)
        outpaths.append(outpath)
    return outpaths

def download_and_Sen2Cor(date, zones, aws_idx=0, redownload=False, clean=False,
                         dest_dir=DEST_DIR):
    """Download Sentinel-2 Level-1C imagery and process to Level-2A.

    Arguments: 
        date: sensing date in isoformat, 'YYYY-MM-DD'
        zones: list of UTM grid zones of form '19NLJ'
        aws_idx: The last number in the s3 file path for the relevant tile(s).
        redownload: bool: Force redownload of image even if path exists.
        clean: bool: To delete input and intermediate files after processing
        dest_dir: Path to directory to write images.

    Returns: List of paths to downloaded images.
    """
    outpaths = []
    for zone in zones:
        prod_id = sentinelhub.AwsTile(zone, date, aws_idx).get_product_id()
        safepath = prod_id + '.SAFE'
        req = sentinelhub.AwsProductRequest(
            product_id=prod_id, tile_list=list(zone), data_folder=dest_dir,
            safe_format=True)
        if not os.path.exists(safepath) or redownload:
            req.save_data()
            
        subprocess.call(['Sen2Cor', safepath])
        outpaths.append(_extract_10mTCI(date, zone, dest_dir))
        if clean:
            shutil.rmtree(safepath)
    return outpaths

def _extract_10mTCI(date, zone, dest_dir):
    """Extract the 10m TCI JPEG2000 from the Level-2A SAFE directory.

    Returns: New path to the jp2 file.
    """
    
    for root, dirs, _ in os.walk(dest_dir):
        for d in dirs:
            if ''.join(date.split('-')) in d and zone in d and 'MSIL2A' in d:
                l2a_dir = os.path.join(root, d)
    outpath = os.path.join(dest_dir, 'Sentinel_{}TCI{}_{}.jp2'.format(
        'l2a', date, zone))
    for root, _, files in os.walk(l2a_dir):
        for f in files: 
            if 'TCI_10m.jp2' in f:
                os.rename(os.path.join(root, f), outpath)
    return outpath
                
def jp2_to_geotiff(jp2, tile_size=None, overwrite=False, clean=False):
    """Convert a JPEG2000 into a tiled GeoTiff.

    Arguments: 
        jp2: Path to a jp2 file
        tile_size: Tile size (e.g. 256 or 512) if geotiff is to be tiled;
            if None, a striped geotiff will be written instead
        overwrite: bool: To replace an existing .tif file with the same 
            prefix as the jp2.
        clean: bool: To delete input and intermediate files after processing

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
