"""Tools for bulk download of Sentinel-2 imagery from its s3 bucket.

In addition to packages in requirements.txt, this module depends on:
    A configured default AWS cli profile;
    Sen2Cor, with the L2A_Process linked to somewhere in the user $PATH.

External functions: 
    download, download_and_Sen2Cor, jp2_to_geotiff, mask_merge_cog
"""

import datetime
import os
import shutil
import subprocess

import sentinelhub

import _env
import cog
import mask

AWS_L2A_GRAB = 'aws s3 cp s3://sentinel-s2-{level}/tiles/{utm_zone}/{lat_band}/{grid_square}/{year}/{month}/{day}/{aws_idx}/R{resolution}m/{band}.jp2 {outpath} --request-payer'

AWS_L1C_GRAB = ''.join(AWS_L2A_GRAB.split('R{resolution}m/'))

DEST_DIR = os.path.join(_env.base_dir, 'tmp')
if not os.path.exists(DEST_DIR):
    os.mkdir(DEST_DIR)

def download(date, zones, level='l2a', aws_idx=0, resolution=10, band='TCI',
             redownload=False, dest_dir=DEST_DIR):
    """Download Sentinel-2 TCI imagery.

    Arguments: 
        date: sensing date in isoformat, 'YYYY-MM-DD'
        zones: list of UTM grid zones of form '19NLJ'
        level: Sentinel processing level, 'l1c' or 'l2a'
        aws_idx: The last number in the s3 file path for the relevant tile(s)
            (a version number). 
        resolution: 10, 20, or 60 (meters/pixel). Only affects l2a
            product. (l1c comes native-resolution only.)
        band: TCI (for RGB), B01, B02, ... B12, or B8A. For the l2a product,
            some bands are available only at lower resolutions.
        redownload: bool: Force redownload of image even if path exists.
        dest_dir: Path to directory to write images.

    Returns: List of paths to downloaded images.
    """
    outpaths = []
    payload = {'level': level.lower(), 'aws_idx': aws_idx,
                   'resolution': resolution, 'band': band.upper()}
    payload.update({k:v.lstrip('0') for k,v in
                        zip(['year', 'month', 'day'], date.split('-'))})
    for zone in zones:
        outpath = os.path.join(
            dest_dir, f'Sentinel_{level}{date}_{zone}_{band}.jp2')
        payload.update({
            'utm_zone': zone[:2],
            'lat_band': zone[2:3],
            'grid_square': zone[3:],
        })
        if not os.path.exists(outpath) or redownload:
            if payload['level'] == 'l2a':
                grab = AWS_L2A_GRAB.format(outpath=outpath, **payload)
            elif payload['level'] == 'l1c':
                grab = AWS_L1C_GRAB.format(outpath=outpath, **payload)
            subprocess.call(grab.split())
        outpaths.append(outpath)
    return outpaths

def download_and_Sen2Cor(date, zones, aws_idx=0, redownload=False,
                         TCI_only=False, dest_dir=DEST_DIR):
    """Download Sentinel-2 Level-1C imagery and process to Level-2A.

    Arguments: 
        date: sensing date in isoformat, 'YYYY-MM-DD'
        zones: list of UTM grid zones of form '19NLJ'
        aws_idx: A version number on the imagery and the last number in the s3
            file path. Typically it should be 0, but in at least one instance 
            Sen2Cor failed on version 0 but succeeded on version 1. 
        redownload: bool: Force redownload of image even if path exists.
        TCI_only: bool: If True, extract the 10m TCI (RGB) image and 
            delete all other processed files.
        dest_dir: Path to directory to write images.

    Returns: List of paths to downloaded images or image directories.
    """
    outpaths = []
    for zone in zones:
        try: 
            prod_id = sentinelhub.AwsTile(zone, date, aws_idx).get_product_id()
        except sentinelhub.DownloadFailedException as e:
            print(repr(e))
            continue
        
        zone_dir = os.path.join(dest_dir,
                                zone + datetime.datetime.now().isoformat())
        if not os.path.exists(zone_dir):
            os.mkdir(zone_dir)
        safepath = os.path.join(zone_dir, prod_id + '.SAFE')
        req = sentinelhub.AwsProductRequest(
            product_id=prod_id, tile_list=[zone], data_folder=zone_dir,
            safe_format=True)
        if not os.path.exists(safepath) or redownload:
            try:
                req.save_data()
            except sentinelhub.DownloadFailedException as e:
                print(repr(e))
                continue

        subprocess.call(['L2A_Process', safepath])
        if TCI_only:
            outpaths.append(_extract_10mTCI(date, zone, dest_dir, zone_dir))
            shutil.rmtree(zone_dir)
        else:
            outpaths.append(zone_dir)
    return outpaths

def _extract_10mTCI(date, zone, dest_dir, zone_dir):
    """Extract the 10m TCI JPEG2000 from the Level-2A SAFE directory.

    Returns: New path to the jp2 file.
    """
    outpath = ''
    for dirpath, _, files in os.walk(zone_dir):
        for f in files: 
            if 'TCI_10m.jp2' in f:
                outpath = os.path.join(
                    dest_dir,
                    'Sentinel_{}TCI{}_{}.jp2'.format('l2a', date, zone))
                os.rename(os.path.join(dirpath, f), outpath)
    return outpath
                
def jp2_to_geotiff(jp2, overwrite=False, clean=False):
    """Convert a JPEG2000 into a striped GeoTiff.

    Arguments: 
        jp2: Path to a jp2 file
        overwrite: bool: To replace an existing .tif file with the same 
            prefix as the jp2.
        clean: bool: To delete input and intermediate files after processing

    Returns: Path to the geotiff
    """
    geotiff = jp2.split('.jp2')[0] + '.tif'
    commands = 'rio convert {} {}'.format(jp2, geotiff).split()
    if os.path.exists(geotiff):
        if overwrite:
            os.remove(geotiff)
        else:
            return geotiff
    subprocess.call(commands)
    if clean:
        os.remove(jp2)
    return geotiff

def mask_merge_cog(jp2s, nodata=None, geojson_mask=None, clean=False, **kwargs):
    """Mask, merge, and cog a list of Sentinel jp2s.

    Arguments:
        jp2s: List of paths to jp2 files
        nodata: An override nodata value for the source imagery
        geojson_mask: Path to a GeoJSON to apply as mask 
        clean: bool: To delete input and intermediate files after processing
        Optional **kwargs to pass to mask.mask and cog.build_local

    Returns: Path to a COG
    """
    geotiffs = [jp2_to_geotiff(jp2, clean=clean) for jp2 in jp2s]
    if geojson_mask:
        masked = [mask.mask(g, geojson_mask, nodata=nodata, clean=clean,
                            **kwargs)
                      for g in geotiffs]
    else:
        masked = geotiffs
    cogged = cog.build_local(masked, nodata=nodata, clean=clean, **kwargs)
    return cogged
