"""Process a collection of GeoTiffs into a Cloud-Optimized GeoTiff (COG).

External functions: build_local, merge, make_cog

Base usgae, given a list of paths to geotiffs:
>>> build_local(geotiffs, **kwargs)

"""
import difflib
import itertools
import json
import subprocess
import os

import numpy as np
import rasterio

def build_local(geotiffs, **kwargs):
    """Build a Cloud-Optimized GeoTiff.

    Arguments:
        geotiffs: List of paths to GeoTiffs to merge into a COG.
        **kwargs: Optional kwargs to pass to merge and make_cog

    Returns: Path to COG
    """
    merged = merge(geotiffs, **kwargs)
    cogged = make_cog(merged, **kwargs)
    return cogged

def merge(geotiffs, nodata=None, memorymax=4e3, tile_size=512, clean=False,
          rio_merge=False, **kwargs):
    """Merge geotiffs with gdalwarp.

    Arguments:
        geotiffs: A list of local paths to GeoTiffs
        nodata: An override nodata value for the source imagery; 
            if None, the routine will attempt to read a common nodata value 
            from input GeoTiff headers.
        memorymax: Value to set gdalwarp -wm and --config GDAL_CACHEMAX options.
        tile_size: Blocksize for internal GeoTiff tiling; if None, 
            output GeoTiff will be striped instead
        clean: bool: To delete the input file after processing
        rio_merge: bool: Send work to alternate rio_merge() routine.

    Returns: Path to the merged GeoTiff
    """
    if rio_merge:
        return rio_merge(geotiffs, nodata=nodata, clean=clean)
    nodata = _get_nodata(geotiffs) if nodata is None else nodata
    outpath = _get_lcss(geotiffs) + 'merged.tif'
    commands = [
        'gdalwarp', '-overwrite',
        '--config', 'GDAL_CACHEMAX', str(memorymax), 
        '-multi', '-wo', 'NUM_THREADS=ALL_CPUS',
        '-r', 'bilinear',
        '-srcnodata', str(nodata),
        *geotiffs, outpath]
    if tile_size:
        commands += [
            '-co', 'tiled=yes',
            '-co', 'BLOCKXSIZE={}'.format(tile_size),
            '-co', 'BLOCKYSIZE={}'.format(tile_size)]
    subprocess.call(commands)
    if clean:
        for geotiff in geotiffs:
            os.remove(geotiff)
    return outpath

# Alternate merge routine. Can be better than 2x faster than gdalwarp
# on up to ~11GB of images (max size tested to completion on a 32GB system),
# but runs in memory and throws a MemoryError as size of output array
# approaches system memory.
def rio_merge(geotiffs, nodata=None, clean=False, **kwargs):
    outpath = _get_lcss(geotiffs) + 'merged.tif'
    nodata = _get_nodata(geotiffs) if nodata is None else nodata
    commands = ['rio', 'merge', '--overwrite', '--nodata', str(nodata),
                    *geotiffs, outpath]
    subprocess.call(commands)
    if clean:
        for geotiff in geotiffs:
            os.remove(geotiff)
    return outpath

def _get_nodata(geotiffs):
    """Extract a common nodata value from geotiffs.

    Raises: ValueError if input GeoTiffs have different nodata values.

    Returns: The common nodata value, or None if none are specified.
    """
    nodatas = []
    for geotiff in geotiffs:
        with rasterio.open(geotiff) as f:
            nodatas.append(f.profile.get('nodata', None))
    nodatas = np.array(nodatas)

    if np.all(nodatas == nodatas[0]):
        return nodatas[0]
    else:
        raise ValueError('Inconsistent nodata values: {}'.format(
            {k:v for k,v in zip(geotiffs, nodatas)}))

def _get_lcss(paths):
    """Find the largest common substring of a list of file paths."""
    path_iterator = iter(paths)
    path_a = next(path_iterator)
    for path_b in path_iterator:
        matcher = difflib.SequenceMatcher(a=path_a, b=path_b)
        match = matcher.find_longest_match(0, len(path_a), 0, len(path_b))
        path_a = path_a[match.a:match.size]
    return path_a

def make_cog(geotiff, profile='jpeg', fallback_profile='deflate',
             mask=True, webmap=True, clean=False, **kwargs):
    """Convert geotiff into a Cloud-Optimized GeoTiff.

    Arguments: 
        geotiff: Path to a georeferenced Tiff
        profile: A rio-cogeo profile
        mask: bool: To handle nodata values via a mask
        webmap: bool: The rio-cogeo web-optimized flag: Reprojects to 
            EPSG:3857 and aligns overviews to standard webmap tiles
        clean: bool: To delete the input file after processing
        **kwargs: Optional kwargs to pass to expand_histogram
        
    Returns: Path to the Cloud-Optimized GeoTiff.
    """
    if _format_is_gray16bit(geotiff):
        geotiff = expand_histogram(geotiff, clean=clean, **kwargs)
        profile = fallback_profile
    if profile == 'jpeg' and not _format_valid_for_jpeg(geotiff):
        print('Rio-cogeo JPEG takes a 3-band uint8 image. No COG written.')
        return
    
    if 'merged' in geotiff:
        outpath = geotiff.split('merged')[0] + '.tif'
    else:
        outpath = geotiff.split('.tif')[0] + '-cog.tif'
        
    commands = [
        'rio', 'cogeo', 'create',
        '-p', profile,
        '-r', 'bilinear', '--overview-resampling', 'bilinear',
        geotiff, outpath]
    if mask:
        commands += ['--add-mask']
    if webmap:
        commands += ['-w']
    subprocess.call(commands)

    print('Wrote {}\n'.format(outpath))
    if clean:
        os.remove(geotiff)
    return outpath

def _format_is_gray16bit(geotiff):
    """Check for a grayscale uint16 profile.

    Returns: bool
    """
    with rasterio.open(geotiff) as f:
        profile = f.profile
    return (profile['count'] == 1 and profile['dtype'] == 'uint16')

def _format_valid_for_jpeg(geotiff):
    """Check geotiff format compatibility to the rio-cogeo JPEG profile.

    Returns: bool
    """ 
    with rasterio.open(geotiff) as f:
        profile = f.profile
    return (profile['count'] == 3 and profile['dtype'] == 'uint8')

# Default percentiles and target_values are suggested for cloud-free images.
# For images with clouds, try target_values=(0,235).
def expand_histogram(geotiff, percentiles=(0,97), target_values=(0,144),
                     clean=False, **kwargs):
    """Expand a grayscale histogram and convert to uint8.

    Arguments:
        geotiff: Path to a GeoTiff
        percentiles: Tuple of minimum and maximum histogram percentiles 
        target_values: Tuple of uint8 pixel values at which to set 
            histogram percentiles
        clean: bool: To delete the input file after processing

    Returns: Path to the adjusted GeoTiff.
    """
    cuts = _get_histogram_cuts(geotiff, percentiles)
    outpath = geotiff.split('.tif')[0] + '-uint8.tif'
    commands = [
        'gdal_translate', '-ot', 'Byte', 
        '-scale', *[str(c) for c in cuts], *[str(v) for v in target_values],
        geotiff, outpath]
    subprocess.call(commands)

    if clean:
        os.remove(geotiff)
    return outpath

def _get_histogram_cuts(geotiff, percentiles):
    """Compute pixel values for given histogram percentiles."""
    hist = _extract_histogram(geotiff)
    min_bin, max_bin = _find_bin(hist['buckets'], percentiles)
    bin_size = (hist['max'] - hist['min'])/hist['count']
    offset = hist['min'] + bin_size/2
    
    min_cut = int(round(offset + min_bin*bin_size))
    max_cut = int(round(offset + max_bin*bin_size))
    return min_cut, max_cut

def _extract_histogram(geotiff):
    """Get geotiff histogram via gdalinfo."""
    commands = ['gdalinfo', geotiff, '-hist', '-json']
    stats = json.loads(subprocess.run(commands, stdout=subprocess.PIPE).stdout)
    band = next(iter(stats['bands']))
    
    auxfile = geotiff + '.aux.xml'
    if os.path.exists(auxfile):
        os.remove(auxfile)
    return band['histogram']

def _find_bin(bin_counts, percentiles):
    """Extract bin numbers for low, high percentiles of the histogram."""
    partial_sums = list(itertools.accumulate(bin_counts))
    partial_percents = 100*np.array(partial_sums)/np.sum(bin_counts)
    
    low_bin = np.where(partial_percents >= percentiles[0])[0][0]
    high_bin = np.where(partial_percents >= percentiles[1])[0][0]
    return low_bin, high_bin

def band_separate_cog(geotiff, profile='deflate', nodata=None, **kwargs):
    """Convert geotiff into Cloud-Optimized GeoTiffs, one per color band.

    Arguments: 
        geotiff: Path to a georeferenced Tiff
        profile: A rio-cogeo profile
        nodata: An override nodata value for the geotiff.
        **kwargs: Optional kwargs to pass to subsidiary processes
        
    Returns: Path to the Cloud-Optimized GeoTiff.
    """
    with rasterio.open(geotiff) as f:
        count = f.profile.get('count', 0)
    bands = list(range(1, count + 1))

    outpaths = []
    for b in bands:
        bandpath = geotiff.split('.tif')[0] + '_B0{}merged.tif'.format(b)
        commands = ['gdal_translate', '-b' , str(b), geotiff, bandpath]
        if nodata is not None:
            commands += ['-a_nodata', str(nodata)]
        subprocess.call(commands)
        outpaths.append(make_cog(bandpath, profile=profile, **kwargs))
    return outpaths
    
