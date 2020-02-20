"""Process a collection of GeoTiffs into a Cloud-Optimized GeoTiff (COG).

External functions: build_local, merge, separate_bands, make_cog

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
          **kwargs):
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

    Returns: Path to the merged GeoTiff
    """
    nodata = _get_nodata(geotiffs) if nodata is None else nodata
    outpath = _get_lcss(geotiffs) + 'merged.tif'
    commands = (
        f'gdalwarp -overwrite --config GDAL_CACHEMAX {memorymax} ' 
        f'-multi -wo NUM_THREADS=ALL_CPUS -r bilinear ')
    if nodata is not None:
        commands += f'-srcnodata {nodata} '
    if tile_size:
        commands += (f'-co tiled=yes '
                     f'-co BLOCKXSIZE={tile_size} -co BLOCKYSIZE={tile_size}')
    commands = commands.split() + [*geotiffs, outpath]
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
    """Merge geotiffs with rio merge."""
    outpath = _get_lcss(geotiffs) + 'merged.tif'
    nodata = _get_nodata(geotiffs) if nodata is None else nodata
    commands = f'rio merge --overwrite --nodata {nodata}'
    commands = commands.split() + [*geotiffs, outpath]
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

def separate_bands(geotiff):
    """Break geotiff into its individual color bands."""
    with rasterio.open(geotiff) as f:
        count = f.profile.get('count', 0)
    bands = list(range(1, count + 1))

    outpaths = []
    for b in bands:
        bandpath = geotiff.split('.tif')[0] + f'_B0{b}.tif'
        commands = f'gdal_translate -b {b} {geotiff} {bandpath}'.split()
        subprocess.call(commands)
        outpaths.append(bandpath)
    return outpaths

def make_cog(geotiff, profile='jpeg', mask=True, webmap=True, clean=False,
             **kwargs):
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
    
    outpath = geotiff.split('.tif')[0] + '-cog.tif'
    commands = (f'rio cogeo create -p {profile} -r bilinear '
                f'--overview-resampling bilinear {geotiff} {outpath}').split()
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
    commands = f'gdal_translate -ot Byte {geotiff} {outpath}'.split()
    commands += ['-scale',
                 *[str(c) for c in cuts], *[str(v) for v in target_values]]
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
    commands = f'gdalinfo {geotiff} -hist -json'.split()
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
