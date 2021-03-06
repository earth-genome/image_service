"""Class to automate searching and downloadling from the DigitalGlobe catalog.

API ref: http://gbdxtools.readthedocs.io/en/latest/index.html

Class DGImageGrabber: Descendant of class base.ImageGrabber

Usage with default specs: 

> from geobox import geobox
> bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
> g = DGImageGrabber()
> g(bbox)

Output images will be uploaded to Google cloud storage and relevant urls 
returned. To save images locally, instantiate with 'bucket=None' and 
an optional directory:
> g = DGImageGrabber(bucket=None, staging_dir='my_dir')

Catalog and image specs have defaults set in default_specs.json, and
can be overriden by passing either specs_filename=alternate_specs.json
or **kwargs to DGImageGrabber. As of writing, the DG-relevant default specs 
take form:

{
    "clouds": 10,   # maximum allowed percentage cloud cover
    "min_intersect": 0.9,  # min fractional overlap between bbox and scene
    "startDate": "2008-09-01T00:00:00.0000Z",  # for catalog search
    "endDate": null,  # for catalog search
    "N_images": 1,
    "skip_days": 0, # min days between scenes if N_images > 1
    "write_styles": [
        "base",
        "vibrant"
    ],
    "landcover_indices": [],
    "thumbnails": false,
    "file_header": "",
    "offNadirAngle": null,   # (relation, angle), e.g. ('<', 10)
    "band_type": "MS",  # mulit-spectral
    "pansharpen": false, 
    "acomp": false,
    "override_proj": null, # any EPSG code, e.g. "EPSG:4326"; if null, a UTM
        projection is determined from the bbox
    "image_source": [
	    "WORLDVIEW02",
	    "WORLDVIEW03_VNIR",
	    "GEOEYE01"
    ]
}

The parameter image_source is from
['WORLDVIEW02', 'WORLDVIEW03_VNIR', 'GEOEYE01', 'QUICKBIRD02', 'IKONOS'].
The first three are are fairly comparable in resolution
(.3-.5 meters/pixel if pansharpened) and are currently active.
The latter two have resolution roughly half that and we decomissioned in 2015.

"""

import asyncio
import os

import dateutil
import numpy as np
import shapely
import gbdxtools  # Clash between Shapely/GEOS libraries. Import after shapely.
import rasterio   # This too. See Issue #13.
from rasterio.enums import ColorInterp

from grabbers import base 
from geobox import geobox
from geobox import projections

KNOWN_IMAGE_SOURCES = ['WORLDVIEW02', 'WORLDVIEW03_VNIR', 'GEOEYE01',
                      'QUICKBIRD02', 'IKONOS']

# DG band numbers for R-G-B-NIR bands. Keys are the total number of bands.
BANDMAP = {
    '4': [2, 1, 0, 3],
    '8': [4, 2, 1, 6]
}

# To standardize image records:
KEYMAP = {  
    'vendor': 'provider',
    'sensorPlatformName': 'sensor',
    'catalogID': 'catalogID',
    'timestamp': 'timestamp',
    'cloudCover': 'clouds',
    'panResolution': 'resolution',
    'browseURL': 'thumbnail',
    'geometry': 'footprintWkt'
}
    
class DGImageGrabber(base.ImageGrabber):
    """Tool to pull DigitalGlobe imagery.

    External attributes and methods are defined in the parent ImageGrabber. 
    """
    
    def __init__(self, client=None, **kwargs):
        if not client:
            client = gbdxtools.catalog.Catalog()
        super().__init__(client, **kwargs)
        self._enforce_date_format()
        self._search_filters = self._build_search_filters()
        self._bandmap = BANDMAP.copy()
        self._keymap = KEYMAP.copy()

    # Initializations to DG requirments:

    def _enforce_date_format(self):
        """Re-assign dates in DG-approved format.
    
        The required format is 'YYYY-MM-DDTHH:MM:SS.XXXXZ'.
        """
        for date in ('startDate', 'endDate'):
            if self.specs[date]: 
                parsed = dateutil.parser.parse(self.specs[date])
                formatted = parsed.isoformat(timespec='milliseconds')
                formatted = formatted.split('+')[0] + 'Z'
                self.specs[date] = formatted
                
    def _build_search_filters(self):
        """Build filters to search catalog."""
        sensors = ("(" + " OR ".join(["sensorPlatformName = '{}'".format(
            source) for source in self.specs['image_source']]) + ")")
        filters = [sensors]
        filters.append('cloudCover < {:d}'.format(int(self.specs['clouds'])))
        if self.specs['offNadirAngle']:
            filters.append('offNadirAngle {} {}'.format(
                self.specs['offNadirAngle']))
        return filters


    # Search and scene preparation.
    
    def _search(self, bbox):
        """Search the catalog for relevant imagery.

        Returns: An iterator over image records.
        """
        records = self.client.search(
            searchAreaWkt=bbox.wkt, filters=self._search_filters,
            startDate=self.specs['startDate'],
            endDate=self.specs['endDate'])
        records.sort(key=lambda r: r['properties']['timestamp'], reverse=True)
        print('Search found {} records.'.format(len(records)), flush=True) 
        return iter(records)

    def _search_id(self, catalogID, *args):
        """Retrieve record for input catalogID."""
        return self.client.get(catalogID)
            
    def _clean(self, record):
        """Streamline image record."""
        cleaned = {self._keymap[k]:v for k,v in record['properties'].items()
                   if k in self._keymap}
        return cleaned

    def _compile_scenes(self, records, bbox):
        """Retrieve dask images from the catalog.

        As Digital Globe tiles are large and sparse, we do not attempt to 
        gather tiles for mosaicking. Rather the challenge is that the API
        will refuse us many tiles we request. A scene here is a list 
        containing a single record, whose assets have been successfully 
        accessed from the DG API.

        Output: Cleaned records, including retrieved dask images.

        Returns: A list of lists, each containing one record.
        """
        self.specs.update({'proj': self._get_projection(bbox)})
        scenes = []
        record = next(records, None)
        while record and len(scenes) < self.specs['N_images']:
            ID, date = record['identifier'], record['properties']['timestamp']
            overlap, frac_area = self._get_overlap(bbox, record)
            if self._well_overlapped(frac_area, ID):
                print('Trying ID {}: {}'.format(ID, date))
                try:
                    daskimg = gbdxtools.CatalogImage(ID, **self.specs)
                    print('Retrieved ID {}'.format(ID))
                except Exception as e:
                    print('CatalogImage exception: {}'.format(e))
                    record = next(records, None)
                    continue

                record.update({'daskimg': daskimg.aoi(bbox=overlap.bounds)})
                scenes.append([record])
                if self.specs.get('skip_days'):
                    record = self._fastforward(
                        records, dateutil.parser.parse(date).date())
                    continue
            record = next(records, None)

        print('Found {} images of {} requested.'.format(
            len(scenes), self.specs['N_images']), flush=True)
        return scenes

    def _get_projection(self, bbox):
        """Determine a geoprojection."""
        if self.specs['override_proj']:
            return self.specs['override_proj']
        else:
            epsg = projections.get_utm_code(bbox.centroid.y, bbox.centroid.x)
            return 'EPSG:{}'.format(epsg)
    
    def _read_footprint(self, record):
        """Extract footprint in record as a shapely shape."""  
        return shapely.wkt.loads(record['properties']['footprintWkt'])

            
    # Scene download

    async def _download(self, scene, bbox):
        """Download scene assets.

        Output: GeoTiff written to disk

        Returns: List containing the path to the GeoTiff
        """
        record = next(iter(scene))
        daskimg = record['daskimg']
        bands = self._bandmap[str(daskimg.shape[0])]
        if not self.specs['landcover_indices']:
            bands = bands[:3]

        path = self._build_filename(bbox, record)
        print('\nStaging at {}\n'.format(path), flush=True)
        daskimg.geotiff(path=path, bands=bands, dtype='uint16', **self.specs)
        self._ensure_image_format(path)

        return [path]

    def _build_filename(self, bbox, record):
        """Compose an image filename."""
        tags = ('bbox{:.4f}_{:.4f}_{:.4f}_{:.4f}'.format(*bbox.bounds))
        filename = (self.specs['file_header'] + record['identifier'] + '_' +
                    record['properties']['timestamp'] + tags + '.tif')
        return filename
    
    def _ensure_image_format(self, path):
        """Enforce uint16 dtype and, for 3-band images, RGB photometric interp.

        The dtype kwarg to DG's daskimg.geotiff() method often or 
        always fails to yield uint16 images (ref Issue #9). And for GeoTIFF, 
        an RGB photometric interpretation (where appropriate) must be asserted
        when the file is created. This routine overwrites the input file
        to handle these two file formatting issues. (Factoring the two 
        operations would require reading and rewriting the file twice.)
        """
        with rasterio.open(path, 'r') as f:
            profile = f.profile
            img = f.read()

        profile.update({'dtype': 'uint16'})
        if profile['count'] == 3:
            profile.update({'photometric': 'RGB'})
        with rasterio.open(path, 'w', **profile) as f:
            f.write(img.astype('uint16'))
            
        
    # Reprocessing
    
    def _coloring(self, path):
        """Produce styles of visual images, with added histogram adjustment."""
        if self.specs['write_styles']:
            reg = self._regularize_histogram(path)
            output_paths = super()._coloring(reg)
            os.remove(reg)
        else:
            output_paths = []
        return output_paths
        
    def _regularize_histogram(self, geotiff, percentile=97, target_value=8e3):
        """Run a rough bandwise histogram expansion.

        This is a companion method to _ensure_image_format() to regularize
        some idiosyncracies of DG images. Relative R-G-B weightings are not 
        reproduced consistently from image to image of the same scene.

        This method does a rough histogram expansion, finding the pixel 
        value at the given percentile for each band and resetting that to the 
        target_value. It allows the routines in postprocessing.color to be 
        applied with the same parameters to both DG and Planet Labs images.

        Output: Writes to file a GeoTiff with histogram expanded

        Returns: Path to the GeoTiff
        """
        with rasterio.open(geotiff) as f:
            profile = f.profile
            img = f.read()
            
        coarsed = []
        for band in img:
            cut = np.percentile(band[np.where(band > 0)], percentile)
            coarsed.append((band / cut) * target_value)

        outfile = '.'.join(geotiff.split('.')[:-1]) + 'reg.tif'
        with rasterio.open(outfile, 'w', **profile) as f:
            f.write(np.array(coarsed, dtype=profile['dtype']))

        return outfile
