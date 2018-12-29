"""Class to automate searching and downloadling from the DigitalGlobe catalog.

API ref: http://gbdxtools.readthedocs.io/en/latest/index.html

Class DGImageGrabber: Descendant of class grabber.ImageGrabber

Usage with default specs: 

> from utilities.geobox import geobox
> bbox = geobox.bbox_from_scale(37.77, -122.42, 1.0)
> g = DGImageGrabber()
> g(bbox)

Catalog and image specs have defaults set in default_specs.json, and can be 
overriden by passing **kwargs to DGImageGrabber. As of writing, the
DG-relevant default specs take form:
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
    "pansharp_scale": 2.5,  # in km; used by _patch_geometric_specs(),
        which sets pansharpen=True below this scale
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

Two parameters are determined within _patch_geometric_specs(self, bbox) and
added to self.specs during the image pull:

- pansharpen: True or False according to whether image is smaller or
larger than pansharp_scale.

- proj: In principle this could be any EPSG code, e.g. EPSG:4326, and can
be set as such by setting override_proj='EPSG:4326'. Generically, here, it
will be the Universal Transverse Mercator (UTM) projection appropriate for
the bbox.


"""

import asyncio

import dateutil
import numpy as np
import shapely
import gbdxtools  # bug in geo libraries.  import this *after* shapely
import rasterio # this too

import grabber 
from utilities.geobox import geobox
from utilities.geobox import projections

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
    
class DGImageGrabber(grabber.ImageGrabber):
    """Tool to pull DigitalGlobe imagery.

    External attributes and methods are defined in the parent ImageGrabber. 
    """
    
    def __init__(self, client=gbdxtools.catalog.Catalog(), **specs):
        super().__init__(client, **specs)
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
                   if k in self._keymap.keys()}
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
        self._patch_geometric_specs(bbox)
        scenes = []
        record = next(records, None)
        while record and len(scenes) < self.specs['N_images']:
            ID, date = record['identifier'], record['properties']['timestamp']
            overlap, frac_area = self._get_overlap(bbox, record)
            if not self._well_overlapped(frac_area, ID):
                continue
            print('Trying ID {}: {}'.format(ID, date))
            try:
                daskimg = gbdxtools.CatalogImage(ID, **self.specs)
                print('Retrieved ID {}'.format(ID))
            except Exception as e:
                print('Exception: {}'.format(e))
                continue

            record.update({'daskimg': daskimg.aoi(bbox=overlap.bounds)})
            scenes.append([record])
            
            if self.specs.get('skip_days'):
                record = self._fastforward(
                    records, dateutil.parser.parse(date).date())
            else: 
                record = next(records, None)

        print('Found {} images of {} requested.'.format(
            len(scenes), self.specs['N_images']), flush=True)
        return scenes

    def _patch_geometric_specs(self, bbox):
        """Determine pansharpening and geoprojection."""
        if self.specs['override_proj']:
            proj = self.specs['override_proj']
        else:
            epsg = projections.get_utm_code(bbox.centroid.y, bbox.centroid.x)
            proj = 'EPSG:{}'.format(epsg)
            
        pansharpen = self._check_highres(bbox)
        self.specs.update({'proj': proj, 'pansharpen': pansharpen})

    def _check_highres(self, bbox):
        """Allow highest resolution when bbox smaller than pansharp_scale."""
        size = np.mean(geobox.get_side_distances(bbox))
        return True if size < self.specs['pansharp_scale'] else False
    
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
        daskimg.geotiff(path=path, bands=bands, **self.specs)
        path = expand_histogram(path)
        return [path]

    def _build_filename(self, bbox, record):
        """Build a filename for image output."""
        tags = ('bbox{:.4f}_{:.4f}_{:.4f}_{:.4f}'.format(*bbox.bounds))
        filename = (self.specs['file_header'] + record['identifier'] + '_' +
                    record['properties']['timestamp'] + tags + '.tif')
        return filename


# Function to regularize raw DG GeoTiffs

def expand_histogram(geotiff, percentile=97, target_value=8e3):
    """Convert to uint16 and do a rough bandwise histogram expansion.

    The dtype kwarg to DG img.geotiff method functions only for Worldview
    images. Across all sensors, allowing default dytpe, images come as 
    float32 with a uint16-like value range or as uint16. I have observed pixel
    values larger than 2**14, but not as of yet larger than 2**16,
    and generally the histogram is concentrated in the first
    twelve bits. Relative R-G-B weightings are not reproduced consistently
    from image to image of the same scene.

    This function does a rough histogram expansion, finding the pixel 
    value at the given percentile for each band and resetting that to the 
    target_value. This allows the routines in postprocessing.color to be 
    applied with the same parameters to both DG and Planet Labs images.

    Output: Overwrites an expanded, uint16 GeoTiff

    Returns: Path to the GeoTiff
    """
    with rasterio.open(geotiff) as dataset:
        profile = dataset.profile.copy()
        img = dataset.read()
        coarsed = np.zeros(img.shape, dtype='uint16')
        for n, band in enumerate(img):
            cut = np.percentile(band[np.where(band > 0)], percentile)
            coarsed[n] = ((band / cut) * target_value).astype('uint16')
            
    profile['dtype'] = 'uint16'
    profile['photometric'] = 'RGB'
    with rasterio.open(geotiff, 'w', **profile) as f:
        f.write(coarsed)
        
    return geotiff
