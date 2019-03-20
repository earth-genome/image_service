"""Functions for automated color correction on an image.

Class ColorCorrect:  Perform basic color correction on an image.

Usage with predefined style 'base': 
> cc = ColorCorrect(style='base')
> corrected = cc(image_file)  

Use style 'base' but override saturation with a higher value:
> cc = ColorCorrect(style='base', saturation=1.5)
> corrected = cc(image_file) 

See STYLES, below, for predefined styles and possible override paramters. 

Argument image_file: 3-band uint16, uint8, or float image, with assumed
    standard maximum values for images (65535, 255, or 1.0 respectively) 
        
Notes:

These routines were developed through experience of adjusting Planet,
DigitalGlobe, and Landsat imagery by hand. The coarse_adjust produces
a minimally workable visual image from a Planet 'analytic' image or a
DG geotiff. The coarse operations are threefold (each with tuneable parameters):

- The histogram is expanded linearly to fill more of the available bit range.
- The histogram for each band is expanded linearly to rebalance colors.
- Dark points for green and blue bands are adjusted as additional compensation
  for atmospheric scatter.

Scaling bright / dark points directly to percentiles on the image
histogram either (a) tends towards posterization; or (b) bases adjustments
on histogram tails.  Therefore, the strategy is to find the (by
default) (5, 95) percentiles as initial reference points and then step
back by factor cut_frac.  I.e. instead of scaling the 5th percentile
of the histogram to black, we find the pixel value of the 5th
percentile, mutiply that value by cut_frac, and rescale the resulting
pixel value to black. For one thing, this prevents over-saturating the
image if there are no true blacks or whites in the scene.

To balance colors, it is important to operate with reference to the
meat of the histogram, not out at the tail. Suppose red is relatively
dark as compared to blue, but red has a longer bright tail.  A modest
cut at the 99.9th percentile may end up brightening blue more than
red.  Therefore, by default, we take the 5th, 95th percentile as initial
reference and again step back by cut_frac. 

Color tuning operations include: 

- Gamma transform (brightens midtones)
- Sigmoid transform (contrast enhancing and brightening)
- Saturation adjustment

"""
import copy
import os
import subprocess
import sys

import numpy as np
import rasterio
from skimage import exposure

# Parameters that apply no correction:
NULL_PARAMS = {
    'percentiles': (5,95),
    'cut_frac': 0,
    'atmos_cut_fracs': {'green': 0, 'blue': 0},
    'gamma': 1.0,
    'sigmoid': {'amount': 1, 'bias': .5},
    'saturation': 1.0
}

# Defaults for coarse correction:
COARSE_PARAMS = {
    'percentiles': (5,95),
    'cut_frac': .65,
    'atmos_cut_fracs': {'green': .3, 'blue': .45}
}

# Predefined styles:
STYLES = {
    'base': {
        'gamma': 1.3,
        'sigmoid': {'amount': 5, 'bias': .2},
        'saturation': 1.3,
        **COARSE_PARAMS
    },
    'vibrant': {
        'gamma': 1.3,
        'saturation': 1.5,
        'sigmoid': {'amount': 6, 'bias': .15},
        **COARSE_PARAMS
    },
    'landsat': {
        'percentiles': (5,95),
        'cut_frac': .4,
        'atmos_cut_fracs': {'green': .2, 'blue': .3},
        'gamma': 1.3,
        'sigmoid': {'amount': 1, 'bias': .5},
        'saturation': 1.2
    }
}
                 
class ColorCorrect(object):
    """Perform basic color correction on an image.

    Parameters for the color correction can be specified by providing
    one of the above predefined styles and/or optionally overriding 
    any specific parameter as a keyword argument to __init__. Parameters
    default to NULL_PARAMS (no correction).  

    Attributes:
        cores: Integer number of processor cores to use, or -1 for all.  
        style: One of the predefined style above or 'custom'
        **params: Optional override params.
        
    External methods:
        __call__: Run coarse and fine-tune color correction.
        coarse_adjust: Produce an image from raw analytic satellite data.
        tune: Tune colors.
    """
    def __init__(self, cores=-1, style='custom', **params):
        self.cores = cores
        self.style = style
        self.params = copy.deepcopy(STYLES.get(style, NULL_PARAMS))
        self.params.update(**params)

    def __call__(self, path):
        """Run coarse and fine-tune color correction."""
        if self._check_coarse():
            coarsed = self.coarse_adjust(path)
            tuned = self.tune(coarsed)
            os.remove(coarsed)
        else:
            tuned = self.tune(path)
        return tuned

    def _check_coarse(self):
        """Check for affirmative coarse correction parameters."""
        cut_fracs = [self.params.get('cut_frac'),
                     *(self.params.get('atmos_cut_fracs', {}).values())]
        return any(cut_fracs)

    def coarse_adjust(self, path):
        """Produce an image from raw analytic satellite data."""
        with rasterio.open(path) as f:
            profile = f.profile.copy()
            img = f.read()
        if not img.any():
            print('Warning: Image {} has all null values.'.format(path))
            return path
        
        img = self._expand_histogram(img)
        img = self._balance_colors(img)
        img = self._remove_atmos(img)

        profile.update({'photometric': 'RGB'})
        outpath = path.split('.tif')[0] + 'vis.tif'
        with rasterio.open(outpath, 'w', **profile) as f:
            f.write(img) 
        return outpath

    def tune(self, path):
        """Tune colors."""
        outpath = path.split('.tif')[0] + self.style + '.tif'
        commands = [
            'rio', 'color', '-j', str(self.cores),
            path, outpath,
            'gamma', 'RGB', str(self.params['gamma']),
            'sigmoidal', 'RGB', *[str(v) for v in
                                  self.params['sigmoid'].values()],
            'saturation', str(self.params['saturation'])
        ]
        subprocess.call(commands)

        # There seems to be a bug in the way rio color writes headers for
        # uint16 files. They can still be read by skimage and rasterio but
        # not Preview or Photoshop. Read/rewrite for a temporary fix:
        with rasterio.open(outpath) as f:
            img = f.read()
            profile = f.profile.copy()
        profile.update({'photometric': 'RGB'})
        with rasterio.open(outpath, 'w', **profile) as f:
            f.write(img)
                
        return outpath
    
    def _expand_histogram(self, img):
        """Linearly rescale histogram."""
        lowcut, highcut = np.percentile(img[np.where(img > 0)],
                                        self.params['percentiles'])
        highcut = self._renorm_highcut(highcut, img.dtype)
        lowcut = self._renorm_lowcut(lowcut)
        expanded = exposure.rescale_intensity(img, in_range=(lowcut, highcut))
        return expanded

    def _balance_colors(self, img):
        """Linearly rescale histogram for each color channel separately."""
        balanced = np.zeros(img.shape, dtype=img.dtype)
        for n, band in enumerate(img):
            lowcut, highcut = np.percentile(band[np.where(band > 0)],
                                            self.params['percentiles'])
            highcut = self._renorm_highcut(highcut, img.dtype)
            lowcut = self._renorm_lowcut(lowcut)
            balanced[n] = exposure.rescale_intensity(
                band, in_range=(lowcut, highcut))
        return balanced

    def _remove_atmos(self, img):
        """Shift blue and green dark points to compensate for atmospheric
            scattering.
        """
        cleaned = img.copy()
        green, blue = img[1:3]
        
        glowcut, _ = np.percentile(green[np.where(green > 0)],
                                   self.params['percentiles'])
        glowcut *= self.params['atmos_cut_fracs']['green']
        blowcut, _ = np.percentile(blue[np.where(blue > 0)],
                                   self.params['percentiles'])
        blowcut *= self.params['atmos_cut_fracs']['blue']
        
        cleaned[1] = exposure.rescale_intensity(
            green, in_range=(glowcut, self._get_max(img.dtype)))
        cleaned[2] = exposure.rescale_intensity(
            blue, in_range=(blowcut, self._get_max(img.dtype)))
        
        return cleaned

    def _renorm_lowcut(self, cut):
        """Shift cut toward zero."""
        return cut * self.params['cut_frac']

    def _renorm_highcut(self, cut, datatype):
        """Shift cut toward img_max."""
        img_max = self._get_max(datatype)
        shifted = img_max - self.params['cut_frac'] * (img_max - cut)
        return shifted

    def _get_max(self, datatype):
        """Determine the maximum allowed pixel value for standard image."""
        if datatype == 'uint16':
            img_max = 65535
        elif datatype == 'uint8':
            img_max = 255
        elif datatype == 'float32':
            img_max = 1.0
        else:
            raise TypeError('Expecting dtype uint16, uint8 or float32.')
        return img_max

if __name__ == '__main__':
    usage_msg = ('Usage: python color.py image.tif')
    try:
        filename = sys.argv[1]
    except (IndexError, FileNotFoundError) as e:
        sys.exit('{}\n{}'.format(repr(e), usage_msg))
    ColorCorrect(cores=1, style='base')(filename)


