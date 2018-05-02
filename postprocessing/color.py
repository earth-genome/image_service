"""Functions for automated basic color correction on an image.

The three basic correction processes are embedded in the internal methods 
    _expand_histogram: Expand histogram reference values to white and black.
    _adjust_contrast: Peform gamma adjustment.
    _balance_color: Shift reference value to max brightness for each color
        channel.

These functions are packaged into:

Class ColorCorrect:  Perform basic color correction on an image.

Usage with default parameters:
> cc = ColorCorrect()
> corrected = cc.correct_and_reduce(image)  (returns uint8)

OR

> corrected = cc.correct(image) (returns image with input dtype)

Or if color will be corrected later by hand:
> cc = ColorCorrect()
> corrected = cc.brightness_and_contrast(image)

Argument image:  numpy array of type uint16, uint8, or float32, with assumed
    standard maximum values for images (65535, 255, or 1.0 respectively)

Additional external function:
    dg_coarse_adjust:  Convert to uint16 and do a rough histogram expansion.
        (Ad hoc to DG to prepare DG geotiffs for correct() routines.) 

Certain pre-set collections of tuneable parameters are given in STYLE_PARAMS.
        
Notes:

These routines were developed through experience of adjusting Planet and
Landsat imagery by hand, using GIMP color curves. Scaling bright / dark points directly to percentiles on the image histogram leads to either (a) adjustments that tend toward posterization; or (b) adjustments based on histogram tails.  Therefore, the strategy is to find the (by default) (1, 99) percentiles as initial reference points and then step back by factor cut_frac.  I.e. instead of scaling the 1st percentile of the histogram to black, we find the pixel value of the first percentile, mutiply that value by cut_frac, and rescale the resulting pixel value to black. For one thing, this prevents over-saturating the image if there are no true blacks or whites in the scene.  

To balance colors, it is even more important to operate with reference to the meat of the histogram, not out at the tail. Suppose red is relatively dark as compared to blue, but red has a longer bright tail.  A modest cut at the 99.9th percentile may end up brightening blue more than red.  Therefore, by default, we take the 95th percentile as initial reference and again step back by cut_frac.

While _expand_histogram and _adjust_contrast can be reversed, roughly, by inverting the gamma transform and recompressing the histogram, the relative color blance cannot be reversed without knowing the initial color balance in the image, and therefore _balance_color should not be applied if color will be corrected later by hand.  This distinction is expressed in the difference between the correct() and brightness_and_contrast() methods.

Images that arrive with uint16 range are manipulated as such, to preserve
frequency resolution, with the option (via correct_and_reduce) to convert to
uint8 for a lossy data compression at the end.  If conversion to uint8
happens before histogram expansion, in particular, the image can be effectively
posterized with only O(10) distinct values in a band.  

"""
import numpy as np

import matplotlib.pyplot as plt
import skimage
from skimage import exposure
import tifffile

STYLE_PARAMS = {
    'natural': {
        'percentiles': (1,99),
        'color_percentiles': (5,95),
        'cut_frac': .75,
        'gamma': .75
    },
    'gloss': {
        'percentiles': (1,99),
        'color_percentiles': (5,95),
        'cut_frac': .85,
        'gamma': .75
    }
}

class ColorCorrect(object):
    """Perform basic color correction on an image.

    Attributes:
        percentiles: high/low histogram reference points for determining
            cutoffs
        color_percentile: high reference point for an individual color band
        cut_frac: factor by which to shift reference pixel values before
            applying cutoffs (see notes above)
        gamma: gamma correction exponent

    External methods:
        brightness_and_contrast: Rescale intensities and enhance contrast.
        correct: Rescale intensities, enhance contrast, and balance colors.
        __call__: Runs correct() and then returns a uint8 array.

    """
        
    def __init__(self,
                 percentiles=(1,99),
                 color_percentiles=(5,95),
                 cut_frac=.75,
                 gamma=.75):
        self.percentiles = percentiles
        self.color_percentiles = color_percentiles
        self.cut_frac = cut_frac
        self.gamma = gamma
        
    def correct(self, img):
        """Rescale image intensities, enhance contrast, and balance colors."""
        print('Shifting white and black points.')
        img = self._expand_histogram(img)
        print('Adjusting contrast.')
        img = self._adjust_contrast(img)
        print('Equalizing color bright points.')
        img = self._balance_colors(img)
        return img

    def correct_and_reduce(self, img):
        """Rescale image intensities, enhance contrast, and balance colors.

        Returns: uint8 array
        """
        img = self.correct(img)
        return skimage.img_as_ubyte(img)

    def brightness_and_contrast(self, img):
        """Rescale image intensities and enhance contrast."""
        img = self._expand_histogram(img)
        img = self._adjust_contrast(img)
        return img

    def _expand_histogram(self, img):
        """Expand histogram reference values to white and black."""
        img_max = self._get_max(img)
        lowcut, highcut = np.percentile(img[np.where(img > 0)],
                                        self.percentiles)
        lowcut = self._renorm_lowcut(lowcut)
        highcut = self._renorm_highcut(highcut, img_max)
        expanded = exposure.rescale_intensity(img, in_range=(lowcut, highcut))
        return expanded

    def _adjust_contrast(self, img):
        """Peform gamma adjustment."""
        return exposure.adjust_gamma(img, gamma=self.gamma)

    def _balance_colors(self, img):
        """Shift reference value to max brightness for each color channel."""
        img_max = self._get_max(img)
        for n, band in enumerate(img.T):
            lowcut, highcut = np.percentile(band[np.where(band > 0)],
                                    self.color_percentiles)
            highcut = self._renorm_highcut(highcut, img_max)
            lowcut = self._renorm_lowcut(lowcut)
            img.T[n] = exposure.rescale_intensity(band,
                                                  in_range=(lowcut, highcut))
        return img
    
    def _renorm_lowcut(self, cut):
        """Shift cut toward zero."""
        return cut * self.cut_frac

    def _renorm_highcut(self, cut, img_max):
        """Shift cut toward img_max."""
        shifted = img_max - self.cut_frac * (img_max - cut)
        if img_max == 255.:
            shifted = round(shifted)
        return shifted

    def _get_max(self, img):
        """Determine the maximum allowed pixel value for standard image."""
        if img.dtype == 'uint16':
            img_max = 65535
        elif img.dtype == 'uint8':
            img_max = 255
        elif img.dtype == 'float32':
            img_max = 1.0
        else:
            raise TypeError('Expecting dtype uint16, uint8 or float32.')
        return img_max

    
def coarse_adjust(img, cut_frac=.9):
    """Convert to uint16 and do a rough histogram expansion.

    As of April 2018, DigitalGlobe does not have their geotiffs in order.
    The dtype kwarg to img.geotiff has no discernible effect. Sometimes
    images come as float32 with a uint16-like value range, sometimes as uint16.
    I have observed pixel values larger than 2**14, but not as of yet larger
    than 2**16, and generally the histogram is concentrated in the first
    twelve bits.  

    Returns: unit16 array
    """
    img = img.astype('uint16')
    cc = ColorCorrect(cut_frac=cut_frac)
    expanded = cc._expand_histogram(img)
    return expanded 
    
