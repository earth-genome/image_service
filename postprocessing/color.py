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
    coarse_adjust:  Convert to uint16 and do a rough histogram expansion.
        (Ad hoc to DG to prepare DG geotiffs for ColorCorrect routines.)

Certain pre-set collections of tuneable parameters are given in STYLE_PARAMS.

Usage from main, for Planet:
> python color.py planetimg.tif

Usage from main, for DG (-c flag coarse corrects and ensures proper dtype)
> python color.py dgimg.tif -c 
        
Notes:

These routines were developed through experience of adjusting Planet, DigitalGlobe, and Landsat imagery by hand, using GIMP color curves. Scaling bright / dark points directly to percentiles on the image histogram leads to either (a) adjustments that tend toward posterization; or (b) adjustments based on histogram tails.  Therefore, the strategy is to find the (by default) (1, 99) percentiles as initial reference points and then step back by factor cut_frac.  I.e. instead of scaling the 1st percentile of the histogram to black, we find the pixel value of the first percentile, mutiply that value by cut_frac, and rescale the resulting pixel value to black. For one thing, this prevents over-saturating the image if there are no true blacks or whites in the scene.  

To balance colors, it is important to operate with reference to the meat of the histogram, not out at the tail. Suppose red is relatively dark as compared to blue, but red has a longer bright tail.  A modest cut at the 99.9th percentile may end up brightening blue more than red.  Therefore, by default, we take the 95th percentile as initial reference and again step back by cut_frac.

While _expand_histogram and _adjust_contrast can be inverted, roughly, by inverting the gamma transform and recompressing the histogram, the relative color blance cannot be reversed without knowing the initial color balance in the image, and therefore one may not want to apply _balance_color if color will be corrected later by hand.  This distinction is expressed in the difference between the correct() and brightness_and_contrast() methods.

Images that arrive with uint16 range are manipulated as such, to preserve
frequency resolution, with the option (via correct_and_reduce) to convert to
uint8 for a lossy data compression at the end.  If conversion to uint8
happens before histogram expansion, in particular, the image can be effectively
posterized with only O(10) distinct values in a band.

Given the above, the percentiles should be fixed and are written as such into the __init__ function.  The tuneable parameters are the cut_frac and gamma.  Some reasonable examples are given in STYLE_PARAMS below.  For the same numerical variation from these values, cut_frac has a larger impact on contrast than gamma. Larger cut_frac means higher contrast, and typically should be set off with higher gamma (decreasing the gamma effect), and vice-versa.   

"""
import numpy as np
import sys

import matplotlib.pyplot as plt
import skimage
from skimage import exposure
import tifffile

STYLE_PARAMS = {
    'matte': {
        'cut_frac': .65,
        'gamma': .6
    },
    'contrast': {
        'cut_frac': .75,
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
        
    def __init__(self, cut_frac=.75, gamma=.75):
        self.percentiles = (1, 99)
        self.color_percentiles = (5, 95)
        self.cut_frac = cut_frac
        self.gamma = gamma

    def correct(self, img):
        """Rescale image intensities, enhance contrast, and balance colors."""
        print('Shifting white and black points.')
        img = self._expand_histogram(img)
        print('Adjusting contrast.')
        img = self._adjust_contrast(img)
        print('Balancing color bright and dark points.')
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
        lowcut, highcut = np.percentile(img[np.where(img > 0)],
                                        self.percentiles)
        highcut = self._renorm_highcut(highcut, img.dtype)
        lowcut = self._renorm_lowcut(lowcut)
        expanded = exposure.rescale_intensity(img, in_range=(lowcut, highcut))
        return expanded
    
    def _adjust_contrast(self, img):
        """Peform gamma adjustment."""
        return exposure.adjust_gamma(img, gamma=self.gamma)

    def _balance_colors(self, img):
        """Shift reference value to max brightness for each color channel."""
        balanced = np.zeros(img.shape, dtype=img.dtype)
        for n, band in enumerate(img.T):
            lowcut, highcut = np.percentile(band[np.where(band > 0)],
                                            self.color_percentiles)
            highcut = self._renorm_highcut(highcut, img.dtype)
            lowcut = self._renorm_lowcut(lowcut)
            balanced.T[n] = exposure.rescale_intensity(
                band, in_range=(lowcut, highcut))
        return balanced
    
    def _renorm_lowcut(self, cut):
        """Shift cut toward zero."""
        return cut * self.cut_frac

    def _renorm_highcut(self, cut, datatype):
        """Shift cut toward img_max."""
        img_max = self._get_max(datatype)
        shifted = img_max - self.cut_frac * (img_max - cut)
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

    
def coarse_adjust(img):
    """Convert to uint16 and do a rough bandwise histogram expansion.

    As of April 2018, the dtype kwarg to DG img.geotiff method has
    no discernible effect. Sometimes images come as float32 with a
    uint16-like value range, sometimes as uint16. I have observed pixel
    values larger than 2**14, but not as of yet larger than 2**16,
    and generally the histogram is concentrated in the first
    twelve bits. Relative R-G-B weightings are not reproduced consistently
    from image to image of the same scene.

    This function does a rough color correction and histogram expansion,
    finding the pixel value for the 99th percentile for each band and
    resetting that to 1e4.  It allows ColorCorrect() to be applied to
    DigitalGlobe images with the same parameters as for Planet.

    Returns: unit16 array
    """
    percentile = 99
    target_value = 1e4
    coarsed = np.zeros(img.shape, dtype='uint16')
    for n, band in enumerate(img.T):
        cut = np.percentile(band[np.where(band > 0)], percentile)
        coarsed.T[n] = ((band / cut) * target_value).astype('uint16')
    return coarsed 


if __name__ == '__main__':
    usage_msg = ('Usage: python color.py image.tif [-c]\n' +
                 'The -c flag indicates a preliminary coarse adjust for ' +
                 'DigitalGlobe GeoTiffs.  The flag must follow the filename.')
    try:
        filename = sys.argv[1]
        img = tifffile.imread(sys.argv[1])
    except (IndexError, FileNotFoundError) as e:
        sys.exit('{}\n{}'.format(repr(e), usage_msg))
    if '-c' in sys.argv:
        img = coarse_adjust(img)
    for style, params in STYLE_PARAMS.items():
        cc = ColorCorrect(**params)
        corrected = cc.correct_and_reduce(img)
        plt.imsave(filename.split('.')[0] + 'cf{}g{}.png'.format(
            params['cut_frac'], params['gamma']), corrected)
