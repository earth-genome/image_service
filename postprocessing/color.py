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
> corrected = cc.correct(image)

Or if color will be corrected later by hand:
> cc = ColorCorrect()
> corrected = cc.brightness_and_contrast(image)

Argument image:  numpy array of type 'uint8' or 'float32', with assumed
    standard maximum values for images (255 for 'uint8', 1.0 for 'float32')

Notes:

These routines were developed through experience of adjusting Planet and
Landsat imagery by hand, using GIMP color curves. Scaling bright / dark points directly to percentiles on the image histogram leads to either (a) adjustments that tend toward posterization; or (b) adjustments based on histogram tails.  Therefore, the strategy is to find the (by default) (1, 99) percentiles as initial reference points and then step back by factor cut_frac.  I.e. instead of scaling the 1st percentile of the histogram to black, we find the pixel value of the first percentile, mutiply that value by cut_frac, and rescale the resulting pixel value to black. For one thing, this prevents over-saturating the image if there are no true blacks or whites in the scene.  

To balance colors, it is even more important to operate with reference to the meat of the histogram, not out at the tail. Suppose red is relatively dark as compared to blue, but red has a longer bright tail.  A modest cut at the 99.9th percentile may end up brightening blue more than red.  Therefore, by default, we take the 95th percentile as initial reference and again step back by cut_frac.

While _expand_histogram and _adjust_contrast can be reversed, roughly, by inverting the gamma transform and recompressing the histogram, the relative color blance cannot be reversed without knowing the initial color balance in the image, and therefore _balance_color should not be applied if color will be corrected later by hand.  This distinction is expressed in the difference between the correct() and brightness_and_contrast() methods.

"""
import numpy as np

import matplotlib.pyplot as plt
from skimage import color
from skimage import exposure

class ColorCorrect(object):
    """Perform basic color correction on an image.

    Attributes:
        percentiles: high/low histogram reference points for determining
            cutoffs; default: (1, 99)
        color_percentile: high reference point for an individual color band;
            default: 95
        cut_frac: factor by which to shift reference pixel values before
            applying cutoffs (see notes above); default: .75
        gamma: gamma correction exponent; default: .75

    External methods:
        brightness_and_contrast: Rescale intensities and enhance contrast.
        correct: Rescale intensities, enhance contrast, and balance colors.

    """
        
    def __init__(self,
                 percentiles=(1,99),
                 color_percentile=95,
                 cut_frac=.75,
                 gamma=.75):
        self.percentiles = percentiles
        self.color_percentile = color_percentile
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
            highcut = np.percentile(band[np.where(band > 0)],
                                    self.color_percentile)
            highcut = self._renorm_highcut(highcut, img_max)
            img.T[n] = exposure.rescale_intensity(band, in_range=(0, highcut))
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
        """Determine the maximum allowed pixel value for image."""
        if img.dtype == 'uint8':
            img_max = 255.
        elif img.dtype == 'float32':
            img_max = 1.0
        else:
            raise TypeError("Image dtype 'uint8' or 'float32' expected.")
        return img_max
