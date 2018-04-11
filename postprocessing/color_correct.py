"""Functions for automated basic color correction on an image.

External functions:
    white_pt_black_pt: Rescale given percentiles of the image histogram
        to white and black.
    adjust_contrast: Peform gamma adjustment.
    color_bright_pts: For each color channel separately, rescale given
        percentile of histogram to maximum brightness.

These functions are packaged into the class:

Class ColorCorrect:  Perform basic color correction on an image.

Usage with default parameters:
> cc = ColorCorrect()
> corrected = cc.correct(image)

Or if color will be corrected later by hand:
> cc = ColorCorrect()
> corrected = cc.brightness_and_contrast(image)

Notes:

If black_pt_white_pt() is applied with default percentiles, histogram clipping
is minimal.  That and gamma adjustment are largely reversible.  These two operations are packaged into ColorCorrect.brightness_and_contrast(). On the other
hand, color_bright_pts() needs to be applied with a relatively aggressive
percentile (default: 98) so as not to be caught by a long outlying tail.
I.e. suppose red is relatively dark as compared to blue, but red has a longer
bright tail.  A modest cut at the 99.9th percentile may end up brightening
blue more than red.  Therefore, the action of color_bright_pts is not
reversible and should not be applied if color will be corrected later by hand.
The method ColorCorrect.correct() includes color_bright_pts().

"""
import numpy as np

import matplotlib.pyplot as plt
from skimage import color
from skimage import exposure

class ColorCorrect(object):
    """Perform basic color correction on an image.

    Attributes:
        percentiles: high/low percentile cutoffs for full histogram
        color_percentile: high cutoff for an individual color band
        gamma: gamma correction exponent

    Methods:
        brightness_and_contrast: Rescale intensities and enhance contrast.
        correct: Rescale intensities, enhance contrast, and balance colors.

    """
        
    def __init__(self,
                 percentiles=(.1, 99.9),
                 color_percentile=98,
                 gamma=.8):
        self.percentiles = percentiles
        self.color_percentile = color_percentile
        self.gamma = gamma

    def correct(self, img):
        """Rescale image intensities, enhance contrast, and balance colors."""
        img = white_pt_black_pt(img, self.white_black_percentiles)
        img = adjust_contrast(img, gamma=self.gamma)
        img = color_bright_pts(img, percentile=self.color_percentile)
        return img

    def brightness_and_contrast(self, img):
        """Rescale image intensities and enhance contrast."""
        img = white_pt_black_pt(img, self.percentiles)
        img = adjust_contrast(img, gamma=self.gamma)
        return img
    
def white_pt_black_pt(img, percentiles):
    """Rescale given percentiles of histogram to white and black.

    Arguments:
        img type: ndarray
        percentiles: tuple of floats, e.g. (.1, 99.9)

    Returns: ndarray
    """
    print('Rescaling white and black points.')
    lowcut, highcut = np.percentile(img[np.where(img > 0)], percentiles)
    return exposure.rescale_intensity(img, in_range=(lowcut, highcut))

def adjust_contrast(img, gamma):
    """Peform gamma adjustment.

    Arguments:
        img type: ndarray
        gamma type: float (gamma < 1 brightens; gamma > 1 darkens)

    Returns: ndarray
    """
    print('Adjusting contrast.')
    return exposure.adjust_gamma(img, gamma=gamma)

def color_bright_pts(img, percentile):
    """Rescale given percentile of histogram to max brightness for each
    color channel separately.

    Arguments:
        img type: ndarray
        percentile: float (e.g.: 98)

    Returns: ndarray
    """
    print('Equalizing color bright points.')
    for n, band in enumerate(img.T):
        highcut = np.percentile(band[np.where(band > 0)], percentile)
        img.T[n] = exposure.rescale_intensity(band, in_range=(0, highcut))
    return img
    
