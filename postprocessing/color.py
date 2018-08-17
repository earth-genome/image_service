"""Functions for automated basic color correction on an image.

The three basic correction processes are embedded in the internal methods 
    _expand_histogram: Linearly rescale histogram, so reference values
        end at white and black.
    _gamma_transform: Peform a nonlinear stretching of the image histogram.
    _balance_color: Linearly rescale histogram for each color channel
        separately.

These functions are packaged into:

Class ColorCorrect:  Perform basic color correction on an image.

Usage with default parameters:
> cc = ColorCorrect()
> corrected = cc.correct(image)  # returns uint8

OR

> cc = ColorCorrect(return_ubyte=False)
> corrected = cc.correct(image)  # returns image with input dtype

Or if color will be corrected later by hand:
> cc = ColorCorrect()
> corrected = cc.enhance_contrast(image)

See STYLES, bottom, for instantiations tuned to produce different effects.

Argument image:  numpy array of type uint16, uint8, or float32, with assumed
    standard maximum values for images (65535, 255, or 1.0 respectively) 

Additional external function:
    coarse_adjust:  Convert to uint16 and do a rough histogram expansion.
        (Ad hoc to DG to prepare DG geotiffs for ColorCorrect routines.)

Usage from main, for Planet, yielding all possible STYLES.  
> python color.py planetimg.tif

Usage from main, for DG (-c flag coarse-corrects and ensures proper dtype)
> python color.py dgimg.tif -c 
        
Notes:

These routines were developed through experience of adjusting Planet, DigitalGlobe, and Landsat imagery by hand, using GIMP color curves. Scaling bright / dark points directly to percentiles on the image histogram leads to either (a) adjustments that tend toward posterization; or (b) adjustments based on histogram tails.  Therefore, the strategy is to find the (by default) (1, 99) percentiles as initial reference points and then step back by factor cut_frac.  I.e. instead of scaling the 1st percentile of the histogram to black, we find the pixel value of the first percentile, mutiply that value by cut_frac, and rescale the resulting pixel value to black. For one thing, this prevents over-saturating the image if there are no true blacks or whites in the scene.  

To balance colors, it is important to operate with reference to the meat of the histogram, not out at the tail. Suppose red is relatively dark as compared to blue, but red has a longer bright tail.  A modest cut at the 99.9th percentile may end up brightening blue more than red.  Therefore, by default, we take the 95th percentile as initial reference and again step back by cut_frac.

While _expand_histogram and _adjust_contrast can be inverted, roughly, by inverting the gamma transform and recompressing the histogram, the relative color blance cannot be reversed without knowing the initial color balance in the image, and therefore one may not want to apply _balance_color if color will be corrected later by hand.  This distinction is expressed in the difference between the correct() and brightness_and_contrast() methods.

Images that arrive with uint16 range are manipulated as such, to preserve
frequency resolution, with the option (via correct_and_reduce) to convert to
uint8 for a lossy data compression at the end.  If conversion to uint8
happens before histogram expansion, in particular, the image can be effectively posterized with only O(10) distinct values in a band.

Given the above, the percentiles should be fixed (except for special off-label use cases, cf. dra below). The tuneable parameters are the cut_frac and gamma.  Some reasonable examples are given in STYLES below.  For the same numerical variation from these values, cut_frac has a larger impact on contrast than gamma. Larger cut_frac means higher contrast, and typically should be set off with higher gamma (decreasing the gamma effect), and vice-versa.   

"""
import sys

import numpy as np
import skimage
import skimage.io
from skimage import exposure


# For ColorCorrect class, a method decorator to return uint8 images,
# activated by class attribute return_ubyte (True/False)

def reduce_to_ubyte(fn):
    """Decorate function to return unit8 images."""
    def reduced(self, img):
        img = fn(self, img)
        if self.return_ubyte:
            return skimage.img_as_ubyte(img)
        else:
            return img
    return reduced

class ColorCorrect(object):
    """Perform basic color correction on an image.

    Attributes:
        cut_frac: factor by which to shift reference pixel values before
            applying cutoffs (see notes above)
        gamma: gamma correction exponent
        percentiles: high/low histogram reference points for determining
            cutoffs
        color_percentile: high reference point for an individual color band
        return_ubyte: Bool.  If True, external methods return uint8 images.
        
    External methods:
        correct: Enhance contrast and balance colors.
        enhance_contrast:  Enhance contrast.
        linearly_enhance_contrast: Perform linear-only contrast stretching.
        dra: Perform a band-wise linear histogram rescaling.
        mincolor_correct: Enhance contrast and perform minimal (blue dark
            point) color correction.
    """
        
    def __init__(self,
                 cut_frac=.75,
                 gamma=.75,
                 percentiles=(1,99),
                 color_percentiles=(5,95),
                 return_ubyte=True):
        self.cut_frac = cut_frac
        self.gamma = gamma
        self.percentiles = percentiles
        self.color_percentiles = color_percentiles
        self.return_ubyte = return_ubyte

    @reduce_to_ubyte
    def correct(self, img):
        """Enhance contrast and balance colors."""
        img = self._expand_histogram(img)
        img = self._gamma_transform(img)
        img = self._balance_colors(img)
        return img

    @reduce_to_ubyte
    def enhance_contrast(self, img):
        """Enhance contrast."""
        img = self._expand_histogram(img)
        img = self._gamma_transform(img)
        return img
    
    @reduce_to_ubyte
    def linearly_enhance_contrast(self, img):
        """Perform linear-only contrast stretching."""
        return self._expand_histogram(img)

    @reduce_to_ubyte
    def dra(self, img):
        """Perform a band-wise linear histogram rescaling.

        This routine reproduces DG DRA when:
            self.cut_frac = 1
            self.color_percentiles = (2,98)
            
        """
        return self._balance_colors(img)

    @reduce_to_ubyte
    def mincolor_correct(self, img):
        """Enhance contrast and perform minimal (blue dark point) color
            correction.
        """
        img = self.enhance_contrast(img)
        img = self._shift_blue_darkpoint(img)
        return img

    def _expand_histogram(self, img):
        """Linearly rescale histogram."""
        lowcut, highcut = np.percentile(img[np.where(img > 0)],
                                        self.percentiles)
        highcut = self._renorm_highcut(highcut, img.dtype)
        lowcut = self._renorm_lowcut(lowcut)
        expanded = exposure.rescale_intensity(img, in_range=(lowcut, highcut))
        return expanded
    
    def _gamma_transform(self, img):
        """Peform gamma adjustment."""
        return exposure.adjust_gamma(img, gamma=self.gamma)

    def _balance_colors(self, img):
        """Linearly rescale histogram for each color channel separately."""
        balanced = np.zeros(img.shape, dtype=img.dtype)
        for n, band in enumerate(img.T):
            lowcut, highcut = np.percentile(band[np.where(band > 0)],
                                            self.color_percentiles)
            highcut = self._renorm_highcut(highcut, img.dtype)
            lowcut = self._renorm_lowcut(lowcut)
            balanced.T[n] = exposure.rescale_intensity(
                band, in_range=(lowcut, highcut))
        return balanced

    def _shift_blue_darkpoint(self, img, lowcut_frac=.5):
        """Shift the blue dark reference value only.

        This is a minimal color correction to remove atmospheric scattering,
            for scenes whose colors naturally are imbalanced (e.g. desert).
        """
        balanced = np.zeros(img.shape, dtype=img.dtype)
        balanced.T[:2] = img.T[:2]
        blue = img.T[2]
        lowcut, _ = np.percentile(blue[np.where(blue > 0)],
                                  self.color_percentiles)
        lowcut *= lowcut_frac
        balanced.T[2] = exposure.rescale_intensity(
            blue, in_range=(lowcut, self._get_max(img.dtype)))
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


# instantiations tuned to produce various output styles:

STYLES = {
    # default:
    'contrast': ColorCorrect(cut_frac=.75, gamma=.75).correct,
    # low contrast:
    'matte': ColorCorrect(cut_frac=.65, gamma=.6).correct,
    # reproduces DG DRA; oversaturated, very high contrast:
    'dra': ColorCorrect(cut_frac=1, color_percentiles=(2,98)).dra,
    # may perform well on scenes whose colors naturally are imbalanced:
    'desert': ColorCorrect(cut_frac=.95).mincolor_correct,
    # for another take on Planet 'visual':
    'expanded': ColorCorrect(cut_frac=.75).linearly_enhance_contrast
}

if __name__ == '__main__':
    usage_msg = ('Usage: python color.py image.tif [-c]\n' +
                 'The -c flag indicates a preliminary coarse adjust for ' +
                 'DigitalGlobe GeoTiffs.  The flag must follow the filename.')
    try:
        filename = sys.argv[1]
        img = skimage.io.imread(sys.argv[1])
    except (IndexError, FileNotFoundError) as e:
        sys.exit('{}\n{}'.format(repr(e), usage_msg))
    if '-c' in sys.argv:
        img = coarse_adjust(img)
    for style, operator in STYLES.items():
    #for style, operator in {'desert': STYLES['desert']}.items():
        corrected = operator(img)
        skimage.io.imsave(filename.split('.')[0] + style + '.png', corrected)

