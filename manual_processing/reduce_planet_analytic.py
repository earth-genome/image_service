"""Dated routine to color correct Planet imagery reduced via
reduce_planet_anlaytic.sh. Run from the parent folder (grab_imagery)
to pickup the postprocessing dependency. Probably needs updating. No
warraties implied.  

"""

import sys

import skimage.io

from postprocessing import color

if __name__ == '__main__':
    try:
        img_file = sys.argv[1]
    except KeyError:
        print('Usage: python manual reduce_planet_visual.py image.tif')
        sys.exit(0)
    img = skimage.io.imread(img_file)
    cc = color.ColorCorrect()
    corrected = cc.correct(img)
    prefix = img_file.split('-')[0]
    suffix = img_file.split('.')[-1]
    skimage.io.imsave(prefix+'-cc.'+suffix, corrected)
