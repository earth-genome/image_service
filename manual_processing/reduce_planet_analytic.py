
import sys

import skimage.io

from postprocessing import color

if __name__ == '__main__':
    try:
        img_file = sys.argv[1]
    except KeyError:
        print('Usage: python reduce_planet_visual.py image.tif')
        sys.exit(0)
    img = skimage.io.imread(img_file)
    cc = color.ColorCorrect()
    corrected = cc.correct(img)
    prefix = img_file.split('-')[0]
    suffix = img_file.split('.')[-1]
    skimage.io.imsave(prefix+'-cc.'+suffix, corrected)
