
import sys

import matplotlib.pyplot as plt

import color_correct

if __name__ == '__main__':
    try:
        img_file = sys.argv[1]
    except KeyError:
        print('Usage: python reduce_planet_visual.py image.tif')
        sys.exit(0)
    img = plt.imread(img_file)
    cc = color_correct.ColorCorrect()
    corrected = cc.correct(img)
    prefix = img_file.split('-')[0]
    suffix = img_file.split('.')[-1]
    plt.imsave(prefix+'-cc.'+suffix, corrected)
