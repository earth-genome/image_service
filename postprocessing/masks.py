
import sys

import skimage.io

# WIP.  Could be done out of memory with gdalwarp/
def ndwi(filename):
    img = skimage.io.imread(filename).astype('float32')
    b, g, r, nir = img.T
    ndwi = (g - nir)/(g + nir)
    outfile = filename.split('.tif')[0] + 'NDWI.png'
    skimage.io.imsave(outfile, ndwi.T, cmap=plt.cm.gray)
    return outfile

if __name__ == '__main__':
    ndwi(sys.argv[1])
