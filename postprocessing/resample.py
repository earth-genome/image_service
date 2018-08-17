
import numpy as np

from PIL import Image
import skimage.io

def make_thumbnail(path, max_dims=(512,512)):
    """Convert image to thumbnail.

    Arguments:
        path: path to an image file
        max_dims: tuple of max output thumbnail dimensions in pixels
            (PIL.Image will preserve aspect ratio within these bounds.)

    Returns: None. (Overwrites input image on success.)
    """
    try: 
        img = Image.open(path)
    except OSError:
        return
    img.thumbnail(max_dims)
    img = np.asarray(img)     # force the resampling now
    skimage.io.imsave(path, img)
    return
