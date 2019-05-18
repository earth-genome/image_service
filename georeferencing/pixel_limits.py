"""Functions to determine pixel value limits for standard image formats."""

import numpy as np

def get_max(dtype):
    """Determine the maximum allowed pixel value based on image dtype."""
    if np.issubdtype(dtype, np.integer):
        return np.iinfo(dtype).max
    elif np.issubdtype(dtype, np.float):
        return 1.0
    else:
        raise TypeError('Image dtype <{}> not recognized.'.format(dtype))

