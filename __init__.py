"""
Since this repo is to be used as a git submodule, add its directory
to sys.path. Imports can be given with respect to packages in this
directory: from postprocessing import color
"""

from inspect import getsourcefile
import os
import sys

current_dir = os.path.dirname(os.path.abspath(getsourcefile(lambda:0)))
sys.path.insert(1, current_dir)
