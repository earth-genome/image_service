
from inspect import getsourcefile
import os
import sys

base_dir = os.path.dirname(
    os.path.dirname(os.path.abspath(getsourcefile(lambda:0))))
sys.path.insert(1, os.path.join(base_dir, 'webapp'))
sys.path.insert(1, base_dir)

