
import logging
import os
import sys

def build_logger(directory, logfile, logger_name=__name__):
    logger = logging.getLogger(logger_name)
    if not os.path.exists(directory):
        os.makedirs(directory)
    fh = logging.FileHandler(os.path.join(directory, logfile))
    logger.addHandler(fh)
    return logger

def signal_handler(*args):
    print('KeyboardInterrupt: Writing logs before exiting...')
    logging.shutdown()
    sys.exit(0)
