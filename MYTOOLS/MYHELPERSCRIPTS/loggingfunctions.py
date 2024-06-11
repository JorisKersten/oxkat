# Imports.
import logging
import time
import sys
import os
from contextlib import suppress
import warnings


# Function to initialize the logger.
def logging_initialization(in_program_name):
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(filename='FlagPercentage.log', filemode='w',
                        format='[%(asctime)s.%(msecs)03d UTC] [%(levelname)s]   %(message)s',   # Variable msecs gives milliseconds.
                        datefmt='%Y-%m-%dT%H:%M:%S',   # Variable asctime does not contain any fraction of seconds.
                        level=logging.INFO)   # Levels: DEBUG, INFO, WARNING, ERROR, CRITICAL.
    logging.info("{} started: logging initialized.".format(in_program_name))
    logging.info("")


# This function sends a message to the log file and also prints it to stdout.
# The input is converted to string and split on newlines before being sent to the logger.
# It is preferable to print only single line messages with this function, since the log does not look great otherwise.
# But how many columns is a single line? There are 39 characters printed before the message appears in the log,
# in case of 'INFO'.
# Let's take 255 characters total allowed. This means that 216 characters are left for the string if INFO is used.
def logandprint(in_data, in_level='INFO'):
    inner_str = str(in_data)
    # inner_strs = inner_str.splitlines()
    inner_strs = inner_str.split('\n')
    if len(inner_strs) < 1:
        inner_strs = ["",]
    for cur_str in inner_strs:
        if in_level == 'DEBUG':
            logging.debug(cur_str)
        elif in_level == 'INFO':
            logging.info(cur_str)
        elif in_level == 'WARNING':
            logging.warning(cur_str)
        elif in_level == 'ERROR':
            logging.error(cur_str)
        elif in_level == 'CRITICAL':
            logging.critical(cur_str)
        else:
            with suppress(OSError): os.fsync(sys.stdout.fileno())
            raise ValueError("Function logandprint: 'level' input not recognized.")
    print(in_data)
