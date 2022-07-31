import logging
from pathlib import Path

import GP_RawInputUtils as raw_utils


def standardize_raw_IPG(full_filename):
    logging.info('Start handling of file ' + '"' + full_filename + '"')

    # get info from full filename and check, if the file can be handled
    filename = Path(full_filename).name
    filename_parts = raw_utils.get_filename_parts(filename)

    # read original file to strings
    IPG_file = open(full_filename)
    IPG_content = IPG_file.readlines()
    IPG_file.close()
