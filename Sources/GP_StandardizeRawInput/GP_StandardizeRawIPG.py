import logging
from pathlib import Path
from pprint import pprint as pp

import GP_RawInputUtils as raw_utils


def get_delimiter_pos(lines):
    """
    finds delimiter (1st empty line) between real time measurements section
    and cumulative measurements section in raw IPG file
    :param lines:
    :return:
        delimiter position, if found
        -1 otherwise
    """
    pos = -1

    for i in range(len(lines)):
        if (lines[i].strip() == ''):
            pos = i
            break

    return pos


def standardize_raw_IPG(full_filename):
    """
    create the following files from the raw IPG file, created by Intel Power Gadget utility (
    https://www.intel.com/content/www/us/en/developer/articles/tool/power-gadget.html):
        - csv-file with real-time measurements (copied from raw file)
        - csv-file with cumulative measurements (converted from text format, present in raw file)

    :param full_filename: full name (including full path) of the raw IPG file
    :return: None
    """
    logging.info('Start handling of file ' + '"' + full_filename + '"')

    # get info from full filename and check, if the file can be handled
    filename = Path(full_filename).name
    filename_parts = raw_utils.get_filename_parts(full_filename)

    # read original file to strings
    IPG_file = open(full_filename)
    IPG_content = IPG_file.readlines()
    IPG_file.close()

    delim_pos = get_delimiter_pos(IPG_content)
    if(delim_pos == -1):
        logging.error('"' + filename + '": wrong format: no sections delimiter found')
    else:
        real_meas_lines = IPG_content[:delim_pos]
        cum_meas_lines = IPG_content[delim_pos+1:]
