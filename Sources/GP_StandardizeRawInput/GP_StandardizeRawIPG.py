import logging
from pathlib import Path
import io
import pandas as pd
from pprint import pprint as pp

import GP_RawInputUtils as raw_utils

IPG_TIME_COLUMN_NAME = "System Time"


def get_delimiter_pos_in_IPG(lines):
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


def transform_IPG_real_meas_to_df(meas_lines, filename_parts):
    """
    transforms list of csv-lines, read from IPG for real measurements, to pandas Dataframe

    :param meas_lines: array of strings in csv-format
    :param filename_parts: parsed RawInputFilenameParts tuple

    :return: converted pandas Dataframe
    """
    # covert IPG csv to Dataframe
    # see https://stackoverflow.com/questions/42171709/creating-pandas-dataframe-from-a-list-of-strings
    meas_df = pd.read_csv(io.StringIO('\n'.join(meas_lines)), delim_whitespace=False)

    # set type of 'System Time' column to datetime manually, as it could not be recognized automatically
    # And then rename the resulting Datetime columnn accordingly
    times_serie = pd.to_datetime(meas_df['System Time'], format="%H:%M:%S:%f")
    datetimes_serie = raw_utils.get_aligned_datetime_serie(times_serie, filename_parts)
    datetimes_serie.name = raw_utils.RAW_DATETIME_COLUMN_NAME

    # get raw date column from calculated datetime and rename the column accordingly
    dates_serie = datetimes_serie.dt.date
    dates_serie.name = raw_utils.RAW_DATE_COLUMN_NAME

    # set standard column name for reported system times
    meas_df.rename(columns={IPG_TIME_COLUMN_NAME: raw_utils.RAW_TIME_COLUMN_NAME}, inplace=True)

    #concat all data to one table and return the result
    meas_df = pd.concat([datetimes_serie, dates_serie, meas_df], axis=1)

    return meas_df


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

    delim_pos = get_delimiter_pos_in_IPG(IPG_content)
    if (delim_pos == -1):
        logging.error('"' + filename + '": wrong format: no sections delimiter found')
    else:
        real_meas_lines = IPG_content[:delim_pos]
        cum_meas_lines = IPG_content[delim_pos + 1:]

        real_meas_df = transform_IPG_real_meas_to_df(real_meas_lines, filename_parts)

