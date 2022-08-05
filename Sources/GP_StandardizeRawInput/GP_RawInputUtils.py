from collections import namedtuple
from pathlib import Path
import re
import datetime as dt
import logging

# =======================================
# ============= CONSTANTS ===============

# ---------------------------------------
# --------- Filenames constants ---------
RAW_FILENAME_DELIM = '__'
RAW_IPG_FILENAME_SUFFIX = 'IPG'

RAW_REALMEAS_FILENAME_SUFFIX = 'RM'
RAW_IPG_REALMEAS_FILENAME_SUFFIX = RAW_IPG_FILENAME_SUFFIX + RAW_FILENAME_DELIM + RAW_REALMEAS_FILENAME_SUFFIX
# ---------------------------------------

# ---------------------------------------
# --------- Filenames constants ---------
RAW_DATETIME_COLUMN_NAME = 'Raw DateTime'
RAW_DATE_COLUMN_NAME = 'Raw Date'
RAW_TIME_COLUMN_NAME = 'Raw Time'
# ---------------------------------------

# =======================================

# =======================================
# ============= STD TYPES ===============
RawInputFilenameParts = namedtuple('FilenameParts', ['PC_name', 'Date', 'Time', 'ScriptId', 'FileExt'])
# =======================================


def get_filename_parts(full_filename):
    """
    Function to parse filenames of the PG raw input files.
    Expected format of the name:
    [PC_NAME]_[Date]_[Time]_[Raw_Script_id].[extension]
    E.g. DESKTOP-FP4OP26_2022-07-30_13-35-55_IPG.csv,
    which is created by Intel Power Gadget (IPG) utility

    Returns the corresponding FilenameParts tuple
    """
    logging.debug('Start parsing of file ' + '"' + full_filename + '"')

    pc_name_str = ''
    date_str = ''
    time_str = ''
    script_id_str = ''

    filename_p = Path(full_filename)
    file_ext_str = filename_p.suffix

    pure_filename = filename_p.stem
    logging.debug('Start analysis of filename ' + '"' + pure_filename + '"')

    # filename_regex = re.compile(r'(^(\w*))_(((\d\d\d\d)-(\d\d)-(\d\d))_((\d\d)-(\d\d)-(\d\d)(.*))_(IPG|Script2)$)')
    regex_str = r'(^(\w*)(.*))__(((\d\d\d\d)-(\d\d)-(\d\d))_(.*))__' + '((' + RAW_IPG_FILENAME_SUFFIX + ')$)'
    filename_regex = re.compile(regex_str)
    filename_parts_re = filename_regex.search(pure_filename)

    if filename_parts_re is None:
        logging.error(pure_filename + 'is not in the expected format for name parsing')
    else:
        pc_name_str = filename_parts_re.group(1)
        date_str = filename_parts_re.group(5)
        time_str = filename_parts_re.group(9)
        script_id_str = filename_parts_re.group(10)

        logging.debug('"' + pure_filename + '" parsing\'s results:')
        for i in range(len(filename_parts_re.groups())):
            logging.debug(str(i) + ': "' + filename_parts_re.group(i) + '"')

    filename_parts = RawInputFilenameParts(PC_name=pc_name_str, Date=date_str, Time=time_str, ScriptId=script_id_str,
                                           FileExt=file_ext_str)
    logging.debug('"' + full_filename + '": final parsing result:\n' + str(filename_parts))

    return filename_parts


def get_aligned_datetime_serie(orig_df, filename_parts):
    """
    calculates correct dates for passed datafraeme, depends on passed filename parts

    :param orig_df: Dataframe with inaligned dates
    :param filename_parts: parsed RawInputFilenameParts tuple
    :return: aligned Serie
    """
    logging.debug('start datetime alignment')

    # times_df[0] = times_df[0].replace(2000)
    aligned_df = orig_df

    # get datetime info stored in the filename
    start_date = dt.date.fromisoformat(filename_parts.Date)
    start_time = dt.datetime.strptime(filename_parts.Time, "%H-%M-%S").time()
    logging.debug('Recognized start date from filename: ' + str(start_date))
    logging.debug('Recognized start time from filename: ' + str(start_time))

    # calculate date of the first measurement:
    #   - the same day as in start_date if start_time stored in filenam is smaller than the first time in the table
    #   - start_date + 1 day otherwise, as measurement seem to be started already on the next day
    first_meas_time = aligned_df[0].time()

    logging.debug('Recognized start time from the table: ' + str(first_meas_time))

    if (first_meas_time < start_time):
        start_date = start_date + dt.timedelta(days=1)
        logging.debug('As start time from the table is smaller than in the filaname, startdate was increased tó: '
                      + str(start_date))

    # set alignd date to the 1st element
    aligned_df[0] = aligned_df[0].replace(start_date.year, start_date.month, start_date.day)

    # set aligned dates for all other element, considering possible day wraparound
    cur_date = start_date
    for i in range(1, len(aligned_df)):
        cur_time = aligned_df[i].time()
        prev_time = aligned_df[i - 1].time()

        if (cur_time < prev_time):
            cur_date = cur_date + dt.timedelta(days=1)
            logging.debug(f'As df[{i}].time {cur_time} is smaller than df[{i - 1}].time {prev_time}, cur date was '
                          f'increased to: {cur_date}')

        aligned_df[i] = aligned_df[i].replace(cur_date.year, cur_date.month, cur_date.day)

    return aligned_df
