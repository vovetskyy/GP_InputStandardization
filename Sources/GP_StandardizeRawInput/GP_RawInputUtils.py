from collections import namedtuple
from pathlib import Path
import re
import logging

FilenameParts = namedtuple('FilenameParts', ['PC_name', 'Date', 'Time', 'ScriptId', 'FileExt'])


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
    file_ext_str = ''

    filename_p = Path(full_filename)
    file_ext_str = filename_p.suffix

    pure_filename = filename_p.stem
    logging.debug('Start analysis of filename ' + '"' + pure_filename + '"')

    # filename_regex = re.compile(r'(^(\w*))_(((\d\d\d\d)-(\d\d)-(\d\d))_((\d\d)-(\d\d)-(\d\d)(.*))_(IPG|Script2)$)')
    filename_regex = re.compile(r'(^(\w*)(.*))__(((\d\d\d\d)-(\d\d)-(\d\d))_(.*))__((IPG|Script2)$)')
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

    filename_parts = FilenameParts(PC_name=pc_name_str, Date=date_str, Time=time_str, ScriptId=script_id_str,
                                   FileExt=file_ext_str)
    logging.debug('"' + full_filename + '": final parsing result:\n' + str(filename_parts))

    return filename_parts
