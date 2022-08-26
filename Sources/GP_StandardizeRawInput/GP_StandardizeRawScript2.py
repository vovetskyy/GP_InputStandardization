import logging
from pathlib import Path
import json
from datetime import datetime
import pandas as pd
from pprint import pprint as pp

import GP_RawInputUtils as rawu


# =======================================
# ============= CONSTANTS ===============


# ---------------------------------------


def get_sys_stats_dict(rec: dict) -> dict:
    """
    extracts SYS_Stats information from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information dictionary
    """
    return rec[0]['SYS Stats']


def get_cpu_stats_dict(rec: dict) -> dict:
    """
    extracts CPU_Stats information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information dictionary
    """
    return rec[1]['CPU Stats']


def get_disk_io_bytes_stats_list(rec: dict) -> list:
    """
    extracts Disk IO information (in bytes) from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information list
    """
    return get_cpu_stats_dict(rec)['IO, read/write, bytes']


def get_disk_io_ms_stats_list(rec: dict) -> list:
    """
    extracts Disk IO information (in ms) from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information list
    """
    return get_cpu_stats_dict(rec)['IO, read/write, milliseconds']


def get_pc_name(rec: dict):
    """
    extracts PC name information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_sys_stats_dict(rec)['Node']


def get_cpu_type(rec: dict):
    """
    extracts CPU type information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_sys_stats_dict(rec)['Machine']


def get_cpu_details(rec: dict):
    """
    extracts CPU detailed information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_sys_stats_dict(rec)['Processor']


def get_cpu_num_cores(rec: dict):
    """
    extracts CPU number of cores information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_cpu_stats_dict(rec)['CPU: Num of cores']


def get_disk_io_read_total_bytes(rec: dict):
    """
    extracts cumulative read bytes Disk IO  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_disk_io_bytes_stats_list(rec)[0]


def get_disk_io_written_total_bytes(rec: dict):
    """
    extracts cumulative written bytes Disk IO  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_disk_io_bytes_stats_list(rec)[1]


def get_disk_io_read_total_ms(rec: dict):
    """
    extracts cumulative read ms Disk IO  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_disk_io_ms_stats_list(rec)[0]


def get_disk_io_written_total_ms(rec: dict):
    """
    extracts cumulative written ms Disk IO  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_disk_io_ms_stats_list(rec)[1]


def get_datetime_df_row(timestamp):
    """
    creates DataFrame row with standardized date/time info
    :param timestamp:
    :return: created DataFrame
    """

    logging.info('Create DataFrame of timestamp ' + timestamp)

    # convert to datetime without timezone, as timezone seems to be irrelevant
    dt = datetime.strptime(timestamp, '%Y_%m_%d_%H_%M_%S_%f_')

    start_date = dt.date().isoformat()
    start_time = dt.time().isoformat()
    start_datetime = rawu.get_date_time_str(start_date, start_time)

    dt_df = pd.DataFrame(list(zip([start_datetime], [start_date], [start_time])))

    return dt_df


def get_static_machine_info_row(rec: dict) -> pd.DataFrame:
    """
    creates DataFrame raw with standardized static system info
    :param rec: one Script2 json record
    :return: created DataFrame
    """
    logging.info('Create static machine info DataFrame')

    pc_name = get_pc_name(rec)
    cpu_type = get_cpu_type(rec)
    cpu_details = get_cpu_details(rec)
    num_cores = get_cpu_num_cores(rec)

    info_df = pd.DataFrame(list(zip([pc_name], [cpu_type], [cpu_details], [num_cores])))

    return info_df


def get_disk_io_info_row(rec: dict) -> pd.DataFrame:
    """
    creates DataFrame raw with standardized info about Disk IO
    :param rec: one Script2 json record
    :return: created DataFrame
    """
    logging.info('Create disk IO DataFrame')

    read_bytes = get_disk_io_read_total_bytes(rec)
    written_bytes = get_disk_io_written_total_bytes(rec)
    read_ms = get_disk_io_read_total_ms(rec)
    written_ms = get_disk_io_written_total_ms(rec)

    info_df = pd.DataFrame(list(zip([read_bytes], [written_bytes], [read_ms], [written_ms])))

    return info_df


def handle_script2_record(timestamp, rec):
    res_dict = {}

    logging.info('Start handling of timestamp ' + timestamp)

    dt_df = get_datetime_df_row(timestamp)
    machine_info_df = get_static_machine_info_row(rec)
    disk_io_info_df = get_disk_io_info_row(rec)
    pp(disk_io_info_df)


def standardize_raw_Script2_file(full_filename: str, out_dir: str):
    """
    converts to std format the json-files, created in the format of the script
    https://github.com/vovetskyy/GP_FastShot1/blob/0aa893d3a6ab1badd33bcb735aee5e09351f960e/main.py

    :param full_filename: string with full name (including full path) of the raw IPG file
    :param out_dir: string with full path to the directory to store resulting file(s)
    :return: None
    """
    logging.info('Start handling of file ' + '"' + full_filename + '"')

    with open(full_filename) as json_file:
        json_dict = json.load(json_file)

    # as dict keys are not mandatory sorted, get timestamps sorted by time
    times_list = list(json_dict.keys())
    times_list.sort()
    # test_item = json_dict[times_list[0]]

    handle_script2_record(times_list[0], json_dict[times_list[0]])


def standardize_raw_Script2_in_dir(parsing_dir: str, out_dir: str):
    """
    finds all raw Script2 files in parsing_dir and stores standardized files in out_dir
    :param parsing_dir:
    :param out_dir:
    :return: None
    """
    logging.info('Start standardization of raw Script2 files from "' + str(parsing_dir) + '" to "' + str(out_dir) + '"')

    parse_path = Path(parsing_dir)
    file_list = (list(parse_path.glob('*' + rawu.RAW_FILENAME_DELIM + rawu.RAW_SCRIPT2_FILENAME_SUFFIX + '.*')))

    for file in file_list:
        # standardize_raw_Script2_file(str(file), out_dir)
        pp(str(file))

    standardize_raw_Script2_file(str(file_list[0]), out_dir)
