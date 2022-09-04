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
# - Original Script2 structure constants -
SCRIPT2_SYS_STATS_STR = 'SYS Stats'
SCRIPT2_SYS_STATS_IDX = 0

SCRIPT2_SYS_STATS_PC_NAME_STR = 'Node'
SCRIPT2_SYS_STATS_CPU_TYPE_STR = 'Machine'
SCRIPT2_SYS_STATS_CPU_DETAILS_STR = 'Processor'
SCRIPT2_SYS_STATS_CPU_LOAD_STR = r'CPU: Load, %, per core'

SCRIPT2_CPU_STATS_STR = 'CPU Stats'
SCRIPT2_CPU_STATS_IDX = 1

SCRIPT2_CPU_STATS_NUM_CORES_STR = 'CPU: Num of cores'

SCRIPT2_IO_BYTES_STR = 'IO, read/write, bytes'
SCRIPT2_IO_MS_STR = 'IO, read/write, milliseconds'

SCRIPT2_IO_BYTES_READ_IDX = 0
SCRIPT2_IO_BYTES_WRITTEN_IDX = 1
SCRIPT2_IO_MS_READ_IDX = 0
SCRIPT2_IO_MS_WRITTEN_IDX = 1

SCRIPT2_MEM_BYTES_STR = 'MEM: total/used/available, bytes'

SCRIPT2_MEM_BYTES_TOTAL_IDX = 0
SCRIPT2_MEM_BYTES_USED_IDX = 1
SCRIPT2_MEM_BYTES_AVAILABLE_IDX = 2

SCRIPT2_NET_BYTES_STR = 'NET Total, sent/received, bytes'

SCRIPT2_NET_BYTES_SENT_IDX = 0
SCRIPT2_NET_BYTES_RECEIVED_IDX = 1


# ---------------------------------------

# ---------------------------------------
# ------------ Table columns ------------

# =======================================


def get_sys_stats_dict(rec: dict) -> dict:
    """
    extracts SYS_Stats information from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information dictionary
    """
    return rec[SCRIPT2_SYS_STATS_IDX][SCRIPT2_SYS_STATS_STR]


def get_cpu_stats_dict(rec: dict) -> dict:
    """
    extracts CPU_Stats information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information dictionary
    """
    return rec[SCRIPT2_CPU_STATS_IDX][SCRIPT2_CPU_STATS_STR]


def get_disk_io_bytes_stats_list(rec: dict) -> list:
    """
    extracts Disk IO information (in bytes) from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information list
    """
    return get_cpu_stats_dict(rec)[SCRIPT2_IO_BYTES_STR]


def get_disk_io_ms_stats_list(rec: dict) -> list:
    """
    extracts Disk IO information (in ms) from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information list
    """
    return get_cpu_stats_dict(rec)[SCRIPT2_IO_MS_STR]


def get_virtual_mem_bytes_stats_list(rec: dict) -> list:
    """
    extracts Virtual Memory (in bytes) from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information list
    """
    return get_cpu_stats_dict(rec)[SCRIPT2_MEM_BYTES_STR]


def get_total_network_bytes_stats_list(rec: dict) -> list:
    """
    extracts total Network Info (in bytes) from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information list
    """
    return get_cpu_stats_dict(rec)[SCRIPT2_NET_BYTES_STR]


def get_pc_name(rec: dict):
    """
    extracts PC name information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_sys_stats_dict(rec)[SCRIPT2_SYS_STATS_PC_NAME_STR]


def get_cpu_type(rec: dict):
    """
    extracts CPU type information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_sys_stats_dict(rec)[SCRIPT2_SYS_STATS_CPU_TYPE_STR]


def get_cpu_details(rec: dict):
    """
    extracts CPU detailed information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_sys_stats_dict(rec)[SCRIPT2_SYS_STATS_CPU_DETAILS_STR]


def get_cpu_num_cores(rec: dict):
    """
    extracts CPU number of cores information  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_cpu_stats_dict(rec)[SCRIPT2_CPU_STATS_NUM_CORES_STR]


def get_cpu_load_list(rec: dict):
    """
    extracts cpu load percentage per core from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_cpu_stats_dict(rec)[SCRIPT2_SYS_STATS_CPU_LOAD_STR]


def get_disk_io_read_total_bytes(rec: dict):
    """
    extracts cumulative read bytes Disk IO  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_disk_io_bytes_stats_list(rec)[SCRIPT2_IO_BYTES_READ_IDX]


def get_disk_io_written_total_bytes(rec: dict):
    """
    extracts cumulative written bytes Disk IO  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_disk_io_bytes_stats_list(rec)[SCRIPT2_IO_BYTES_WRITTEN_IDX]


def get_disk_io_read_total_ms(rec: dict):
    """
    extracts cumulative read ms Disk IO  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_disk_io_ms_stats_list(rec)[SCRIPT2_IO_MS_READ_IDX]


def get_disk_io_written_total_ms(rec: dict):
    """
    extracts cumulative written ms Disk IO  from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_disk_io_ms_stats_list(rec)[SCRIPT2_IO_MS_WRITTEN_IDX]


def get_virtual_mem_total_bytes(rec: dict):
    """
    extracts total Virtual memory bytes from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_virtual_mem_bytes_stats_list(rec)[SCRIPT2_MEM_BYTES_TOTAL_IDX]


def get_virtual_mem_used_bytes(rec: dict):
    """
    extracts used Virtual memory bytes from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_virtual_mem_bytes_stats_list(rec)[SCRIPT2_MEM_BYTES_USED_IDX]


def get_virtual_mem_avail_bytes(rec: dict):
    """
    extracts available Virtual memory bytes from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_virtual_mem_bytes_stats_list(rec)[SCRIPT2_MEM_BYTES_AVAILABLE_IDX]


def get_network_total_sent_bytes(rec: dict):
    """
    extracts total network sent bytes from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_total_network_bytes_stats_list(rec)[SCRIPT2_NET_BYTES_SENT_IDX]


def get_network_total_received_bytes(rec: dict):
    """
    extracts total network sent bytes from Script2 json dictionary
    :param rec: one Script2 json record
    :return: extracted information
    """
    return get_total_network_bytes_stats_list(rec)[SCRIPT2_NET_BYTES_RECEIVED_IDX]


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

    dt_df = pd.DataFrame([(start_datetime, start_date, start_time)], columns=rawu.TIMESTAMPS_COLUMN_NAMES_RM)

    return dt_df


def get_static_machine_info_row(rec: dict) -> pd.DataFrame:
    """
    creates DataFrame row with standardized static system info
    :param rec: one Script2 json record
    :return: created DataFrame
    """
    logging.info('Create static machine info DataFrame row')

    pc_name = get_pc_name(rec)
    cpu_type = get_cpu_type(rec)
    cpu_details = get_cpu_details(rec)
    num_cores = get_cpu_num_cores(rec)

    info_df = pd.DataFrame([(pc_name, cpu_type, cpu_details, num_cores)],
                           columns=[rawu.RAW_PC_NAME_COLUMN_NAME, rawu.CPU_TYPE_COLUMN_NAME,
                                    rawu.CPU_DETAILS_COLUMN_NAME, rawu.CPU_NUM_CORES_COLUMN_NAME])

    return info_df


def get_disk_io_info_row(rec: dict) -> pd.DataFrame:
    """
    creates DataFrame row with standardized info about Disk IO
    :param rec: one Script2 json record
    :return: created DataFrame
    """
    logging.info('Create disk IO DataFrame row')

    read_bytes = get_disk_io_read_total_bytes(rec)
    written_bytes = get_disk_io_written_total_bytes(rec)
    read_ms = get_disk_io_read_total_ms(rec)
    written_ms = get_disk_io_written_total_ms(rec)

    info_df = pd.DataFrame([(read_bytes, written_bytes, read_ms, written_ms)],
                           columns=[rawu.DISK_IO_BYTES_READ_TOTAL_COLUMN_NAME,
                                    rawu.DISK_IO_BYTES_WRITTEN_TOTAL_COLUMN_NAME,
                                    rawu.DISK_IO_MS_READ_TOTAL_COLUMN_NAME,
                                    rawu.DISK_IO_MS_WRITTEN_TOTAL_COLUMN_NAME])

    return info_df


def get_virtual_mem_info_row(rec: dict) -> pd.DataFrame:
    """
    creates DataFrame row with standardized info about Disk IO
    :param rec: one Script2 json record
    :return: created DataFrame
    """
    logging.info('Create Virtual Mem DataFrame row')

    total_bytes = get_virtual_mem_total_bytes(rec)
    used_bytes = get_virtual_mem_used_bytes(rec)
    avail_bytes = get_virtual_mem_avail_bytes(rec)

    info_df = pd.DataFrame([(total_bytes, used_bytes, avail_bytes)],
                           columns=[rawu.VIRTUAL_MEM_BYTES_TOTAL_COLUMN_NAME,
                                    rawu.VIRTUAL_MEM_BYTES_USED_COLUMN_NAME,
                                    rawu.VIRTUAL_MEM_BYTES_AVAILABLE_COLUMN_NAME])

    return info_df


def get_network_total_info_row(rec: dict) -> pd.DataFrame:
    """
    creates DataFrame row with standardized info about Disk IO
    :param rec: one Script2 json record
    :return: created DataFrame
    """
    logging.info('Create Total Network Info DataFrame row')

    sent_bytes = get_network_total_sent_bytes(rec)
    received_bytes = get_network_total_received_bytes(rec)

    info_df = pd.DataFrame([(sent_bytes, received_bytes)],
                           columns=[rawu.NETWORK_BYTES_SENT_TOTAL_COLUMN_NAME,
                                    rawu.NETWORK_BYTES_RECEIVED_TOTAL_COLUMN_NAME])

    return info_df


def get_cpu_cores_load_info_row(rec):
    """
    creates DataFrame row with standardized info about CPU core loads
    :param rec: one Script2 json record
    :return: created DataFrame
    """
    logging.info('Create Total CPU load Info DataFrame row')
    load_list = get_cpu_load_list(rec)
    columns_names = [rawu.get_cpu_load_per_core_column_name(x) for x in range(len(load_list))]

    info_df = pd.DataFrame([tuple(load_list)], columns=columns_names)

    return info_df


def get_sys_overall_row():
    """
    creates DataFrame row with standardized info overall system "process"
    :return: created dataframe
    """
    logging.info('Create Overall system as "Process" DataFrame row')
    info_df = pd.DataFrame([rawu.OVERALL_SYSTEM_PROCESS_NAME], columns=[rawu.OVERALL_SYSTEM_COLUMN_NAME])

    return info_df


def get_avg_cpu_load_row(cores_load):
    mean_df = cores_load.mean(axis=1)
    sys_load = pd.DataFrame(mean_df, columns=[rawu.OVERAL_CPU_LOAD_COLUMN_NAME])

    return sys_load


def parse_script2_record(timestamp, rec):
    logging.info('Start handling of timestamp ' + timestamp)

    dt_df = get_datetime_df_row(timestamp)
    machine_info_df = get_static_machine_info_row(rec)
    disk_io_info_df = get_disk_io_info_row(rec)
    virtual_mem_info_df = get_virtual_mem_info_row(rec)
    total_net_info_df = get_network_total_info_row(rec)
    cpu_load_per_core_df = get_cpu_cores_load_info_row(rec)
    avg_cpu_load_df = get_avg_cpu_load_row(cpu_load_per_core_df)
    overal_sys_process_df = get_sys_overall_row()

    sys_rec = pd.concat([dt_df, machine_info_df, disk_io_info_df, virtual_mem_info_df,
                         total_net_info_df, overal_sys_process_df, cpu_load_per_core_df, avg_cpu_load_df],
                        axis=1)

    return sys_rec


def store_standardized_Script2_to_csv(df: pd.DataFrame, out_dir: str):
    pass


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

    # create final DataFrames with the overall system info and process-specific info
    sys_list = []
    for i in range(len(times_list)):
        sys_rec = parse_script2_record(times_list[i], json_dict[times_list[i]])
        sys_list.append(sys_rec)
    sys_df = pd.concat(sys_list)
    store_standardized_Script2_to_csv(sys_df, out_dir)


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
        # pp(str(file))
        continue

    standardize_raw_Script2_file(str(file_list[0]), out_dir)
