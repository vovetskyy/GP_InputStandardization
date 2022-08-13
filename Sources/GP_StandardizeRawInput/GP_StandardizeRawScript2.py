import logging
from pathlib import Path
import json
import pandas as pd
from pprint import pprint as pp

import GP_RawInputUtils as rawu


def standardize_raw_Script2_file(full_filename:str, out_dir:str):
    """
    converts to std format the json-files, created in the format of the script
    https://github.com/vovetskyy/GP_FastShot1/blob/0aa893d3a6ab1badd33bcb735aee5e09351f960e/main.py

    :param full_filename: string with full name (including full path) of the raw IPG file
    :param out_dir: string with full path to the ditectory to store resulting file(s)
    :return: None
    """
    logging.info('Start handling of file ' + '"' + full_filename + '"')

    with open(full_filename) as json_file:
        json_dict = json.load(json_file)
    # pp(json_dict.keys())



def standardize_raw_Script2_in_dir(parsing_dir, out_dir):
    """
    finds all raw Script2 files in parsing_dir and stores standardized files in out_dir
    :param parsing_dir:
    :param out_dir:
    :return: None
    """
    logging.info('Start standardization of raw Script2 files from "' + str(parsing_dir) + '" to "' + str(out_dir) + '"')

    parse_path = Path(parsing_dir)
    file_list = (list(parse_path.glob('*__Script2.*')))

    for file in file_list:
        # standardize_raw_Script2_file(str(file), out_dir)
        pp(str(file))

    standardize_raw_Script2_file(str(file_list[0]), out_dir)
