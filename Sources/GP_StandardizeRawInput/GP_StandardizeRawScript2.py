import logging
from pathlib import Path
import io
import pandas as pd
from pprint import pprint as pp

import GP_RawInputUtils as rawu


def standardize_raw_Script2_in_dir(parsing_dir, out_dir):
    """
    finds all raw Script2 files in parsing_dir and stores standardized files in out_dir
    :param parsing_dir:
    :param out_dir:
    :return: None
    """
    # parse IPG raw inputs
    parse_path = Path(parsing_dir)
    file_list = (list(parse_path.glob('*__IPG.*')))

    for file in file_list:
        # standardize_raw_IPG_file(str(file), out_dir)
        pp(str(file))
