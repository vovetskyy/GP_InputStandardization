import argparse
import logging
from pathlib import Path
from pprint import pprint as pp
import GP_StandardizeRawIPG as ipg

DEF_OUT_DIR = '__STD_RAW_OUTPUT'


def standardize_raw_IPG_in_dir(parsing_dir, out_dir):
    """
    finds all raw IPG files in parsing_dir and stores standardized files in out_dir
    :param parsing_dir:
    :param out_dir:
    :return: None
    """
    # parse IPG raw inputs
    parse_path = Path(parsing_dir)
    file_list = (list(parse_path.glob('*__IPG.*')))

    for file in file_list:
        ipg.standardize_raw_IPG_file(str(file), out_dir)
        # pp(str(file))

    # ipg.standardize_raw_IPG(r'c:\Wit\Scripts\GreenParrot\FastShot1\RawOutput\DESKTOP-FP4OP26\DESKTOP-FP4OP26__2022-07-31_12-16-45__IPG.csv',
    #                       out_dir)


if __name__ == "__main__":
    # let's start with logging
    logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
    logging.info('Start GP raw input standardization')

    # parse command-line options
    cmd_parser = argparse.ArgumentParser(description='Standardization of GP raw input files')
    cmd_parser.add_argument('--indir', help='Directory to parse for raw input. By default -- current_dir',
                            default=str(Path.cwd()))
    cmd_parser.add_argument('--outdir',
                            help='Directory to store result of the parsing. By default  -- current_dir\\' + DEF_OUT_DIR,
                            default=str(Path(Path.cwd(), DEF_OUT_DIR)))
    cmd_parser.print_help()  # print it anyway as user-friendly hint

    cmd_args = cmd_parser.parse_args()

    # parsing_dir = r'c:\Wit\Scripts\GreenParrot\FastShot1\RawOutput\DESKTOP-FP4OP26'
    parsing_dir = cmd_args.indir
    # out_dir = r'c:\Wit\Scripts\GreenParrot\__STD_OUTPUT'
    out_dir = cmd_args.outdir

    logging.info('Start parsing of "' + parsing_dir + '"')
    logging.info('Results will be stored to  "' + out_dir + '"')

    standardize_raw_IPG_in_dir(parsing_dir, out_dir)
