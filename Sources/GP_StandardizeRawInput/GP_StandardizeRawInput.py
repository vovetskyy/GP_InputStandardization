import logging
from pathlib import Path
from pprint import pprint as pp
import GP_StandardizeRawIPG as ipg


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
    logging.info('start')

    parsing_dir = r'c:\Wit\Scripts\GreenParrot\FastShot1\RawOutput\DESKTOP-FP4OP26'
    out_dir = r'c:\Wit\Scripts\GreenParrot\__STD_OUTPUT'

    parse_path = Path(parsing_dir)
    file_list = (list(parse_path.glob('*__IPG.*')))
    # pp(file_list[0])

    for file in file_list:
        ipg.standardize_raw_IPG(str(file), out_dir)

    # ipg.standardize_raw_IPG(r'c:\Wit\Scripts\GreenParrot\FastShot1\RawOutput\DESKTOP-FP4OP26\DESKTOP-FP4OP26__2022-07-31_12-16-45__IPG.csv',
    #                        out_dir)
