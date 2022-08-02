import logging
import GP_StandardizeRawIPG as ipg


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
    logging.info('start')

    ipg.standardize_raw_IPG(r'c:\Wit\Scripts\GreenParrot\FastShot1\RawOutput\DESKTOP-FP4OP26\DESKTOP-FP4OP26__2022-07-31_12-16-45__IPG.csv')

