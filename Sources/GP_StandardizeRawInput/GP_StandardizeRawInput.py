import logging
import GP_StandardizeRawIPG as ipg

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')


def test():
    print('K')
    pass


if __name__ == "__main__":
    logging.info('start')
    ipg.standardize_raw_IPG(r'c:\Wit\Scripts\GreenParrot\FastShot1\RawOutput\DESKTOP-FP4OP26\DESKTOP-FP4OP26__2022-07-14_15-28-42__IPG.csv')
