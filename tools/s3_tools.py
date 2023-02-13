import os
import glob
import logging


def clear_s3_folder(path: str):

    files = glob.glob(f'{path}*.json')

    for f in files:
        os.remove(f)
        logging.info(f, 'Файл удален')
