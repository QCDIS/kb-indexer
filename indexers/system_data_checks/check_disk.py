import os

from .. import utils


def main():
    for dirpath, dirnames, filenames in os.walk(utils.get_data_dir()):
        if 'nltk_data' in dirpath:
            continue
        if len(filenames):
            n = len(filenames)
            print(f'{n/1e3:7.3f}k {dirpath}')
