import os


def main():
    data_dir = os.environ['DATA_DIR']

    for dirpath, dirnames, filenames in os.walk(data_dir):
        if 'nltk_data' in dirpath:
            continue
        if len(filenames):
            n = len(filenames)
            print(f'{n/1e3:7.3f}k {dirpath}')
