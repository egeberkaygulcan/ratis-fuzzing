import os
import shutil
import logging
import argparse

from fuzzer import Fuzzer
from pandas import read_csv

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true')

    return parser.parse_args()

if __name__ == '__main__':
    experiment_config = read_csv('experiment_config.csv', index_col=False)
    args = parse_args()
    if not args.verbose:
        logging.disable()
    else:
        logging.basicConfig(level=logging.INFO)

    snapshots_path = '/tmp/ratis'
    
    for index, row in experiment_config.iterrows():
        if os.path.exists(snapshots_path):
            shutil.rmtree(snapshots_path)
        fuzzer = Fuzzer(args, row.to_dict())
        fuzzer.run()