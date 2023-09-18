import os
import shutil
import logging
import argparse

from fuzzer import Fuzzer
from pandas import read_csv

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-c', '--config', type=str, default='random_state_config.csv')
    parser.add_argument('-l', '--load', action='store_true')

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    if not args.verbose:
        logging.disable()
    else:
        logging.basicConfig(level=logging.INFO)
    
    experiment_config = read_csv(args.config, index_col=False)

    
    load = args.load
    for index, row in experiment_config.iterrows():
        fuzzer = Fuzzer(args, load, row.to_dict())
        ret = fuzzer.run()
        load = True
        while ret is False:
            logging.error("Restarting run!")
            fuzzer = Fuzzer(args, load, row.to_dict())
            ret = fuzzer.run()
        