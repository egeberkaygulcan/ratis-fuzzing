import os
import pickle
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--plot', action='store_true')
    parser.add_argument('-s', '--states', type=str, nargs='+')
    parser.add_argument('-st', '--stats', type=str, nargs='+')

    return parser.parse_args()

def load_pickles(files):
    pickles = []
    for file in files:
        filename = os.path.basename(file)
        with open(file, 'rb') as f:
            data = pickle.load(f)
        pickles.append((data, filename))
    return pickles

if __name__ == '__main__':
    args = parse_args()

    stats = load_pickles(args.stats)
    states = load_pickles(args.states)

    for state in states[0][0].values():
        print(state)
        print('---------------')
        
    print(stats)
    