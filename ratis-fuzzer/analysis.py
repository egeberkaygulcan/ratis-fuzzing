import os
import pickle
import argparse
import matplotlib.pyplot as plt

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--plot', action='store_true')
    parser.add_argument('-s', '--states', type=str, nargs='+')
    parser.add_argument('-st', '--stats', type=str, nargs='+')
    parser.add_argument('-rd', '--root_dir', type=str, default='experiments_berkay')

    return parser.parse_args()

def load_stats(root_dir, files):
    pickles = []
    for file in files:
        file = os.path.join(root_dir, f'{file}_stats.pkl')
        filename = os.path.basename(file)
        with open(file, 'rb') as f:
            data = pickle.load(f)
        pickles.append((data, filename))
    return pickles

if __name__ == '__main__':
    args = parse_args()

    stats = load_stats(args.root_dir, args.stats)
    # states = load_pickles(os.path.join(args.root_dir, f'{args.states}_states.pkl'))

    coverage = [(stat['coverage'], name) for stat, name in stats]
    for cov, name in coverage:
        plt.plot(range(len(cov)), cov)
    plt.legend(['random', 'state', 'trace'])
    plt.xlabel("Iterations")
    plt.ylabel("# of distinct states")
    plt.savefig('ratis_cov.png', dpi=300)


    