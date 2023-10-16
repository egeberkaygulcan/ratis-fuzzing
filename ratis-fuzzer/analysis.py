import os
import pickle
import argparse
import matplotlib.pyplot as plt

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--plot', action='store_true')
    parser.add_argument('-s', '--states', type=str, nargs='+')
    parser.add_argument('-st', '--stats', type=str, nargs='+')
    parser.add_argument('-rd', '--root_dir', type=str, default='experiments_berkay/output/saved')

    return parser.parse_args()

def load_stats(root_dir, files):
    pickles = []
    for file in files:
        file = os.path.join(root_dir, f'{file}_stats.pkl')
        with open(file, 'rb') as f:
            data = pickle.load(f)
        pickles.append(data)
    return pickles

def report_bugs(stats, labels):
    bugs = [stat['bug_iterations'] for stat in stats]
    for i, bug in enumerate(bugs):
        print(f'# of bugs {labels[i]}: {len(bug)}')
        print(bug)
        print('----------')

if __name__ == '__main__':
    args = parse_args()

    stats = load_stats(args.root_dir, args.stats)
    # states = load_pickles(os.path.join(args.root_dir, f'{args.states}_states.pkl'))

    coverage = [stat['coverage'] for stat in stats]
    for cov in coverage:
        plt.plot(range(len(cov)), cov)
    plt.legend(['random', 'state', 'trace'])
    plt.xlabel("Iterations")
    plt.ylabel("# of distinct states")
    plt.savefig('plots/ratis_cov_.png', dpi=300)

    report_bugs(stats, ['random', 'state', 'trace'])



    