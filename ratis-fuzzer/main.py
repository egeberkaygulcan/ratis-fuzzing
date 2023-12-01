import os
import time
import random
import pickle
import asyncio
import logging
import argparse
import traceback

from pandas import read_csv
from types import SimpleNamespace

from model_fuzz.fuzzer import Fuzzer
from model_fuzz.guider import TLCGuider, TraceGuider, EmptyGuider
from model_fuzz.mutator import SwapMutator, SwapCrashStepsMutator, SwapCrashNodesMutator, CombinedMutator, MaxMessagesMutator, DefaultMutator

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-c', '--config', type=str, default='random_state_config.csv')
    parser.add_argument('-l', '--load', action='store_true')
    parser.add_argument('-ct', '--control', type=str)
    parser.add_argument('-s', '--seed', type=int, default=123456)

    return parser.parse_args()

def validate_config(config):
        new_config = SimpleNamespace()
        
        mutators = [SwapMutator(), SwapCrashNodesMutator(), SwapCrashStepsMutator()]
        if "mutator" not in config:
            new_config.mutator = CombinedMutator(mutators)
        else:
            if config["mutator"] == "all":
                new_config.mutator = CombinedMutator(mutators)
            else:
                new_config.mutator = DefaultMutator()
            
        
        if "base_network_port" not in config:
            new_config.base_network_port = 7071
        else:
            new_config.base_network_port = config["base_network_port"]
        
        if "base_server_client_port" not in config:
            new_config.base_server_client_port = 10000
        else:
            new_config.base_server_client_port = config["base_server_client_port"]
            
        if "base_peer_port" not in config:
            new_config.base_peer_port = 6000
        else:
            new_config.base_peer_port = config["base_peer_port"]

        if "iterations" not in config:
            new_config.iterations = 10000
        else:
            new_config.iterations = config["iterations"]

        if "horizon" not in config:
            new_config.horizon = 100
        else:
            new_config.horizon = config["horizon"]

        if "nodes" not in config:
            new_config.nodes = 3
        else:
            new_config.nodes = config["nodes"]
        
        if "crash_quota" not in config:
            new_config.crash_quota = 1
        else:
            new_config.crash_quota = config["crash_quota"]

        if "mutations_per_trace" not in config:
            new_config.mutations_per_trace = 5
        else:
            new_config.mutations_per_trace = config["mutations_per_trace"]
        
        if "seed_population" not in config:
            new_config.seed_population = 20
        else:
            new_config.seed_population = config["seed_population"]

        new_config.seed_frequency = 200
        if "seed_frequency" in config:
            new_config.seed_frequency = config["seed_frequency"]
        
        if "test_harness" not in config:
            new_config.test_harness = 3
        else:
            new_config.test_harness = config["test_harness"]

        tlc_addr = "127.0.0.1:2023"
        if "tlc_addr" in config:
            tlc_addr = config["tlc_addr"]
        
        if 'guider' in config:
            if config['guider'] == 'trace':
                new_config.guider = TraceGuider(tlc_addr)
            elif config['guider'] == 'state':
                new_config.guider = TLCGuider(tlc_addr)
            elif config['guider'] == 'empty':
                new_config.guider = EmptyGuider(tlc_addr)
            else:
                new_config.guider = TLCGuider(tlc_addr)
        else:
            new_config.guider = TLCGuider(tlc_addr)


        if 'exp_name' not in config:
            new_config.exp_name = 'random'
        else:
            new_config.exp_name = config['exp_name']
        
        if 'jar_path' not in config:
            new_config.jar_path = '../ratis-examples/target/ratis-examples-2.5.1.jar'
        else:
            new_config.jar_path = config['jar_path']

        if 'error_path' not in config:
            new_config.error_path = 'output/errors'
        else:
            new_config.error_path = config['error_path']

        new_config.snapshots_path = "/tmp/ratis"
        if "snapshots_path" in config:
            new_config.snapshots_path = config["snapshots_path"]
        
        if "max_message_to_schedule" not in config:
            new_config.max_messages_to_schedule = 20
            
        save_dir = 'output/saved'
        if "save_dir" in config:
            new_config.save_dir = config["save_dir"]
        else:
            new_config.save_dir = save_dir
        
        if 'save_every' not in config:
            new_config.save_every = 10
        else:
            new_config.save_every = config['save_every']

        if 'num_workers' not in config:
            new_config.num_workers = 10
        else:
            new_config.num_workers = config['num_workers']

        return new_config

def main():
    args = parse_args()
    if not args.verbose:
        logging.disable()
    else:
        logging.basicConfig(level=logging.INFO)
    
    experiment_config = read_csv(args.config, index_col=False)
    random_seed = + time.time_ns()
    random.seed(random_seed)
    os.makedirs('output/saved', exist_ok=True)
    with open('output/saved/random_seed.pkl', 'wb') as f:
        pickle.dump(random_seed, f)
    
    load = args.load
    for index, row in experiment_config.iterrows():
        config = validate_config(row.to_dict())
        fuzzer = Fuzzer(load, config)
        if args.control is not None:
            asyncio.run(fuzzer.run_controlled(args.control))
            break
        try:
            asyncio.run(fuzzer.run())
        except:
            pass
        finally:
            load = True

if __name__ == '__main__':
    try:
        main() # random 447
    except Exception as e:
        traceback.print_exc()
