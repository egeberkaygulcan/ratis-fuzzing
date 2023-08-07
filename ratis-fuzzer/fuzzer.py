import time
import json
import random
import requests
import logging
import traceback

from tqdm import tqdm
from network import Network
from types import SimpleNamespace
from cluster import RatisCluster

# Aggreement for consensus violations
# Recovery
# Termination for consensus violation
# Liveness, wether the cluster reaches consensus eventually (Bounded liveness)

class DefaultMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace):
        new_trace = []
        for e in trace:
            new_trace.append(e)
        return new_trace
    
class TLCGuider:
    def __init__(self, tlc_addr) -> None:
        self.tlc_addr = tlc_addr
        self.states = {}
    
    # TODO - Return new states
    def check_new_state(self, trace, event_trace):
        trace_to_send = event_trace
        trace_to_send.append({"reset": True})
        r = requests.post("http://"+self.tlc_addr+"/execute", json=json.dumps(trace_to_send))
        if r.ok:
            response = r.json()
            have_new = False
            for i in range(len(response["states"])):
                tlc_state = {"state": response["states"][i], "key" : response["keys"][i]}
                if tlc_state["key"] not in self.states:
                    self.states[tlc_state["key"]] = tlc_state
                    have_new = True
            return have_new
        return False
    

class Fuzzer:
    def __init__(self, args, config = {}) -> None:
        self.config = self._validate_config(config)
        self.args = args
        self.network = Network(self.config.network_addr, self.config)
        self.guider = self.config.guider
        self.mutator = self.config.mutator
        self.trace_queue = []
        self.cluster = RatisCluster(self.config, self.network)

    def _validate_config(self, config):
        new_config = SimpleNamespace()
        if "mutator" not in config:
            new_config.mutator = DefaultMutator()
        else:
            new_config.mutator = config["mutator"] # TODO - turn into function
        
        if "network_addr" not in config:
            new_config.network_addr = ("127.0.0.1", 7074)
        else:
            new_config.network_addr = config["network_addr"]
        
        if "guider" not in config:
            tlc_addr = "127.0.0.1:2023"
            if "tlc_addr" in config:
                tlc_addr = config["tlc_addr"]
            new_config.guider = TLCGuider(tlc_addr)
        else:
            new_config.guider = config["guider"]

        if "no_iterations" not in config:
            new_config.no_iterations = 100
        else:
            new_config.no_iterations = 100

        if "horizon" not in config:
            new_config.horizon = 100
        else:
            new_config.horizon = config["horizon"]

        if "nodes" not in config:
            new_config.nodes = 3
        else:
            new_config.nodes = config["nodes"]
        
        if "crash_quota" not in config:
            new_config.crash_quota = 0
        else:
            new_config.crash_quota = config["crash_quota"]

        if "mutations_per_trace" not in config:
            new_config.mutations_per_trace = 5
        else:
            new_config.mutations_per_trace = config["mutations_per_trace"]
        
        if "init_population" not in config:
            new_config.init_population = 100
        else:
            new_config.init_population = config["init_population"]
        
        if "test_harness" not in config:
            new_config.test_harness = 5
        else:
            new_config.test_harness = config["test_harness"]

        if 'num_client_requests' not in config:
            new_config.num_client_requests = 2
        else: 
            new_config.num_client_requests = config['num_client_requests']

        if 'num_concurrent_client_requests' not in config:
            new_config.num_concurrent_client_requests = 1
        else:
            new_config.num_concurrent_client_requests = config['num_concurrent_client_requests']

        if 'parent_dir' not in config:
            new_config.parent_dir = '/Users/berkay/Documents/Research/ratis-fuzzing/ratis-fuzzer/output'
        else:
            new_config.parent_dir = config['parent_dir']

        if 'exp_name' not in config:
            new_config.exp_name = 'naive_random'
        else:
            new_config.exp_name = config['exp_name']

        return new_config

    def run(self):
        # self.cluster.start(0)

        # logging.info('Sleeping...')
        # time.sleep(3)

        # self.cluster.send_client_request()

        # logging.info('Sleeping...')
        # time.sleep(1)

        # self.cluster.send_client_request()

        # logging.info('Sleeping...')
        # time.sleep(1)

        # self.cluster.shutdown()

        for i in range(self.config.init_population):
            (trace, event_trace) = self.run_iteration(True)
            # for j in range(self.config.mutations_per_trace):
            #     self.trace_queue.append(self.mutator.mutate(trace))  
        # new_states = self.guider.check_new_state(trace, event_trace)   

        # self.cluster.shutdown()

        # for i in range(self.config.no_iterations):
        #     to_mimic = None
        #     if len(self.trace_queue) > 0:
        #         to_mimic = self.trace_queue.pop(0)
        #     (trace, event_trace) = self.run_iteration(to_mimic)
        #     if self.guider.check_new_state(trace, event_trace):
        #         for j in range(self.config.mutations_per_trace):
        #             self.trace_queue.append(self.mutator.mutate(trace))

        # TODO: plot coverage

        

    def run_iteration(self, random_, schedule=None):
        logging.info('***************** STARTING ITERATION *******************')
        trace = []
        # crashed = None

        # crash_points = {}
        # restart_points = {}
        # schedule = []
        # client_requests = []
        # node_ids = list(range(1, self.config.nodes+1))
        # if mimic is None:
        #     for c in random.sample(range(0, self.config.horizon, 2), self.config.crash_quota):
        #        crash_points[c] = random.choice(node_ids)

        #     # client_requests = random.sample(range(self.config.horizon), self.config.num_client_requests)
            
        #     for i in range(self.config.horizon):
        #         schedule.append(random.choice(node_ids))
        # else:
        #     schedule = [0 for i in range(self.config.horizon)]
        #     for ch in mimic:
        #         if ch["type"] == "Crash":
        #             crash_points[ch["node"]] = ch["step"]
        #         elif ch['type'] == 'Restart':
        #             restart_points[ch['node']] = ch['step']
        #         elif ch["type"] == "Schedule":
        #             schedule[ch["step"]] = ch["node"]
        #         elif ch["type"] == "ClientRequest":
        #             client_requests.append(ch["step"])


        self.cluster.start(0)
        while self.network.check_replicas(self.config.nodes):
            time.sleep(0.001)

        node_ids = []
        try:
            if random_:
                for i in tqdm(range(self.config.horizon), disable=not self.args.verbose):
                    if self.cluster.get_completed_requests() >= self.config.num_client_requests:
                        break
                    node_ids = self.network.check_mailboxes()
                    while len(node_ids) == 0:
                        time.sleep(0.001)
                        node_ids = self.network.check_mailboxes()
                    if self.cluster.get_leader() is not None and self.cluster.get_pending_requests() < self.config.num_client_requests:
                        node_ids.append(0)
                    node_id = random.sample(node_ids, 1)[0]
                    if node_id == 0:
                        self.cluster.send_client_request()
                    else:
                        # logging.info(f'Scheduling: {node_id}')
                        self.network.schedule_replica(str(node_id))
                    trace.append(node_id)
                    node_ids = []
        except Exception as e:
            logging.error(f'Exception on fuzzer loop: {e}')
            traceback.print_exc(e)
        finally:
            self.cluster.shutdown()
            event_trace = self.network.get_event_trace()
            self.network.clear_mailboxes()


        return (trace, event_trace)