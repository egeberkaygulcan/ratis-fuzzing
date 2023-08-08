import os
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
    
class TLCGuider:
    def __init__(self, tlc_addr, record_path=None) -> None:
        self.tlc_addr = tlc_addr
        self.record_path = record_path
        self.states = {}
    
    def check_new_state(self, trace, event_trace, name, record = False) -> int:
        trace_to_send = event_trace
        trace_to_send.append({"reset": True})
        logging.debug("Sending trace to TLC: {}".format(trace_to_send))
        try:
            r = requests.post("http://"+self.tlc_addr+"/execute", json=trace_to_send)
            if r.ok:
                response = r.json()
                logging.debug("Received response from TLC: {}".format(response))               
                new_states = 0
                for i in range(len(response["states"])):
                    tlc_state = {"state": response["states"][i], "key" : response["keys"][i]}
                    if tlc_state["key"] not in self.states:
                        self.states[tlc_state["key"]] = tlc_state
                        new_states += 1
                if record:
                    with open(os.path.join(self.record_path, name+".log"), "w") as record_file:
                        lines = ["Trace sent to TLC: \n", json.dumps(trace_to_send, indent=2)+"\n\n", "Response received from TLC:\n", json.dumps(response, indent=2)+"\n"]
                        record_file.writelines(lines)
                return new_states
            else:
                logging.info("Received error response from TLC, code: {}, text: {}".format(r.status_code, r.content))
        except Exception as e:
            logging.info("Error received from TLC: {}".format(e))
            pass

        return 0
    
    def coverage(self):
        return len(self.states.keys())

    def reset(self):
        self.states = {}

    

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
            new_config.no_iterations = config["no_iterations"]

        if "horizon" not in config:
            new_config.horizon = 50
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
            new_config.test_harness = 1
        else:
            new_config.test_harness = config["test_harness"]

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
        # time.sleep(0.5)

        # self.cluster.send_client_request()

        # logging.info('Sleeping...')
        # time.sleep(1)

        # self.cluster.send_client_request()

        # logging.info('Sleeping...')
        # time.sleep(1)

        # self.cluster.shutdown()

        if self.config.exp_name == 'naive_random':
            for i in range(self.config.no_iterations):
                (trace, event_trace) = self.run_iteration(i)
                # new_states = self.guider.check_new_state(trace, event_trace, str(i), record=False)
                # print(new_states)
                # if new_states > 0:
                #     for j in range(new_states * self.config.mutations_per_trace):
                #         mutated_trace = self.mutator.mutate(trace)
                #         if mutated_trace is not None:
                #             self.trace_queue.append(mutated_trace)
                # self.trace_queue.append(trace)
                # new_states = self.guider.check_new_state(trace, event_trace) 
            # (trace, event_trace) = self.run_iteration(1, mimic=self.trace_queue.pop(0))
        # else:
        #     pass
            # for i in range(self.config.init_population):
            #     (trace, event_trace) = self.run_iteration(i)
                # for j in range(self.config.mutations_per_trace):
                #     self.trace_queue.append(self.mutator.mutate(trace))  
            # new_states = self.guider.check_new_state(trace, event_trace)   


            # for i in range(self.config.no_iterations):
            #     to_mimic = None
            #     if len(self.trace_queue) > 0:
            #         to_mimic = self.trace_queue.pop(0)
            #     (trace, event_trace) = self.run_iteration(to_mimic)
            #     if self.guider.check_new_state(trace, event_trace):
            #         for j in range(self.config.mutations_per_trace):
            #             self.trace_queue.append(self.mutator.mutate(trace))

            # TODO: plot coverage

        

    def run_iteration(self, iteration, mimic=None):
        logging.info('***************** STARTING ITERATION *******************')
        trace = []
        crashed = set()

        crash_points = {}
        start_points = {}
        schedule = []
        client_requests = []
        random_ = False
        if mimic is None:
            random_ = True
            node_ids = self.cluster.get_node_ids() # list(range(1, self.config.nodes+1))
            for c in random.sample(range(0, self.config.horizon, 2), self.config.crash_quota):
                node_id = random.choice(node_ids)
                crash_points[c] = node_id
                s = random.choice(range(c, self.config.horizon))
                start_points[s] = node_id

            client_requests = random.sample(range(self.config.horizon), self.config.test_harness)
            # for choice in random.choices(node_ids, k=self.config.horizon):
            #     schedule.append(choice)
        else:
            schedule = [1 for i in range(self.config.horizon)]
            for ch in mimic:
                if ch["type"] == "Crash":
                    crash_points[ch["step"]] = ch["node"]
                elif ch["type"] == "Start":
                    start_points[ch["step"]] = ch["node"]
                elif ch["type"] == "Schedule":
                    schedule[ch["step"]] = ch["node"]
                elif ch["type"] == "ClientRequest":
                    client_requests.append(ch["step"])

        # def record_logs(record_path, cluster):
        #     LOG.debug("Recording logs to path: {}".format(record_path))
        #     with open(record_path, "w") as record_file:
        #         lines = []
        #         for _id, logs in cluster.get_log_lines().items():
        #             lines.append("Log for node: {}\n".format(_id))
        #             lines.append("------ Stdout -----\n")
        #             for line in logs["stdout"]:
        #                 lines.append(line+"\n")
        #             lines.append("------ Stderr -----\n")
        #             for line in logs["stderr"]:
        #                 lines.append(line+"\n")
        #             lines.append("\n\n")
        #         record_file.writelines(lines)

        logging.debug("Creating cluster")
        self.cluster.start(0)
        while self.network.check_replicas(self.config.nodes):
            time.sleep(0.001)
        event_trace = []
        pending_requests = 0
        try:
            for i in range(self.config.horizon): # tqdm(range(self.config.horizon), disable=not self.args.verbose):
                if self.cluster.check_timeout():
                    break
                logging.debug("Taking step {}".format(i))
                if i in start_points and start_points[i] in crashed:
                    node_id = start_points[i]
                    logging.info(f"Starting crashed node {node_id}")
                    # self.cluster.node(node_id).start()
                    self.network.send_restart(str(node_id))
                    trace.append({"type": "Start", "node": node_id, "step": i})
                    self.network.add_event({"name": "Add", "params": {"i": node_id}})
                    crashed.remove(node_id)
                
                if i in crash_points:
                    node_id = crash_points[i]
                    logging.info(f"Crashing node {node_id}")
                    if node_id not in crashed:
                        self.network.send_crash(str(node_id))
                    crashed.add(node_id)
                    trace.append({"type": "Crash", "node": node_id, "step": i})
                    self.network.add_event({"name": "Remove", "params": {"i": node_id}})
                                
                # for node_id in self.cluster.node_ids():
                #     logging.debug("Updating state of node: {}".format(node_id))
                #     if node_id not in crashed:
                #         info = self.cluster.node(node_id).info()
                #         state = info['raft_role']
                #         lastlog_index = int(info["raft_current_index"])
                #         commit_index = int(info["raft_commit_index"])
                #         term = int(info["raft_current_term"])
                #         self.network.add_event({"name": "UpdateState", "params": {"i": node_id, "state": state, "last_index": lastlog_index, "commit_index": commit_index, "term": term}})
                if random_:
                    node_id = -1
                    node_ids = self.network.check_mailboxes()
                    while len(node_ids) == 0:
                        if self.cluster.check_timeout():
                            break
                        time.sleep(0.001)
                        node_ids = self.network.check_mailboxes()
                    node_id = random.choice(node_ids)
                else:
                    node_id = str(schedule[i])
                    node_ids = self.network.check_mailboxes()
                    while node_id not in node_ids:
                        time.sleep(0.001)
                        node_ids = self.network.check_mailboxes()
                self.network.schedule_replica(node_id)
                trace.append({"type": "Schedule", "node": node_id, "step": i, "max_messages": node_id})

                if i in client_requests:
                    try:
                        logging.debug(f"Executing client request {i}")                        
                        # self.cluster.execute_async('INCRBY', 'counter', 1, with_retry=False)
                        self.cluster.send_client_request()
                        trace.append({"type": "ClientRequest", "step": i})
                        self.network.add_event({"name": 'ClientRequest', "params": {"leader": self.cluster.get_leader(), "request": self.cluster.client_request_counter-1}})
                    except:
                        pass
                    # trace.append({"type": "ClientRequest", "step": i})
                
                # if self.cluster.get_leader() is not None:
                #     for _ in range(pending_requests):
                #         self.cluster.send_client_request()
                #         pending_requests -= 1
                #         trace.append({"type": "ClientRequest", "step": i})
                #         self.network.add_event({"name": 'ClientRequest', "params": {"leader": self.cluster.get_leader(), "request": self.cluster.client_request_counter-1}})
        except Exception as e:
            print(e)
            # record_logs(path.join(self.config.report_path, "{}_{}.log".format(self.config.record_file_prefix, iteration)), cluster)
        finally:
            logging.debug("Destroying cluster")
            try:
                i = self.config.horizon - 1
                while self.cluster.get_completed_requests() < self.config.test_harness:
                    if self.cluster.check_timeout():
                        break
                    node_id = -1
                    node_ids = self.network.check_mailboxes()
                    while len(node_ids) == 0:
                        if self.cluster.check_timeout():
                            break
                        time.sleep(0.001)
                        node_ids = self.network.check_mailboxes()
                    node_id = random.choice(node_ids)
                    self.network.schedule_replica(node_id)
                    trace.append({"type": "Schedule", "node": node_id, "step": i, "max_messages": node_id})
                    i += 1
                self.cluster.shutdown()
            except:
                pass
                # record_logs(path.join(self.config.report_path, "{}_{}.log".format(self.config.record_file_prefix, iteration)), cluster)

        event_trace = self.network.get_event_trace()
        self.network.clear_mailboxes()

        return (trace, event_trace)
        # logging.info('***************** STARTING ITERATION *******************')
        # trace = []
        # self.cluster.start(0)
        # while self.network.check_replicas(self.config.nodes):
        #     time.sleep(0.001)

        # node_ids = []
        # try:
        #     if random_:
        #         for i in tqdm(range(self.config.horizon), disable=not self.args.verbose):
        #             if self.cluster.get_completed_requests() >= self.config.num_client_requests:
        #                 break
        #             node_ids = self.network.check_mailboxes()
        #             while len(node_ids) == 0:
        #                 time.sleep(0.001)
        #                 node_ids = self.network.check_mailboxes()
        #             if self.cluster.get_leader() is not None and self.cluster.get_pending_requests() < self.config.num_client_requests:
        #                 node_ids.append(0)
        #             node_id = random.sample(node_ids, 1)[0]
        #             if node_id == 0:
        #                 self.cluster.send_client_request()
        #             else:
        #                 # logging.info(f'Scheduling: {node_id}')
        #                 self.network.schedule_replica(str(node_id))
        #             trace.append(node_id)
        #             node_ids = []
        # except Exception as e:
        #     logging.error(f'Exception on fuzzer loop: {e}')
        #     traceback.print_exc(e)
        # finally:
        #     self.cluster.shutdown()
        #     event_trace = self.network.get_event_trace()
        #     self.network.clear_mailboxes()


        # return (trace, event_trace)