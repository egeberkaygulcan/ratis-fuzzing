import os
import time
import json
import random
import pickle 
import shutil
import logging
import requests
import traceback

from network import Network
from cluster import RatisCluster
from types import SimpleNamespace

# Aggreement for consensus violations
# Recovery
# Termination for consensus violation
# Liveness, wether the cluster reaches consensus eventually (Bounded liveness)

########## MUTATOR CLASSES ###########

class DefaultMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace):
        new_trace = []
        for e in trace:
            new_trace.append(e)
        return new_trace

class RandomMutator():
    def __init__(self) -> None:
        pass

    def mutate(self, trace):
        return None
    
class SwapMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace: list[dict]) -> list[dict]:
        new_trace = []
        max_step = 0
        for e in trace:
            if e["type"] == "Schedule" and e["step"] > max_step:
                max_step = e["step"]
        
        [first, second] = random.sample(range(max_step), 2)
        first_value = ()
        second_value = ()
        for e in trace:
            if e["type"] == "Schedule" and e["step"] == first:
                first_value = {"type": "Schedule", "node": e["node"], "step": e["step"]}
            elif e["type"] == "Schedule" and e["step"] == second:
                second_value = {"type": "Schedule", "node": e["node"], "step": e["step"]}
        
        for e in trace:
            if e["type"] != "Schedule":
                new_trace.append(e)
            if e["step"] == first:
                new_trace.append(second_value)
            elif e["step"] == second:
                new_trace.append(first_value)
            else:
                new_trace.append(e)
        
        return new_trace
    
class SwapCrashStepsMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace: list[dict]) -> list[dict]:
        new_trace = []

        crash_steps = set()
        for e in trace:
            if e["type"] == "Crash":
                crash_steps.add(e["step"])
        
        [first, second] = random.sample(list(crash_steps), 2)
        for e in trace:
            if e["type"] != "Crash":
                new_trace.append(e)
            
            if e["step"] == first:
                new_trace.append({"type": "Crash", "node": e["node"], "step": second})
            elif e["step"] == second:
                new_trace.append({"type": "Crash", "node": e["node"], "step": first})
            else:
                new_trace.append(e)
            
        return new_trace
    
class SwapCrashNodesMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace: list[dict]) -> list[dict]:
        new_trace = []

        crash_steps = {}
        for e in trace:
            if e["type"] == "Crash":
                crash_steps[e["step"]] = e["node"]
        
        [first, second] = random.sample(list(crash_steps.keys()), 2)
        for e in trace:
            if e["type"] != "Crash":
                new_trace.append(e)
            
            if e["step"] == first:
                new_trace.append({"type": "Crash", "node":crash_steps[second], "step": e["step"]})
            elif e["step"] == second:
                new_trace.append({"type": "Crash", "node":crash_steps[first], "step": e["step"]})
            else:
                new_trace.append(e)

        return new_trace


class CombinedMutator:
    def __init__(self, mutators) -> None:
        self.mutators = mutators
    
    def mutate(self, trace: list[dict]) -> list[dict]:
        new_trace = []
        for e in trace:
            new_trace.append(e)
        
        for m in self.mutators:
            new_trace = m.mutate(new_trace)
        
        return new_trace

########## GUIDER CLASSES ###########

class TLCGuider:
    def __init__(self, tlc_addr) -> None:
        self.tlc_addr = tlc_addr
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
                return new_states
            else:
                logging.info("Received error response from TLC, code: {}, text: {}".format(r.status_code, r.content))
        except Exception as e:
            logging.info("Error received from TLC: {}".format(e))
            pass

        return 0
    
    def coverage(self):
        return len(self.states.keys())

    def save_states(self, dir_):
        with open(dir_, 'wb') as f:
            pickle.dump(self.states, f)
        
    def load_states(self, dir_):
        with open(dir_, 'rb') as f:
            self.states = pickle.load(f)

    def reset(self):
        self.states = {}

class TraceGuider:
    def __init__(self, tlc_addr) -> None:
        self.traces = []
        self.tlc_addr = tlc_addr
        self.states = {}
    
    def check_new_state(self, trace, event_trace, name, record = False) -> int:
        trace_str = ''
        for step in trace:
            if step['type'] == 'ClientRequest':
                trace_str = trace_str + 'ClientRequest_'
            else:
                trace_str = trace_str + f'{step["type"]}_{step["node"]}_'
        if trace_str not in self.traces:
            self.traces.append(trace_str)
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
                    return new_states
            except Exception as e:
                logging.info("Error received from TLC: {}".format(e))
                pass
        return 0
                
    
    def coverage(self):
        return len(self.states.keys())

    def save_states(self, dir_):
        with open(dir_, 'wb') as f:
            pickle.dump((self.traces, self.states), f)
        
    def load_states(self, dir_):
        with open(dir_, 'rb') as f:
            self.traces, self.traces = pickle.load(f)

    def reset(self):
        self.traces = []
        self.states = {}


########## FUZZER CLASS ###########

class Fuzzer:
    def __init__(self, args, load, config = {}) -> None:
        self.config = self._validate_config(config)
        self.args = args
        self.network = Network(self.config.network_addr, self.config)
        self.guider = self.config.guider
        self.mutator = self.config.mutator
        self.trace_queue = []
        self.cluster = RatisCluster(self.config, self.network)
        self.stats = {
            "coverage" : [0],
            "random_traces": 0,
            "mutated_traces": 0
        }
        self.prev_iters = 0

        if load:
            self.load()
    
    def reset(self):
        self.cluster.reset()
        self.guider.reset()
        self.trace_queue = []
        self.stats = {
            "coverage" : [0],
            "random_traces": 0,
            "mutated_traces": 0
        }
    
    def save(self, iters):
        path = os.path.join(self.config.save_dir, str(iters))
        os.makedirs(path, exist_ok=True)
        self.guider.save_states(os.path.join(path, f'{self.config.exp_name}_states.pkl'))
        with open(os.path.join(path, f'{self.config.exp_name}_traces.pkl'), 'wb') as f:
            pickle.dump(self.trace_queue, f)
        
        with open(os.path.join(path, f'{self.config.exp_name}_stats.pkl'), 'wb') as f:
            pickle.dump(self.stats, f)

    def load(self):
        dirs = os.listdir(self.config.save_dir)
        dirs = [int(dir_) for dir_ in dirs]
        self.prev_iters = max(dirs)
        path = os.path.join(self.config.save_dir, str(self.prev_iters))

        self.guider.load_states(os.path.join(path, f'{self.config.exp_name}_states.pkl'))
        with open(os.path.join(path, f'{self.config.exp_name}_traces.pkl'), 'rb') as f:
            self.trace_queue = pickle.load(f)
        
        with open(os.path.join(path, f'{self.config.exp_name}_stats.pkl'), 'rb') as f:
            self.stats = pickle.load(f)

    def _validate_config(self, config):
        new_config = SimpleNamespace()
        
        mutators = [SwapMutator(), SwapCrashNodesMutator(), SwapCrashStepsMutator()]
        if "mutator" not in config:
            new_config.mutator = CombinedMutator(mutators)
        else:
            if config["mutator"] == "all":
                new_config.mutator = CombinedMutator(mutators)
            else:
                new_config.mutator = DefaultMutator()
            
        
        if "network_addr" not in config:
            new_config.network_addr = ("127.0.0.1", 7074)
        else:
            new_config.network_addr = config["network_addr"]

        if "iterations" not in config:
            new_config.iterations = 10
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
            new_config.crash_quota = 10
        else:
            new_config.crash_quota = config["crash_quota"]

        if "mutations_per_trace" not in config:
            new_config.mutations_per_trace = 5
        else:
            new_config.mutations_per_trace = config["mutations_per_trace"]
        
        if "seed_population" not in config:
            new_config.seed_population = 10
        else:
            new_config.seed_population = config["seed_population"]

        new_config.seed_frequency = 1000
        if "seed_frequency" in config:
            new_config.seed_frequency = config["seed_frequency"]
        
        if "test_harness" not in config:
            new_config.test_harness = 3
        else:
            new_config.test_harness = config["test_harness"]

        new_config.record_file_prefix = ""
        if "record_file_prefix" in config:
            new_config.record_file_prefix = config["record_file_prefix"]

        tlc_addr = "127.0.0.1:2023"
        if "tlc_addr" in config:
            tlc_addr = config["tlc_addr"]
        
        if 'guider' in config:
            if config['guider'] == 'trace':
                new_config.guider = TraceGuider(tlc_addr)
            elif config['guider'] == 'state':
                new_config.guider = TLCGuider(tlc_addr)
            else:
                new_config.guider = TLCGuider(tlc_addr)
        else:
            new_config.guider = TLCGuider(tlc_addr)


        if 'exp_name' not in config:
            new_config.exp_name = 'naive_random'
        else:
            new_config.exp_name = config['exp_name']
        
        if 'jar_path' not in config:
            new_config.jar_path = '/Users/berkay/Documents/Research/ratis-fuzzing/ratis-examples/target/ratis-examples-2.5.1.jar'
        else:
            new_config.jar_path = config['jar_path']

        if 'error_path' not in config:
            new_config.error_path = './errors'
        else:
            new_config.error_path = config['error_path']

        new_config.snapshots_path = "/tmp/ratis"
        if "snapshots_path" in config:
            new_config.snapshots_path = config["snapshots_path"]
            
        save_dir = './saved'
        if "save_dir" in config:
            new_config.save_dir = config["save_dir"]
        else:
            new_config.save_dir = save_dir
        
        os.makedirs(save_dir, exist_ok=True)
        
        if 'save_every' not in config:
            new_config.save_every = 100
        else:
            new_config.save_every = config['save_every']

        return new_config

    def seed(self, iter):
        logging.info("Seeding for iteration {}".format(iter))
        self.trace_queue = []
        for i in range(self.config.seed_population):
            (trace, _) = self.run_iteration("seed_{}_{}".format(iter, i))
            self.trace_queue.append(trace)

    def run(self):
        logging.info("Starting fuzzer loop")
        naive_random = self.config.exp_name == 'naive_random'
        start = time.time_ns()
        for i in range(self.config.iterations):
            iter_count = i + self.prev_iters
            if iter_count >= self.config.iterations:
                return True
            if i != 0 and i % self.config.save_every == 0:
                self.save(iter_count)
            logging.info(f'##### Starting fuzzer iteration {iter_count} #####')
            if i % self.config.seed_frequency == 0 and not naive_random:
                self.seed(iter_count)

            to_mimic = None
            if len(self.trace_queue) > 0:
                to_mimic = self.trace_queue.pop(0)
            if to_mimic is None:
                self.stats["random_traces"] += 1
            else:
                self.stats["mutated_traces"] += 1
            try:
                traces = self.run_iteration("fuzz_{}".format(iter_count), to_mimic)
                if traces is not None:
                    (trace, event_trace) = traces
                else:
                    self.cluster.shutdown()
                    return False
            except Exception as ex:
                logging.error(f"Error running iteration {iter_count}: {ex}")
                self.cluster.shutdown()
                return False
            else:
                new_states = self.guider.check_new_state(trace, event_trace, str(iter_count), record=False)
                logging.info(f'New states: {new_states}')
                logging.info(f'Total states: {self.guider.coverage()}')
                if new_states > 0 and not naive_random:
                    for j in range(new_states * self.config.mutations_per_trace):
                        try:
                            mutated_trace = self.mutator.mutate(trace)
                            if mutated_trace is not None:
                                self.trace_queue.append(mutated_trace)
                        except:
                            logging.error(f"Error mutating {iter_count}: {ex}")
                        finally:
                            continue
                self.stats["coverage"].append(self.guider.coverage())
        self.stats["runtime"] = time.time_ns() - start
        self.save(self.config.iterations)
        self.cluster.shutdown()
        return True

        

    def run_iteration(self, iteration, mimic=None):
        logging.debug('***************** STARTING ITERATION *******************')
        trace = []
        crashed = set()

        crash_points = {}
        start_points = {}
        schedule = []
        client_requests = []
        if mimic is None:
            node_ids = [i for i in range(1,self.config.nodes+1)]
            for c in random.sample(range(0, self.config.horizon, 2), self.config.crash_quota):
                node_id = random.choice(node_ids)
                crash_points[c] = node_id
                s = random.choice(range(c, self.config.horizon))
                start_points[s] = node_id

            client_requests = random.sample(range(self.config.horizon), self.config.test_harness)
            for choice in random.choices(node_ids, k=self.config.horizon):
                schedule.append(choice)
        else:
            schedule = [1 for i in range(self.config.horizon)]
            for ch in mimic:
                if ch["type"] == "Crash":
                    crash_points[ch["step"]] = ch["node"]
                elif ch["type"] == "Start":
                    start_points[ch["step"]] = ch["node"]
                elif ch["type"] == "Schedule":
                    schedule.append(ch["node"])
                elif ch["type"] == "ClientRequest":
                    client_requests.append(ch["step"])

        logging.debug("Starting cluster")
        self.cluster.start(iteration)
        while self.network.check_replicas():
            if self.cluster.error_flag:
                break
            time.sleep(1e-3)
        event_trace = []
        wait_count = 0
        try:
            for i in range(self.config.horizon):
                logging.debug("Taking step {}".format(i))
                if self.cluster.error_flag:
                    break
                if i in start_points and start_points[i] in crashed:
                    node_id = start_points[i]
                    logging.debug(f"Starting crashed node {node_id}")
                    self.network.send_restart(str(node_id))
                    trace.append({"type": "Start", "node": node_id, "step": i})
                    self.network.add_event({"name": "Add", "params": {"i": node_id}})
                    crashed.remove(node_id)
                
                if i in crash_points:
                    node_id = crash_points[i]
                    logging.debug(f"Crashing node {node_id}")
                    if node_id not in crashed:
                        self.network.send_crash(str(node_id))
                    crashed.add(node_id)
                    trace.append({"type": "Crash", "node": node_id, "step": i})
                    self.network.add_event({"name": "Remove", "params": {"i": node_id}})
                
                mailboxes = self.network.check_mailboxes()
                while len(mailboxes) < 1:
                    if self.cluster.error_flag:
                        break
                    if wait_count >= 50 and i > 0:
                        break
                    time.sleep(1e-3)
                    wait_count += 1
                    mailboxes = self.network.check_mailboxes()
                wait_count = 0
                if self.cluster.error_flag:
                    break

                node_id = schedule[i]
                if str(node_id) in mailboxes:
                    self.network.schedule_replica(str(node_id))
                else:
                    if len(mailboxes) > 0:
                        self.network.schedule_replica(str(random.choice(mailboxes)))
                trace.append({"type": "Schedule", "node": node_id, "step": i})

                if i in client_requests:
                    try:
                        logging.debug(f"Executing client request {i}")                        
                        self.network.send_client_request()
                        trace.append({"type": "ClientRequest", "step": i})
                        self.network.add_event({"name": 'ClientRequest', "params": {"leader": self.network.leader_id, "request": self.cluster.client_request_counter-1}})
                    except:
                        pass
        except Exception as e:
            logging.error(f'run_iteration exception: {e}')
            try:
                traceback.print_exc()
            except:
                logging.error('Cannot print exception')
        finally:
            try:
                logging.debug("Shutting down cluster")
                ret = True
                if not self.cluster.error_flag:
                    ret = self.network.send_shutdown()
                if ret is False:
                    return None
            except Exception as e:
                logging.error('Cannot send shutdown!')
                logging.error(e)
                return None

        event_trace = self.network.get_event_trace()

        if self.cluster.error_log is not None:
            stderr, stdout = self.cluster.error_log
            path = os.path.join(self.config.error_path, f'{self.config.exp_name}_{iteration}')
            os.makedirs(path, exist_ok=True)
            with open(os.path.join(path, 'stderr.log'), 'w+') as f:
                f.writelines(stderr)
            with open(os.path.join(path, 'stdout.log'), 'w+') as f:
                f.writelines(stdout)
            with open(os.path.join(path, 'trace.log'), 'w+') as f:
                for trace in event_trace:
                    f.write(f'{str(trace)}\n')
            return None



        self.cluster.reset()
        return (trace, event_trace)