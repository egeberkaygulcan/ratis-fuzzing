import os
import subprocess
import threading

from enum import Enum
from network import Network

class ErrorType(Enum):
    PROCESS_ERROR=0
    MULTIPLE_LEADERS=1

class ServerState(Enum):
    FOLLOWER=0
    LEADER=1


class RatisServer:
    def __init__(self, id, port, peers) -> None:
        self.id = id
        self.ratis_client_port = 7074 + self.id
        self.port = port
        self.peers = peers
        self.process = None
        self.term = 0
        self.commit_index = 0
        self.state = ServerState.FOLLOWER
        self.return_code = 0
        
        print(f'Server {self.id} created.')

    def start(self, verbose):
        cmd = f'./start_server.sh {self.id} {self.peers} {self.ratis_client_port}'
        if verbose:
            self.process = subprocess.Popen(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
        else:
            with open(os.devnull, 'w') as fp:
                self.process = subprocess.Popen(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True, stdout=fp)
        self.catch_error()
    
    def shutdown(self):
        print(f'Server {self.id} shutting down.')
        self.process.terminate()
        self.process.kill()
        self.return_code = self.process.returncode

    def check_output(self):
        return self.process.communicate()
    
    def validate(self):
        if self.process.returncode != 0:
            return self.check_output
    
    def update_server(self, term, commit_idx, state):
        self.term = term
        self.commit_index = commit_idx
        self.state = state
    
    def catch_error(self):
        def run():
            out, err = self.check_output()
            if err != 0:
                print('Exception!!!')
            return
        
        thread = threading.Thread(target=run)
        thread.start()

class RatisClient:
    def __init__(self, peers) -> None:
        self.peers = peers
        self.outputs = []
        self.errors = []
        self.return_codes = []
        self.lock = threading.Lock()
        print('RatisClient created.')
    
    def send_request(self, name, value):
        def run(name, value, peers, on_exit):
            cmd = f'./run_client.sh {name} {value} {peers}'
            process = subprocess.Popen(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
            process.wait()
            out, err = process.communicate()
            on_exit(out, err, process.returncode)
            return
        
        print('Client request sent.')
        thread = threading.Thread(target=run, args=(name, value, self.peers, self.on_exit))
        thread.start()

    def on_exit(self, out, err, ret_code):
        self.lock.acquire()
        self.outputs.append(out)
        self.errors.append(err)
        self.return_codes.append(ret_code)
        self.lock.release()
    
    def reset(self):
        self.outputs = []
        self.errors = []
    
    def validate(self):
        for i, code in enumerate(self.return_codes):
            if code != 0:
                return self.outputs[i], self.errors[i]
        return None
    
class RatisCluster:
    def __init__(self, config, network) -> None:
        self.nodes = config.nodes
        self.config = config
        self.client_request_counter = 0
        self.network = network
        self.peers_param = ','.join([f'{i}:127.0.0.1:{6000+i}' for i in range(1, self.nodes + 1)])
        self.servers = [RatisServer(i, 6000+i, self.peers_param) for i in range(1, self.nodes + 1)]
        self.client = RatisClient(self.peers_param)

        print('RatisCluster created.')
    
    def send_client_request(self):
        self.client.send_request(self.client_request_counter, 0)
        self.client_request_counter += 1

    def validate(self):
        # Client validate
        client_val = self.client.validate()
        if client_val is not None:
            print('!!!!!! Client process received error !!!!!!')
            return ErrorType.PROCESS_ERROR, client_val
        
        # Server validate

        # Leader validate
        leader_count = 0
        for server in self.servers:
            leader_count += 1 if server.state == ServerState.LEADER else 0
        if leader_count > 1:
            return ErrorType.MULTIPLE_LEADERS, None

        return None
    
    def start(self, verbose=False):
        print('RatisCluster starting.')
        self.network.run()
        for server in self.servers:
            server.start(verbose)

    def reset(self):
        self.client_request_counter = 0
        self.client.reset()
        self.network.reset()

    def shutdown(self, id=None):
        if id is not None:
            for server in self.servers:
                self.network.send_shutdown(server.id)
            for server in self.servers:
                server.shutdown()
            # subprocess.run(['sh', 'stop-all.sh'], shell=True)
            self.network.shutdown()
            print('RatisCluster shutting down.')
        else:
            self.network.send_shutdown(id)
            self.servers[id-1].shutdown()
            print('Server {id} shutdown.')
    