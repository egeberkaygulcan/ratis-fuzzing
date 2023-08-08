import os
import math
import time
import shutil
import logging
import subprocess
import threading

from enum import Enum
from network import Network

class ErrorType(Enum):
    CLIENT_ERROR=0
    SERVER_ERROR=1
    MULTIPLE_LEADERS=2

class ServerState(Enum):
    FOLLOWER=0
    CANDITATE=1
    LEADER=2
    SHUTDOWN=3

class RatisServer:
    def __init__(self, id, port, peers, network) -> None:
        self.id = id
        self.ratis_client_port = 7074 + self.id
        self.port = port
        self.peers = peers
        self.process = None
        self.term = 0
        self.commit_index = 0
        self.state = ServerState.FOLLOWER
        self.timer = None
        self.state_lock = threading.Lock()
        self.network = network
        self.timeout_ = False
        
        logging.info(f'Server {self.id} created.')

    def start(self, filepath, timeout=None):
        cmd = f'./start_server.sh {self.id} {self.peers} {self.ratis_client_port}'

        f = open(os.path.join(filepath, f'server_{self.id}.txt'), 'w+')
        self.process = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdout=f, stderr=f)
        self.timer = threading.Timer(float(timeout), self.timeout)
        self.timer.start()
        logging.info(f'Server {self.id} started.')

    def timeout(self):
        self.return_code = -1
        self.process.kill()
        self.timeout_ = True
        logging.error(f'Server {self.id} timed out.')
        
    def shutdown(self):
        #self.return_code = self.process.returncode
        self.timer.cancel()
        self.process.kill()
        logging.info(f'Server {self.id} shut down.')

    def update_state(self, new_state):
        try:
            self.state_lock.acquire()
            if new_state == 'follower':
                self.state = ServerState.FOLLOWER
            elif new_state == 'candidate':
                self.state = ServerState.CANDITATE
            elif new_state == 'leader':
                self.state = ServerState.LEADER
                # self.network.send_leader_id(str(self.id))
            else:
                self.state = ServerState.FOLLOWER
        finally:
            self.state_lock.release()
        logging.debug(f'Server {self.id} changed its state to {self.state}')

    def handle_event(self, event):
        type = event['type']
        if type == 'state_change':
            self.update_state(event['new_state'])

    
    def validate(self):
        return None

class RatisClient:
    def __init__(self, peers) -> None:
        self.peers = peers
        self.errors = []
        # self.expected_output = []
        # self.isAssign = True
        self.threads = []
        self.lock = threading.Lock()
        self.name_counter = 0
        self.completed_requests = 0
        self.pending_requests = 0
        logging.info('RatisClient created.')
    
    def send_request(self, value, filepath, leader_id, timeout=None):
        def run(cmd, timeout, filepath, on_exit):
            out, err = False, False
            try:
                f = open(os.path.join(filepath, f'client.txt'), 'a+')
                subprocess.run(cmd, shell=True, universal_newlines=True, timeout=timeout, check=True, stdout=f, stderr=f)
            except subprocess.CalledProcessError as e:
                err = True
            except subprocess.TimeoutExpired as e:
                err = True
            on_exit(err)
            return
        
        name = str(self.name_counter)
        # if self.isAssign:
        #     cmd = f'./client_assign.sh {name} {value} {self.peers}'
        #     self.expected_output.append('Success: true')
        # else:
        #     cmd = f'./client_get.sh {name} {self.peers}'
        #     self.name_counter += 1
        #     self.expected_output.append(f'{name}={value}')
        # self.isAssign = not self.isAssign
        cmd = f'./client_assign.sh {name} {value} {self.peers}'
        thread = threading.Thread(target=run, args=(cmd, timeout, filepath, self.on_exit))
        thread.start()
        self.threads.append(thread)
        self.pending_requests += 1
        logging.info('Client request sent.')
        # logging.info(f'Client request sent: {"assign" if not self.isAssign else "get"}, {name}')

    def on_exit(self, err):
        self.lock.acquire()
        self.errors.append(err)
        self.completed_requests += 1
        self.lock.release()
    
    def reset(self):
        self.req_count = 0
        self.errors = []

    def timeout(self):
        for thread in self.threads:
            thread.join()
    
    def validate(self):
        return None
    
class RatisCluster:
    def __init__(self, config, network) -> None:
        self.nodes = config.nodes
        self.config = config
        self.client_request_counter = 0
        self.network = network
        self.peers_param = ','.join([f'{i}:127.0.0.1:{6000+i}' for i in range(1, self.nodes+1)])
        self.servers = [RatisServer(i, 6000+i, self.peers_param, self.network) for i in range(1, self.nodes+1)]
        self.client = RatisClient(self.peers_param)
        self.average_run_time = 0
        self.start_time = None
        self.end_time = None
        self.run_id = -1

        logging.info('RatisCluster created.')
    
    def send_client_request(self):
        path = os.path.join(self.config.parent_dir, '_'.join([self.config.exp_name, str(self.run_id)]))
        timeout = 15 if self.average_run_time == 0 else int(math.ceil(self.average_run_time)) + 5
        # TODO - Add write-write-read loop
        self.client.send_request(self.client_request_counter, path, self.get_leader(), timeout)
        self.client_request_counter += 1

    def validate(self):
        return None
    
    def start(self, run_id):
        logging.info('RatisCluster starting.')

        self.run_id = run_id
        path = os.path.join(self.config.parent_dir, '_'.join([self.config.exp_name, str(self.run_id)]))
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

        timeout = 15 if self.average_run_time == 0 else int(math.ceil(self.average_run_time)) + 5
        self.start_time = time.time()
        self.network.run()
        for server in self.servers:
            self.network.set_event_callback(str(server.id), server.handle_event)
        for server in self.servers:
            server.start(path, timeout)

    def reset(self):
        self.client_request_counter = 0
        self.start_time = None
        self.end_time = None
        self.client.reset()
        self.network.reset()

    def shutdown(self):
        for server in self.servers:
            self.network.send_shutdown(str(server.id))
        time.sleep(0.05)
        for server in self.servers:
            server.shutdown()
        
        self.network.shutdown()
        # self.reset()
        cmd = './stop-all.sh'
        subprocess.run(cmd, shell=True)
        logging.info('RatisCluster shut down.')

    def crash(self, id):
        self.network.send_crash(str(id))
        logging.info(f'Server {id} crashed.')
    
    def restart(self, id):
        self.network.send_restart(str(id))
        logging.info(f'Server {id} restarted.')
    
    def get_leader(self):
        for server in self.servers:
            if server.state == ServerState.LEADER:
                return server.id
        return None
    
    def schedule_leader(self):
        leader = self.get_leader()
        if leader is not None:
            self.network.schedule_replica(str(leader))
        else:
            logging.error('Leader not found')
    
    def check_timeout(self):
        for server in self.servers:
            if server.timeout_:
                self.client.timeout()
                return True
        return False

    def get_completed_requests(self):
        return self.client.completed_requests
    
    def get_pending_requests(self):
        return self.client.pending_requests
    
    def get_node_ids(self):
        return [server.id for server in self.servers]
        
