import os
import math
import json
import time
import base64
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
    COMMIT_INDEX_ERROR=3
    TERM_ERROR=4
    ENTRY_ERROR = 5
    ENTRY_LEN_ERROR = 5

class ServerState(Enum):
    FOLLOWER=0
    CANDITATE=1
    LEADER=2
    SHUTDOWN=3

class RatisServer:
    def __init__(self, id, port, peers, config) -> None:
        self.id = id
        self.ratis_client_port = 7074 + self.id
        self.port = port
        self.peers = peers
        self.config = config
        self.state_lock = threading.Lock()
        self.process = None
        self.timer = None
        self.timeout_ = False
        self.dir_ = ''

        self.term = 0
        self.entries = []
        self.commit_index = 0
        self.state = ServerState.FOLLOWER
        
        logging.info(f'Server {self.id} created.')
    
    def reset(self):
        self.term = 0
        self.commit_index = 0
        self.entries = []
        self.state = ServerState.FOLLOWER

        self.process = None
        self.timer = None
        self.timeout_ = False
        self.dir_ = ''

    def start(self, timeout=None):
        cmd = f'./start_server.sh {self.id} {self.peers} {self.ratis_client_port}'

        f_out = open(os.path.join(os.path.join(self.config.tmp_dir, self.dir_), f'server_{self.id}_stdout.log'), 'w+')
        self.process = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdout=f_out, stderr=f_out)
        self.timer = threading.Timer(float(timeout), self.timeout)
        self.timer.start()
        logging.info(f'Server {self.id} started.')

    def timeout(self):
        self.return_code = -1
        if self.process is not None:
            self.process.kill()
        self.timeout_ = True
        logging.error(f'Server {self.id} timed out.')
        
    def shutdown(self):
        self.timer.cancel()
        if self.process is not None:
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
            else:
                self.state = ServerState.FOLLOWER
        finally:
            self.state_lock.release()
        logging.debug(f'Server {self.id} changed its state to {self.state}')
    
    def update_commit(self, new_commit_index):
        self.commit_index = new_commit_index
    
    def update_term(self, new_term):
        self.term = new_term
    
    def update_log(self, new_entries):
        for key in new_entries.keys():
            new_entry = new_entries[key]['data']
            if new_entry not in self.entries:
                self.entries.append(new_entry)

    def handle_event(self, event):
        type = event['type']
        if type == 'state_change':
            self.update_state(event['new_state'])
        elif type == 'commit_update':
            self.update_state(event['commit_index'])
        elif type == 'log_update':
            self.update_log(event['entries'])
        elif type == 'term_update':
            self.update_term(event['entries'])

    def set_dir_(self, dir_):
        self.dir_ = dir_

class RatisClient:
    def __init__(self, peers, config) -> None:
        self.peers = peers
        # self.expected_output = []
        # self.isAssign = True
        self.lock = threading.Lock()
        self.config = config

        self.dir_ = ''
        self.timeout_ = False
        self.name_counter = 0
        self.completed_requests = 0
        self.pending_requests = 0
        self.errors = []
        self.threads = []

        logging.info('RatisClient created.')
    
    def reset(self):
        self.dir_ = ''
        self.timeout_ = False
        self.name_counter = 0
        self.completed_requests = 0
        self.pending_requests = 0
        self.errors = []
        self.threads = []
    
    def send_request(self, value, timeout=None):
        def run(cmd, timeout, filepath, on_exit):
            out, err = False, False
            try:
                f_out = open(os.path.join(filepath, 'client_stdout.log'), 'a+')
                subprocess.run(cmd, shell=True, universal_newlines=True, timeout=timeout, check=True, stdout=f_out, stderr=f_out)
            except subprocess.CalledProcessError as e:
                err = True
            except subprocess.TimeoutExpired as e:
                err = True
            on_exit(err)
            return
        
        name = str(self.name_counter)
        cmd = f'./client_assign.sh {name} {value} {self.peers}'
        thread = threading.Thread(target=run, args=(cmd, timeout, os.path.join(self.config.tmp_dir, self.dir_), self.on_exit))
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

    def timeout(self):
        self.timeout_ = True
        for thread in self.threads:
            thread.join()
    
    def validate(self):
        if not os.path.exists(os.path.join(os.path.join(self.config.tmp_dir, self.dir_), 'client_stdout.log')):
            return None
        lines = []
        with open(os.path.join(os.path.join(self.config.tmp_dir, self.dir_), 'client_stdout.log'), 'rb') as f:
            lines = f.readlines()
        lines = [line.decode('utf-8').strip() for line in lines]
        # print(lines)
        success = sum([1 if line == 'Success: true' else 0 for line in lines])
        err = False
        if success != self.pending_requests:
            logging.error('Succesful request mismatch on client validation.')
            err = True
        
        if self.pending_requests != self.completed_requests:
            logging.error('Pending requests do not match with completed requests.')
            err = True

        # if self.timeout_:
        #     logging.error('Client timeout flag set.')
        #     err = True

        if  err:
            src = os.path.join(self.config.tmp_dir, self.dir_)
            dst = os.path.join(self.config.out_dir, self.dir_)
            if not os.path.exists(self.config.out_dir):
                os.makedirs(self.config.out_dir)
            shutil.move(src, dst)
            return ErrorType.CLIENT_ERROR

        return None

    def set_dir_(self, dir_):
        self.dir_ = dir_
    
class RatisCluster:
    def __init__(self, config, network) -> None:
        self.nodes = config.nodes
        self.config = config
        self.network = network
        self.peers_param = ','.join([f'{i}:127.0.0.1:{6000+i}' for i in range(1, self.nodes+1)])
        self.servers = [RatisServer(i, 6000+i, self.peers_param, self.config) for i in range(1, self.nodes+1)]
        self.client = RatisClient(self.peers_param, self.config)

        self.client_request_counter = 0
        self.average_run_time = 0
        self.start_time = 0
        self.run_id = -1
        self.val_flag = False

        logging.info('RatisCluster created.')
    
    def reset(self):
        if not self.check_timeout():
            self.average_run_time += time.time_ns() - self.start_time

        self.cleanup()

        self.client_request_counter = 0
        self.start_time = 0
        self.run_id = -1
        self.val_flag = False

        self.client.reset()
        for server in self.servers:
            server.reset()
        self.network.reset()
    
    def cleanup(self):
        path = os.path.join(self.config.tmp_dir, '_'.join([self.config.exp_name, str(self.run_id)]))
        if os.path.exists(path):
            shutil.rmtree(path)
    
    def shutdown(self):
        self.network.shutdown()

    def start(self, run_id):
        logging.info('RatisCluster starting.')

        self.run_id = run_id
        path = os.path.join(self.config.tmp_dir, '_'.join([self.config.exp_name, str(self.run_id)]))
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

        timeout = 30 if self.average_run_time == 0 else int(math.ceil(self.average_run_time/1e9)) + 10
        self.start_time = time.time_ns()
        self.network.run()
        for server in self.servers:
            self.network.set_event_callback(str(server.id), server.handle_event)
        for server in self.servers:
            server.set_dir_('_'.join([self.config.exp_name, str(self.run_id)]))
            server.start(timeout)
    
    def send_client_request(self):
        self.client.set_dir_('_'.join([self.config.exp_name, str(self.run_id)]))
        timeout = 30 if self.average_run_time == 0 else int(math.ceil(self.average_run_time/1e9)) + 10
        self.client.send_request(self.client_request_counter, timeout)
        self.client_request_counter += 1
    
    def write_server_error(self, content, lines=False):
        os.makedirs(os.path.join(self.config.out_dir, '_'.join([self.config.exp_name, str(self.run_id)])), exist_ok=True)
        with open(os.path.join(os.path.join(self.config.out_dir, '_'.join([self.config.exp_name, str(self.run_id)])), 'server_errors.log'), 'a+') as f:
            if not lines:
                f.write(content)
            else:
                f.writelines(content)

    def validate(self):
        errs = []
        client_val = self.client.validate()
        if client_val is not None:
            errs.append(client_val)
        
        # Multiple leader check
        leader_count = 0
        for server in self.servers:
            if server.state == ServerState.LEADER:
                leader_count += 1
        if leader_count > 1:
            logging.error('Multiple leaders present.')
            errs.append(ErrorType.MULTIPLE_LEADERS)
            for server in self.servers:
                self.write_server_error(f'{server.id} , {server.state}\n')
            self.write_server_error('------------------------------/n')


        # Commit index check
        leader_commit = 0
        leader_id = 0
        max_commit = 0
        for server in self.servers:
            if server.state == ServerState.LEADER:
                leader_commit = server.commit_index
                if leader_commit > max_commit:
                    max_commit = leader_commit
                leader_id = server.id
            else:
                if server.commit_index > max_commit:
                    max_commit = server.commit_index
        if leader_id != 0:
            if max_commit > leader_commit:
                logging.error('Leader commit smaller than max commit')
                errs.append(ErrorType.COMMIT_INDEX_ERROR)
                for server in self.servers:
                    self.write_server_error(f'{server.id} , {server.state} , {server.commit_index}\n')
                self.write_server_error('------------------------------/n')

        # Term check
        leader_term = 0
        leader_id = 0
        max_term = 0
        for server in self.servers:
            if server.state == ServerState.LEADER:
                leader_term = server.term
                if leader_term > max_term:
                    max_term = leader_term
                leader_id = server.id
            else:
                if server.term > max_term:
                    max_term = server.term
        if leader_id != 0:
            if max_term > leader_term:
                logging.error('Leader term smaller than max term')
                errs.append(ErrorType.TERM_ERROR)
                for server in self.servers:
                    self.write_server_error(f'{server.id} , {server.state} , {server.term}\n')
                self.write_server_error('------------------------------/n')

        # Entry check
        leader_id = 0
        leader_entries = []
        entries_list = []
        for server in self.servers:
            if server.state == ServerState.LEADER:
                leader_id = server.id
                leader_entries = server.entries
            else:
                entries_list.append(server.entries)

        if leader_id != 0:
            for entries in entries_list:
                if len(entries) > len(leader_entries):
                    logging.error('Number of entries larger than leader.')
                    errs.append(ErrorType.ENTRY_LEN_ERROR)
                    for server in self.servers:
                        self.write_server_error(f'{server.id} , {server.state} , {len(server.entries)}\n')
                        self.write_server_error(server.entries, lines=True)
                        self.write_server_error('\n')
                    self.write_server_error('------------------------------/n')
                else:
                    for i, entry in enumerate(entries):
                        if entry != leader_entries[i]:
                            logging.error('Entries inconsistent.')
                            errs.append(ErrorType.ENTRY_ERROR)
                            for server in self.servers:
                                self.write_server_error(f'{server.id} , {server.state}\n')
                                self.write_server_error(server.entries, lines=True)
                                self.write_server_error('\n')
                            self.write_server_error('------------------------------/n')
        else:
            max_entries = []
            for entries in entries_list:
                if len(entries) > len(max_entries):
                    max_entries = entries
            
            for entries in entries_list:
                for i, entry in enumerate(entries):
                    if entry != max_entries[i]:
                        logging.error('Entries inconsistent.')
                        errs.append(ErrorType.ENTRY_ERROR)
                        for server in self.servers:
                            self.write_server_error(f'{server.id} , {server.state}\n')
                            self.write_server_error(server.entries, lines=True)
                            self.write_server_error('\n')
                        self.write_server_error('------------------------------/n')
        
        if len(errs) > 0:
            logging.error('Cluster validation failed.')
            errs = [err.__str__()+'\n' for err in errs]
            with open(os.path.join(os.path.join(self.config.out_dir, '_'.join([self.config.exp_name, str(self.run_id)])), 'err_types.log'), 'w+') as f:
                f.writelines(errs)
        
        return len(errs) > 0

    def server_shutdown(self):
        for server in self.servers:
            self.network.send_shutdown(str(server.id))
        time.sleep(0.05)
        for server in self.servers:
            server.shutdown()
        
        cmd = './stop-all.sh'
        subprocess.run(cmd, shell=True)
        logging.info('RatisServers shut down.')

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
    
    def set_val_flag(self):
        self.val_flag = True
        
