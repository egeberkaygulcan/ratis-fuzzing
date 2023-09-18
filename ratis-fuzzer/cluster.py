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
from itertools import cycle

import psutil
from network import Network

class RatisCluster:
    def __init__(self, config, network) -> None:
        self.nodes = config.nodes
        self.config = config
        self.network = network
        self.timeout = 30
        self.ports = cycle([7080, 7081, 7082, 7083, 7084, 7085])
        self.current_port = next(self.ports)
        self.thread = None

        self.client_request_counter = 0
        self.average_run_time = 0
        self.start_time = 0
        self.run_id = -1
        self.error_flag = False
        self.error_log = None

        logging.debug('RatisCluster created.')
    
    def reset(self):
        self.error_flag = False
        self.error_log = None
        self.client_request_counter = 0
        self.run_id = -1
        self.thread.join()
        self.thread = None
        self.current_port = next(self.ports)
        self.network.reset(self.current_port)
        if os.path.exists('./data'):
            shutil.rmtree('./data')
    
    def shutdown(self):
        self.network.shutdown()
        if os.path.exists('./data'):
            shutil.rmtree('./data')
        logging.info('Cluster shutdown.')

    def start(self, run_id):
        logging.debug('RatisCluster starting.')
        self.network.run()
        self.start_process()

    def start_process(self):
        def run(cmd, timeout):
            try:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True, timeout=timeout)
                logging.debug(result.stdout)
            except subprocess.CalledProcessError as e:
                logging.error('CalledProcessError.')
                self.error_flag = True
                logging.error(e.stderr)
                logging.error(e.stdout)
                self.error_log = (e.stderr, e.stdout)
                self.network.cluster_error = True
                # TODO - Record error
            except subprocess.TimeoutExpired as e:
                logging.error('TimeoutExpired.')
                self.error_flag = True
                logging.error(e.stderr)
                logging.error(e.stdout)
                self.network.cluster_error = True
                # TODO - Handle timeout
            finally:
                kill_cmd = "pkill -f 'java -cp'"
                subprocess.run(kill_cmd, shell=True)
            return

        cmd = f'java -ea -cp {self.config.jar_path} org.apache.ratis.examples.counter.server.CounterServer {self.config.nodes} {self.current_port}'
        self.thread = threading.Thread(target=run, args=(cmd, self.timeout))
        self.thread.start()
        logging.debug('Cluster started.')

    # def get_pid(self):
    #     connections = psutil.net_connections()
    #     port = 7080
    #     for con in connections:
    #         if con.raddr != tuple():
    #             if con.raddr.port == port:
    #                 return con.pid, con.status
    #         if con.laddr != tuple():
    #             if con.laddr.port == port:
    #                 return con.pid, con.status

    def get_completed_requests(self):
        return self.client.completed_requests
    
    def get_pending_requests(self):
        return self.client.pending_requests
