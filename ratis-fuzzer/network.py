from http import HTTPStatus
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit, parse_qs
from threading import Lock, Thread
import requests
import json
import base64
import traceback
import logging

class Request:
    def __init__(self, method, path, headers, query=None, content=None) -> None:
        self.path = path
        self.headers = headers
        self.query = query
        self.content = content
        self.method = method

class Response:
    def __init__(self, status_code, content) -> None:
        self.status_code = status_code
        self.headers = {}
        self.content = content

    def set_header(self, key, value):
        self.headers[key] = value

    def set_content_json(self):
        self.headers["Content-Type"] = "application/json"

    def json(status_code, content):
        r = Response(status_code, content)
        r.set_content_json()
        return r


class _ServerHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, handler = None, **kwargs) -> None:
        self.handler = handler
        super().__init__(*args, **kwargs)

    def respond(self, response: Response):
        if response is None:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Failed to generate a response")

        if response.status_code >= HTTPStatus.BAD_REQUEST:
            for key in response.headers:
                self.send_header(key, response.headers[key])
            self.send_error(response.status_code, response.content)
        else:
            self.send_response(response.status_code)
            for key in response.headers:
                self.send_header(key, response.headers[key])

            content = None
            if response.content is not None and len(response.content) > 0:
                content = response.content.encode("UTF-8", "replace")
            
            if content is None:
                self.send_header("Content-Length", 0)
                self.end_headers()
            else:
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

            return
        
    def log_message(self, format, *args):
        return

    def do_GET(self):
        (_, _, path, query, _) = urlsplit(self.path)
        parsed_qs = parse_qs(query)
        request = Request("GET", path, self.headers, parsed_qs)
        response = self.handler.handle(request)
        self.respond(response)

    def do_POST(self):
        (_, _, path, query, _) = urlsplit(self.path)
        parsed_qs = parse_qs(query)
        length = self.headers.get('content-length')
        content = ""
        if int(length) > 0:
            content = self.rfile.read(int(length))
        
        request = Request("POST", path, self.headers, parsed_qs, content)
        response = self.handler.handle(request)
        self.respond(response)


class Router:
    def __init__(self) -> None:
        self.handlers = {}
    
    def add_route(self, path, handler):
        self.handlers[path] = handler

    def handle(self, request):
        if request.path in self.handlers:
            response = self.handlers[request.path](request)
            return response
        return Response(HTTPStatus.NOT_FOUND, "Path does not exist")
    

class Server(ThreadingHTTPServer):
    def __init__(self, 
                 server_address, 
                 handler: Router, 
                 bind_and_activate: bool = True) -> None:
        self.handler = handler
        super().__init__(server_address, _ServerHandler, bind_and_activate)

    def finish_request(self, request, client_address) -> None:
        if self.RequestHandlerClass == _ServerHandler:
            return self.RequestHandlerClass(request, client_address, self, handler=self.handler)
        return super().finish_request(request, client_address)
    

class Message:
    def __init__(self, fr, to, type, msg, id=None) -> None:
        self.fr = fr
        self.to = to
        self.type = type
        self.msg = msg
        self.id = id

    def from_str(m):
        if "from" not in m or "to" not in m or "type" not in m or "data" not in m:
            return None
        return Message(m["from"], m["to"], m["type"], m["data"], m["id"] if "id" in m else None)

    def __str__(self) -> str:
        return f'fr: {self.fr}, to: {self.to}, type: {self.type}, msg: {self.msg}, id: {self.id}'


class Network:
    def __init__(self, addr, config) -> None:
        self.addr = addr
        self.lock = Lock()
        self.event_lock = Lock()
        self.mailboxes = {}
        self.replicas = {}
        self.event_callbacks = {}
        self.event_trace = []
        self.config = config

        router = Router()
        router.add_route("/replica", self._handle_replica)
        router.add_route("/message", self._handle_message)
        router.add_route("/event", self._handle_event)

        self.server = Server(addr, router)
        self.server_thread = Thread(target=self.server.serve_forever)

    def run(self):
        self.server_thread.start()
        logging.info('Network started.')
    
    def shutdown(self):
        self.server.shutdown()
        self.server_thread.join()
        logging.info('Network shutdown.')
    
    def reset(self):
        self.lock.acquire()
        self.mailboxes = {}
        self.replicas = {}
        self.event_trace = []
        self.event_callbacks = {}
        self.lock.release()
    
    def check_mailboxes(self):
        queued_mailboxes = []
        try:
            self.lock.acquire()
            for id in self.mailboxes.keys():
                if len(self.mailboxes[id]) != 0:
                    queued_mailboxes.append(id)
        finally:
            self.lock.release()
        return queued_mailboxes
    
    def check_replicas(self, num_nodes):
        result = True
        try:
            self.lock.acquire()
            if len(self.replicas) >= num_nodes:
                return False
        finally:
            self.lock.release()
        return result
    
    def _handle_replica(self, request: Request) -> Response:
        replica = json.loads(request.content)
        logging.info(f'Replica {replica["id"]} received.')
        if "id" in replica:
            try:
                self.lock.acquire()
                self.replicas[replica["id"]] = replica
            finally:
                self.lock.release()

        return Response.json(HTTPStatus.OK, json.dumps({"message": "Ok"}))

    def _handle_message(self, request: Request) -> Response:
        content = json.loads(request.content)
        content['data'] = json.loads(base64.b64decode(content["data"]).decode('utf-8'))
        msg = Message.from_str(content)
        logging.debug(f'Message received of type {msg.type} from {msg.fr} to {msg.to}.')
        if msg is not None:
            if msg.type == 'config_query':
                return Response.json(HTTPStatus.OK, json.dumps({'nodes': self.config.nodes}))
            try:
                self.lock.acquire()
                if msg.to not in self.mailboxes:
                    self.mailboxes[msg.to] = []
                self.mailboxes[msg.to].append(msg)
            finally:
                self.lock.release()

        return Response.json(HTTPStatus.OK, json.dumps({"message": "Ok"}))
    
    def _handle_event(self, request: Request) -> Response:
        logging.debug("Received event: {}".format(request.content))
        event = json.loads(request.content)
        if "server_id" in event:
            try:
                params = self._map_event_params(event)
                e = {"name": event["type"], "params": params}
                if params != None:
                    e["params"]["replica"] = event["replica"]
                    self.lock.acquire()
                    self.event_trace.append(e)
            finally:
                if self.lock.locked():
                    self.lock.release()

        return Response.json(HTTPStatus.OK, json.dumps({"message": "Ok"}))
    
    
    def _map_event_params(self, event):
        if event["type"] == "ClientRequest":
            return {
                "leader": int(event["params"]["leader"]),
                "request": self._get_request_number(event["params"]["request"])
            }
        elif event["type"] == "B":
            return {
                "node": int(event["params"]["node"]),
                "term": int(event["params"]["term"])
            }
        elif event["type"] == "Timeout":
            return {
                "node": int(event["params"]["node"])
            }
        elif event["type"] == "MembershipChange":
            return {
                "action": event["params"]["action"],
                "node": int(event["params"]["node"])
            }
        elif event["type"] == "UpdateSnapshot":
            return {
                "node": int(event["params"]["node"]),
                "snapshot_index": int(event["params"]["snapshot_index"]),
            }
        elif event["type"] == 'state_change':
            try:
                self.lock.acquire()
                self.event_callbacks[event['server_id']](event)
            finally:
                self.lock.release()
            return None
        else:
            -1
    
    def set_event_callback(self, server_id, callback):
        try:
            self.lock.acquire()
            self.event_callbacks[server_id] = callback
        finally:
            self.lock.release()
    
    def get_replicas(self):
        replicas = []
        try:
            self.lock.acquire()
            replicas = list(self.replicas.items())
        finally:
            self.lock.release()
        return replicas
    
    def get_event_trace(self):
        event_trace = []
        try:
            self.lock.acquire()
            for e in self.event_trace:
                event_trace.append(e)
        finally:
            self.lock.release()
        return event_trace
    
    def add_event(self, e):
        try:
            self.lock.acquire()
            self.event_trace.append(e)
        finally:
            self.lock.release()
    
    def schedule_replica(self, replica):
        addr = ""
        messages_to_deliver = []
        addr_list = []
        logging.debug(f'Scheduling replica {replica}')
        try:
            self.lock.acquire()
            for m in self.mailboxes[replica]:
                messages_to_deliver.append(m)
                if m.type == 'request_vote_request' or m.type == 'append_entries_response':
                    addr = self.replicas[m.to]["addr"]
                elif m.type == 'request_vote_response' or m.type == 'append_entries_request':
                    addr = self.replicas[m.fr]["addr"]
                else:
                    addr = self.replicas[replica]["addr"]
                addr_list.append(addr)
            self.mailboxes[str(replica)] = []
        finally:
            self.lock.release()
        for i, next_msg in enumerate(messages_to_deliver):
            try:
                requests.post("http://"+addr_list[i]+"/message", json=json.dumps(next_msg.__dict__))
            except:
                pass
                # logging.error(f'COULD NOT SEND MESSAGE')
                # store {replica} , {next_msg}
            finally:
                continue
    
    def send_shutdown(self, replica):
        logging.info(f'Sending shutdown to {replica}')
        try:
            self.lock.acquire()
            # addr = self.replicas[list(self.replicas.keys())[0]]['addr']
            addr = self.replicas[replica]["addr"]
            msg = Message(0, 1, "shutdown", 0)
            requests.post("http://"+addr+"/message", json=json.dumps(msg.__dict__))
        except:
            pass
        finally:
            self.lock.release()
    
    def send_crash(self, replica):
        # logging.info(f'Sending crash to {replica}')
        try:
            self.lock.acquire()
            addr = self.replicas[replica]["addr"]
            msg = Message(0, 1, "crash", 0)
            requests.post("http://"+addr+"/message", json=json.dumps(msg.__dict__))
        except:
            traceback.print_exc()
        finally:
            self.lock.release()
    
    def send_restart(self, replica):
        # logging.info(f'Sending restart to {replica}')
        try:
            self.lock.acquire()
            addr = self.replicas[replica]["addr"]
            msg = Message(0, 1, "restart", 0)
            requests.post("http://"+addr+"/message", json=json.dumps(msg.__dict__))
        except:
            traceback.print_exc()
        finally:
            self.lock.release()

    def clear_mailboxes(self):
        try:
            self.lock.acquire()
            for key in self.mailboxes:
                self.mailboxes[key] = []
            self.event_trace = []
        finally:
            self.lock.release()