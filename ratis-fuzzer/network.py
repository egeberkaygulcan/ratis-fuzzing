from http import HTTPStatus
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit, parse_qs
from threading import Lock, Thread
import requests
import json
import base64

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
        self.mailboxes = {}
        self.replicas = {}
        self.event_trace = []
        self.config = config

        router = Router()
        router.add_route("/replica", self._handle_replica)
        router.add_route("/message", self._handle_message)

        self.server = Server(addr, router)
        self.server_thread = Thread(target=self.server.serve_forever)

    def run(self):
        self.server_thread.start()
        print('Network started.')
    
    def shutdown(self):
        self.server.shutdown()
        self.server_thread.join()
        print('Network shutdown.')
    
    def reset(self):
        self.lock.acquire()
        self.mailboxes = {}
        self.replicas = {}
        self.event_trace = []
        self.lock.release()
    
    def check_mailbox(self, id):
        if id not in self.mailboxes.keys():
            return True
        return len(self.mailboxes[id]) == 0
    
    def _handle_replica(self, request: Request) -> Response:
        replica = json.loads(request.content)
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
        event = json.loads(request.content)
        if "replica" in event:
            try:
                e = {"name": event["type"], "params": event["params"]}
                e["params"]["replica"] = event["replica"]
                self.lock.acquire()
                self.event_trace.append(e)
            finally:
                self.lock.release()

        return Response.json(HTTPStatus.OK, json.dumps({"message": "Ok"}))
    
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
        try:
            self.lock.acquire()
            if str(replica) in self.mailboxes and len(self.mailboxes[str(replica)]) > 0:
                for m in self.mailboxes[str(replica)]:
                    messages_to_deliver.append(m)
                self.mailboxes[str(replica)] = []
                addr = self.replicas[replica]["addr"]
        finally:
            self.lock.release()

        for next_msg in messages_to_deliver:
            try:
                # next_msg.msg = None
                requests.post("http://"+addr+"/message", json=json.dumps(next_msg.__dict__))
            except Exception as e:
                print('COULD NOT SEND MESSAGE')
                print(e)

    def schedule_client_request(self):
        print('CLIENT REQUEST SENT')
        try:
            self.lock.acquire()
            addr = self.replicas[list(self.replicas.keys())[0]]['addr']
            msg = Message(0, 1, "client_request", 0)
            requests.post("http://"+addr+"/message", json=json.dumps(msg.__dict__)) 
        finally:
            self.lock.release()
    
    def schedule_crash(self, id):
        try:
            self.lock.acquire()
            addr = self.replicas[list(self.replicas.keys())[0]]['addr']
            msg = Message(0, 1, "crash", id)
            requests.post("http://"+addr+"/message", json=json.dumps(msg.__dict__))
        finally:
            self.lock.release()
    
    def schedule_restart(self, id):
        try:
            self.lock.acquire()
            addr = self.replicas[list(self.replicas.keys())[0]]['addr']
            msg = Message(0, 1, "restart", id)
            requests.post("http://"+addr+"/message", json=json.dumps(msg.__dict__))
        finally:
            self.lock.release()
    
    def send_exit(self):
        print('*-*-*-* Sending Exit *-*-*-*')
        try:
            self.lock.acquire()
            addr = self.replicas[list(self.replicas.keys())[0]]['addr']
            msg = Message(0, 1, "exit", 0)
            requests.post("http://"+addr+"/message", json=json.dumps(msg.__dict__))
        except Exception as e:
            print(e)
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