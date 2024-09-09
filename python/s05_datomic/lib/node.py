from copy import deepcopy
import threading
import json
import sys
import time
from typing import Callable

from promise import Promise
from rpcerror import RPCError

def excepthook(exctype, value, tb):
    print(f"Uncaught exception: {value}", file=sys.stderr, flush=True)
    raise value

#threading.excepthook = excepthook

class Node:
    def __init__(self) -> None:
        self.node_id = None
        self.node_ids = []
        self.next_msg_id = 0

        self.handlers = {}
        self.callbacks = {}
        self.periodic_tasks = []

        self.lock = threading.RLock()
        self.log_lock = threading.Lock()

        def handle_init(req: dict) -> None:
            self.node_id = req["body"]["node_id"]
            self.node_ids = req["body"]["node_ids"]

            self.reply(req, { "type": "init_ok" })
            self.log(f"Node {self.node_id} initialized")

            self.start_periodic_tasks()
        self.on("init", handle_init)

    def main(self) -> None:
        for line in sys.stdin:
            req = json.loads(line)
            self.log(f"Received {line}")

            with self.lock:
                handler = None
                if "in_reply_to" in req["body"]:
                    if req["body"]["in_reply_to"] in self.callbacks:
                        handler = self.callbacks[req["body"]["in_reply_to"]]
                        del self.callbacks[req["body"]["in_reply_to"]]
                        self.log(f"Deleted callback for {req}")
                    else:
                        self.log(f"No callback for {req}")
                else:
                    handler = self.handlers.get(req["body"]["type"])
                if not handler:
                    raise ValueError(f"No handler for {req['body']['type']}")

            def target():
                try:
                    handler(req) # type: ignore
                except Exception as e:
                    self.log(f"Error handling {req}: {e}")
                    self.reply(req, RPCError.crash(str(e)).as_dict())
            threading.Thread(target=target).start()

    def every(self, ds: int, callback: Callable) -> None:
        self.periodic_tasks.append((ds, callback))

    def start_periodic_tasks(self) -> None:
        for ds, callback in self.periodic_tasks:
            def target():
                while True:
                    callback()
                    time.sleep(ds)
            threading.Thread(target=target).start()

    # Register a handler for a message type
    def on(self, type: str, handler: Callable) -> None:
        if type in self.handlers:
            raise ValueError(f"Handler for {type} already registered.")
        self.handlers[type] = handler

    def rpc(self, dest: str, body: dict, callback: Callable) -> None:
        with self.lock:
            msg_id = self.next_msg_id + 1
            self.callbacks[msg_id] = callback
            body = deepcopy(body)
            body.update({ "msg_id": msg_id })
            self.send(dest, body)

    def sync_rpc(self, dest: str, body: dict):
        promise = Promise()
        self.rpc(dest, body, lambda res: promise.resolve(res))
        return promise.wait()

    def send(self, dest: str, body: dict) -> None:
        msg = {
            "src": self.node_id,
            "dest": dest,
            "body": body
        }
        with self.lock:
            self.log(f"Sending {msg}")
            json.dump(msg, sys.stdout)
            print(flush=True)

    def reply(self, req: dict, body: dict) -> None:
        body = deepcopy(body)
        body["in_reply_to"] = req["body"].get("msg_id") or req["id"]
        self.send(req["src"], body)

    def log(self, msg: str) -> None:
        with self.log_lock:
            print(msg, file=sys.stderr, end=None, flush=True)
