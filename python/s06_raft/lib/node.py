from copy import deepcopy
import threading
import json
import sys
import time
from typing import Callable

from .promise import Promise
from .rpcerror import RPCError

def excepthook(exctype, value, tb):
    print(f"Uncaught exception: {value}", file=sys.stderr, flush=True)
    raise value

#threading.excepthook = excepthook

class Node:
    """
    Node class for handling RPCs and periodic tasks.

    Initialization:
    - register "init" RPC handler

    Main loop:
    - read from stdin
    - determine handler for the message type
        - call registered initial handler for standard messages
        - call registered callback for replies

    "init" RPC handler:
    - at this point when the "init" message is received, all required periodic task handlers should have been registered
    - spawns a thread to execute each periodic task handler

    """
    def __init__(self) -> None:
        self.node_id = None
        self.node_ids = []
        self.next_msg_id = 1

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

    @property
    def other_node_ids(self) -> list:
        return [node_id for node_id in self.node_ids if node_id != self.node_id]

    def main(self) -> None:
        for line in sys.stdin:
            req = json.loads(line)
            self.log(f"Received {line}")

            handler = None
            with self.lock:
                if "in_reply_to" in req["body"]:
                    in_reply_to = req["body"]["in_reply_to"]
                    if in_reply_to in self.callbacks:
                        handler = self.callbacks[in_reply_to]
                        del self.callbacks[in_reply_to]
                        self.log(f"Deleted callback for {req}")
                    else:
                        self.log(f"No callback for {req}")
                else:
                    handler = self.handlers.get(req["body"]["type"])
            if handler is None:
                self.log(f"No handler for {req['body']['type']}")
                return

            def target():
                try:
                    handler(req)
                except Exception as e:
                    self.log(f"Error handling {req}: {e}")
                    # self.reply(req, RPCError.crash(str(e)).as_dict())
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
            msg_id = self.next_msg_id
            self.next_msg_id += 1
            self.callbacks[msg_id] = callback
            body = deepcopy(body)
            body.update({ "msg_id": msg_id })
            self.send(dest, body)

    def brpc(self, body: dict, handler: Callable) -> None:
        for node_id in self.other_node_ids:
            self.rpc(node_id, body, handler)

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

if __name__ == "__main__":
    Node().main()
