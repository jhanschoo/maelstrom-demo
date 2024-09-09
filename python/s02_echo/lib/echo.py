#!/home/jhanschoo/.local/share/hatch/env/virtual/maelstrom-demo/TDl_d-w4/maelstrom-demo/bin/python

from copy import deepcopy
import json
import sys

class EchoServer:
    def __init__(self) -> None:
        self.node_id = None
        self.next_msg_id = 0

    def main(self) -> None:
        for line in sys.stdin:
            req = json.loads(line)
            print(f"Received {line}", file=sys.stderr, end=None)

            body = req["body"]
            type = body["type"]
            if type == "init":
                self.node_id = body["node_id"]
                print(f"Initialized node {self.node_id}", file=sys.stderr, end=None)
                self.reply(req, {"type": "init_ok"})
            if type == "echo":
                print(f"Echoing {body}", file=sys.stderr, end=None)
                body["type"] = "echo_ok"
                self.reply(req, body)

    def reply(self, req: dict, body: dict) -> None:
        self.next_msg_id += 1
        body = deepcopy(body)
        body["in_reply_to"] = req["body"]["msg_id"]
        body["msg_id"] = self.next_msg_id
        res = {
            "src": self.node_id,
            "dest": req["src"],
            "body": body
        }
        json.dump(res, sys.stdout)
        print(flush=True)
