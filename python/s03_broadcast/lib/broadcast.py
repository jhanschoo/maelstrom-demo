import threading
import time
from .node import Node

# Broadcast wraps around a Node using composition over inheritance
class Broadcast:
    def __init__(self) -> None:
        self.node = Node()
        self.neighbors = []
        self.lock = threading.Lock()
        self.messages = set()

        def handle_topology(req: dict) -> None:
            self.neighbors = req["body"]["topology"][self.node.node_id]
            self.node.log(f"My neighbors are {self.neighbors}")
            self.node.reply(req, {"type": "topology_ok"})
        self.node.on("topology", handle_topology)

        def handle_read(req: dict) -> None:
            with self.lock:
                self.node.reply(req, {
                    "type": "read_ok",
                    "messages": list(self.messages)
                })
        self.node.on("read", handle_read)

        def handle_broadcast(req: dict) -> None:
            self.node.reply(req, {"type": "broadcast_ok"})
            m = req["body"]["message"]
            # read-and-mutation operation of "live" object variable, so lock
            with self.lock:
                is_new_message = m not in self.messages
                if not is_new_message:
                    return
                self.messages.add(m)
            src = req["src"]
            # self.neighbors expected to be static at this point
            unacked = list(filter(lambda n: n != src, self.neighbors))
            while unacked:
                self.node.log(f"Need to replicate {req} to: {unacked}")
                for dest in unacked:
                    def callback(res):
                        self.node.log(f"Got response {res}")
                        if res["body"]["type"] == "broadcast_ok":
                            unacked.remove(res["src"])
                            self.node.log(f"Removed {res['src']}, remaining: {unacked}")
                    self.node.rpc(dest, {
                        "type": "broadcast",
                        "message": m
                    }, callback)
                time.sleep(1)
            self.node.log(f"Done with message {req}")

        self.node.on("broadcast", handle_broadcast)

if __name__ == "__main__":
    Broadcast().node.main()