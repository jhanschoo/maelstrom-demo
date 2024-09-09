import threading
from typing import Optional
from .node import Node

class GSet:
    def __init__(self, s: Optional[set] = None) -> None:
        self.s = s if s is not None else set()

    @staticmethod
    def from_list(l: list) -> "GSet":
        return GSet(set(l))

    def to_list(self) -> list:
        return list(self.s)

    def read(self) -> list:
        return list(self.s)

    def merge(self, other: "GSet") -> "GSet":
        return GSet(self.s.union(other.s))

    def add(self, e) -> "GSet":
        return GSet(self.s.union({e}))

class GSetServer:
    def __init__(self) -> None:
        self.node = Node()
        self.lock = threading.Lock()
        self.crdt = GSet()

        def handle_read(req: dict) -> None:
            with self.lock:
                self.node.reply(req, {
                    "type": "read_ok",
                    "value": self.crdt.read()
                })
        self.node.on("read", handle_read)

        def handle_add(req: dict) -> None:
            e = req["body"]["element"]
            with self.lock:
                self.crdt = self.crdt.add(e)
            self.node.reply(req, {"type": "add_ok"})
        self.node.on("add", handle_add)

        def handle_replicate(req: dict) -> None:
            other = GSet.from_list(req["body"]["value"])
            with self.lock:
                self.crdt = self.crdt.merge(other)
        self.node.on("replicate", handle_replicate)

        def replicate() -> None:
            with self.lock:
                v = list(self.crdt.to_list())
            self.node.log(f"Replicating {v}")
            for n in self.node.node_ids:
                if n == self.node.node_id:
                    continue
                self.node.send(n, {
                    "type": "replicate",
                    "value": v
                })
        self.node.every(5, replicate)

if __name__ == "__main__":
    GSetServer().node.main()