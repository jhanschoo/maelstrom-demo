import threading
from typing import Optional, override
from .node import Node

class PNCounter:
    def __init__(self, pos: Optional["GCounter"] = None, neg: Optional["GCounter"] = None) -> None:
        self.pos = pos if pos is not None else GCounter()
        self.neg = neg if neg is not None else GCounter()

    @staticmethod
    def from_dict(d: dict) -> "PNCounter":
        return PNCounter(
            pos=GCounter.from_dict(d["pos"]),
            neg=GCounter.from_dict(d["neg"])
        )

    def as_dict(self) -> dict:
        return {
            "pos": self.pos.as_dict(),
            "neg": self.neg.as_dict()
        }

    def read(self) -> int:
        return self.pos.read() - self.neg.read()

    def merge(self, other: "PNCounter") -> "PNCounter":
        return PNCounter(
            pos=self.pos.merge(other.pos),
            neg=self.neg.merge(other.neg)
        )

    def add(self, k, delta: int = 1) -> "PNCounter":
        if delta >= 0:
            return PNCounter(pos=self.pos.add(k, delta), neg=self.neg)
        else:
            return PNCounter(pos=self.pos, neg=self.neg.add(k, -delta))

class GCounter:
    def __init__(self, c: Optional[dict] = None) -> None:
        self.c = c if c is not None else {}

    @staticmethod
    def from_dict(d: dict) -> "GCounter":
        return GCounter(d)

    def as_dict(self) -> dict:
        return self.c

    def read(self) -> int:
        return sum(self.c.values())

    def merge(self, other: "GCounter") -> "GCounter":
        keys = set(self.c.keys()).union(other.c.keys())
        c = {k: max(self.c.get(k, 0), other.c.get(k, 0)) for k in keys}
        return GCounter(c)

    def add(self, k, delta: int = 1) -> "GCounter":
        c = dict(self.c)
        c[k] = c.get(k, 0) + delta
        return GCounter(c)

class CounterServer:
    def __init__(self) -> None:
        self.node = Node()
        self.lock = threading.Lock()
        self.crdt = PNCounter()

        def handle_read(req: dict) -> None:
            with self.lock:
                self.node.reply(req, {
                    "type": "read_ok",
                    "value": self.crdt.read()
                })
        self.node.on("read", handle_read)

        def handle_add(req: dict) -> None:
            delta = req["body"]["delta"]
            with self.lock:
                self.crdt = self.crdt.add(self.node.node_id, delta)
            self.node.reply(req, {"type": "add_ok"})
        self.node.on("add", handle_add)

        def handle_replicate(req: dict) -> None:
            other = PNCounter.from_dict(req["body"]["value"])
            with self.lock:
                self.crdt = self.crdt.merge(other)
        self.node.on("replicate", handle_replicate)

        def replicate() -> None:
            with self.lock:
                v = self.crdt.as_dict()
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
    CounterServer().node.main()