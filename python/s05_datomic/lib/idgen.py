import threading
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from node import Node

class IDGen:
    def __init__(self, node: "Node") -> None:
        self.node = node
        self.lock = threading.Lock()
        self.i: int = -1

    def next(self) -> str:
        with self.lock:
            self.i += 1
            return f"{self.node.node_id}-{self.i}"