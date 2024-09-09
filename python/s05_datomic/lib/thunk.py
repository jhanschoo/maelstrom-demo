import time
from typing import Any, Callable, Optional, cast
from .node import Node
from .rpcerror import RPCError

class Thunk[T]:
    SVC = "lww-kv"
    UNPOPULATED = object()
    cache = {}

    def __init__(self, node: Node, id: str, val: Optional[T]) -> None:
        """
        Initializes a Thunk object.

        Thunks objects in this distributed system falls under one of two types:
        1. Immutable writer: The Thunk is created with an id (at time of creation), and a value, and this k-v pair is persisted using SVC when `.save()` is called. This k-v pair is immutable, and if multiple Thunks are created with the same id, the value of the Thunk must be the same.
        2. Immutable reader: The Thunk is created with a seen id, and may have been created with a value. At time of creation, this k-v pair is already persisted in the SVC, and k-v pairing is immutable.
        Args:
            node (Node): The node associated with the Thunk.
            id (str): The ID of the Thunk. Should be unique across all Thunks in the distributed system.
            val (Optional[T], optional): The value of the Thunk. Defaults to None.
        """

        self.node = node
        self._id: str = id
        self._val: T = val if val is not None else cast(T, self.UNPOPULATED)
        self.final: bool = False

    @staticmethod
    def obtain[U](id: str, create_if_miss: Callable[[], U]) -> U:
        if id not in Thunk.cache:
            Thunk.cache[id] = create_if_miss()
        return Thunk.cache[id]

    def _svc_serialize_val(self, val: T) -> Any:
        return val
    
    def _svc_deserialize_val(self, val: Any) -> Optional[T]:
        return val

    @property
    def id(self) -> str:
        return self._id

    @property
    def val(self) -> T:
        if self._val is not self.UNPOPULATED:
            return self._val
        if not self.final:
            read_res = None
            while read_res != "read_ok":
                res = self.node.sync_rpc(self.SVC, {
                    "type": "read",
                    "key": self.id
                })["body"]
                read_res = res["type"]
                if read_res != "read_ok":
                    self.node.log(f"Failed to read thunk {self.id}, retrying...")
                    time.sleep(0.01)
            val = self._svc_deserialize_val(res["value"])
            if val is None:
                raise RPCError.abort(f"Failed to read thunk {self.id}")
            self._val = val
            self.final = True
        return cast(T, self._val)

    def save(self) -> None:
        if self._val is self.UNPOPULATED:
            """If `self.val` is None, then the Thunk is a reader, and should not be saved."""
            return
        if self.final:
            return
        res = self.node.sync_rpc(self.SVC, {
            "type": "write",
            "key": self.id,
            "value": self._svc_serialize_val(self._val)
        })
        if res["body"]["type"] == "write_ok":
            self.final = True
        else:
            raise RPCError.abort(f"Failed to save thunk {self.id}")
