from copy import copy
from thunk import Thunk
from typing import TYPE_CHECKING, Optional, override
if TYPE_CHECKING:
    from node import Node
    from idgen import IDGen

class AMap[T](Thunk[dict[str, Thunk[list[T]]]]):

    def __init__(self, node: "Node", idgen: "IDGen", id: str, val) -> None:
        super().__init__(node, id, val)
        self.idgen = idgen
        self.default_value = []

    @override
    def _svc_serialize_val(self, val: dict[str, Thunk[list[T]]]):
        return [ (k, thunk.id) for k, thunk in val.items() ]

    @override
    def _svc_deserialize_val(self, val: Optional[list[tuple[str, str]]]) -> Optional[dict[str, Thunk[list[T]]]]:
        if val is None:
            return None
        return {
            k: Thunk[list[T]].obtain(id, lambda: Thunk[list[T]](self.node, id, None))
            for k, id in val
        }

    @override
    def save(self) -> None:
        if self._val is None:
            """If `self.val` is None, then the Thunk is a reader, and should not be saved."""
            return
        for thunk in self._val.values():
            thunk.save()
        super().save()

    def __getitem__(self, k):
        thunk = self.val.get(k)
        if thunk:
            return thunk.val
        return self.default_value

    def assoc(self, k: str, v: list[T]) -> "AMap[T]":
        thunk = Thunk(self.node, self.idgen.next(), v)
        val = copy(self.val)
        val[k] = thunk
        return AMap(self.node, self.idgen, self.idgen.next(), val)

    def transact(self, txns: list[tuple[str, str, list[T]]]) -> list:
        ret: list[tuple[str, str, list[T]]] = []
        # alias self to that, for readability, so that we write
        # "that = ..." rather than "self = ..."
        that: "AMap[T]" = self
        for txn in txns:
            f, k, v = txn
            if f == "r":
                ret.append((f, k, that[k]))
            elif f == "append":
                ret.append(txn)

                l = that[k] + [v]

                that = that.assoc(k, l)
        return [that, ret]
