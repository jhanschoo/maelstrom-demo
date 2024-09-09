from copy import copy

from .rpcerror import RPCError

class AMap:
    def __init__(self, m = {}) -> None:
        self.m = m

    def apply(self, instr: dict) -> tuple["AMap", dict]:
        k: str = instr["key"]
        if instr["type"] == "read":
            if k in self.m:
                return (self, { "type": "read_ok", "value": self.m[k] })
            # else fall through
        elif instr["type"] == "write":
            m = copy(self.m)
            m[k] = instr["value"]
            return (AMap(m), { "type": "write_ok" })
        elif instr["type"] == "cas":
            if k in self.m:
                if self.m[k] == instr["from"]:
                    m = copy(self.m)
                    m[k] = instr["to"]
                    return (AMap(m), { "type": "cas_ok" })
                else:
                    return (self, RPCError.precondition_failed(f"expected {instr["from"]}, but had {self.m[k]}").as_dict())
            # else fall through
        return (self, RPCError.key_does_not_exist("not found").as_dict())