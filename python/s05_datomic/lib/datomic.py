import random
import threading
import time
from amap import AMap
from idgen import IDGen
from node import Node
from rpcerror import RPCError
from thunk import Thunk

class State:
    SVC = "lin-kv"
    KEY = "root"

    def __init__(self, node: Node, idgen: IDGen) -> None:
        self.node = node
        self.idgen = idgen
        self.m = AMap(node, idgen, idgen.next(), {})

    def update_map(self) -> None:
        m_id = self.node.sync_rpc(State.SVC, {
            "type": "read",
            "key": State.KEY
        })["body"]["value"]
        self.m = Thunk.obtain(m_id, lambda: AMap(self.node, self.idgen, m_id, None))
    
    def transact(self, txns: list[tuple]) -> list:
        while True:
            m, ret = self.m.transact(txns)
            if m.id != self.m.id:
                m.save()
            res = self.node.sync_rpc(State.SVC, {
                "type": "cas",
                "key": State.KEY,
                "from": self.m.id,
                "to": m.id,
                "create_if_not_exists": True
            })
            if res["body"]["type"] == "cas_ok":
                self.m = m
                return ret
            time.sleep(random.uniform(0, 0.05))
            self.update_map()

class Transactor:
    def __init__(self) -> None:
        self.node = Node()
        self.lock = threading.Lock()
        self.state = State(self.node, IDGen(self.node))

        def handle_txn(req: dict) -> None:
            txn = req["body"]["txn"]
            self.node.log(f"Received txn {txn}")
            try:
                with self.lock:
                    res = self.state.transact(txn)
            except RPCError as e:
                self.node.reply(req, e.as_dict())
                return
            self.node.reply(req, {
                "type": "txn_ok",
                "txn": res
            })
        self.node.on("txn", handle_txn)

if __name__ == "__main__":
    Transactor().node.main()
