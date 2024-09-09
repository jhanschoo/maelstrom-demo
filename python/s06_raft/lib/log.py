from copy import copy

from .node import Node
from .rpcerror import RPCError


class Log:
    """
    The log is the main append-only (up to uncommitted entries, up to log compression) data structure that is replicated (up to uncommitted entries, up to log compression) across all nodes in the cluster.

    Note: the log is 1-indexed, so the initial (sentinel) entry is at index 1.

    All the entries except the entries corresponding to the last term in the log are guaranteed to be committed, if the method contracts are followed.
    """
    def __init__(self, node: Node) -> None:
        """
        The replicated log always starts with a sentinel entry.
        """
        self.node = node
        self.entries = [{"term": 0, "idx": 1, "op": None}]

    def __getitem__(self, i: int) -> dict:
        return self.entries[i-1]

    def extend(self, entries: list):
        """
        Contract: the entries are contiguous and start from the next index after the last entry in the log.
        """
        if not entries:
            return
        self.entries.extend(entries)

    def from_index(self, i: int) -> list:
        """
        Return all entries from index i to the end of the log, as a list.
        """
        if i < 1:
            raise ValueError(f"invalid index {i}")
        return self.entries[i-1:]

    def truncate(self, i: int) -> None:
        self.entries = self.entries[:i]

    @property
    def last(self) -> dict:
        return self.entries[-1]

    def __len__(self) -> int:
        return len(self.entries)