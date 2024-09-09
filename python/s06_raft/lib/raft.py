from copy import copy
import math
import random
import threading
import time
from typing import Optional

from .amap import AMap
from .log import Log
from .node import Node
from .rpcerror import RPCError


class Raft:
    """
    Raft consensus algorithm implementation.
    Entry points:
        - handlers to RPCs
        - recurring tasks
    Since each RPC call and registered task is handled in a separate thread, we require locks to protect shared state.

    - `become_candidate`
    - `become_follower`
    - `become_leader`

    Note: Heartbeats are impleted in means of a frequent, fixed interval, auxillary check for whether it is time to fire the next event. This is a limitation of our decision to use the API in our `Node` class. If `Raft` were allowed to schedule its own events, then we could have used a more efficient, event-driven approach, since the following condition holds:
    - if an event were already scheduled for a time, we would not require the same class of event to fire at an earlier time.
    This allows allows us to use the following algorithm:
    - these events must fire at least periodically (e.g. every second or two), and a thread is used to maintain this.
        - we can make this yet more efficient by having the thread terminate when there are no events scheduled.
    - if a waiting thread wakes, and sees that the deadline is in the future, (due to changes from elsewhere), it can sleep until the deadline.

    Note: we have a memory leak just in case that a term lasts indefinitely, and we require a way to properly expire RPC callback handlers to address this.
    """
    def __init__(self, m = {}) -> None:
        # Components
        self.node = Node()
        self.lock = threading.RLock()
        self.state_machine = AMap()
        self.log = Log(self.node)

        # Heartbeats
        self.election_timeout = 2
        self.heartbeat_interval = 1
        self.min_replication_interval = 0.05

        self.election_deadline = time.time()
        self.step_down_deadline = time.time()
        self.last_rep_attempt = time.time()

        # Event handlers
        self.node.on("read", self.handle_client_req)
        self.node.on("write", self.handle_client_req)
        self.node.on("cas", self.handle_client_req)
        self.node.on("request_vote", self.handle_request_vote)
        self.node.on("append_entries", self.handle_append_entries)
        self.node.every(0.1, self.check_candidacy)
        self.node.every(0.1, self.check_step_down)
        self.node.every(self.min_replication_interval, lambda: self.replicate_log(False))

        # State machine, initial state
        self.state = "follower"
        self.term = 0
        self.voted_for: Optional[str] = None
        self.leader = None

        # Leader state
        self.commit_idx = 0
        self.last_applied = 1
        # next_idx tracks, for each other node, the starting index of our entries that we do not know to be replicated on that node
        self.next_idx = None
        # other_len tracks, for each other node, the length of their logs; except that it is 0 for each other node until we are certain that they are a prefix of our log
        self.other_len = None

    # handlers
    def handle_request_vote(self, req: dict) -> None:
        """
        Handle a request for a vote from another node (henceforth, candidate).
        - We have only one vote to give per term.
        - If the candidate is requesting a vote for a past term, we may ignore the request
        - If the other node's log is not up-to-date, we may ignore the request. This is because the candidate may have missed replicating committed entries. Since leaders commit only entries that are replicated to a majority of nodes, this behavior guarantees that when a candidate is elected as leader, it has all committed entries, but not necessarily all uncommitted entries.
            - It suffices to check that the candidate has replicated all entries up to the last term, and has replicated at least as many entries as we have.
        """
        with self.lock:
            body = req["body"]
            self.maybe_become_follower(body["term"])
            granted = False
            if body["term"] < self.term:
                self.node.log(f"Candidate term {body['term']} is less than current term {self.term}, not granting vote.")

            elif self.voted_for:
                self.node.log(f"Already voted for {self.voted_for}, not granting vote.")
            elif body["last_log_term"] < self.log.last["term"]:
                self.node.log(f"Candidate's last log term {body['last_log_term']} is less than my last log term {self.log.last['term']}, not granting vote.")
            elif body["last_log_term"] == self.log.last["term"] \
                and body["last_log_idx"] < self.log.last["idx"]:
                self.node.log(f"Our terms are both at {body['last_log_term']} but candidate's log is shorter at {body['last_log_idx']} instead of {self.log.last["idx"]}, not granting vote.")
            else:
                self.node.log(f"Granting vote to candidate {body['candidate_id']}")
                granted = True
                self.voted_for = body["candidate_id"]
                self.reset_election_deadline()
            self.node.reply(req, {
                "type": "request_vote_res",
                "term": self.term,
                "vote_granted": granted
            })

    def handle_client_req(self, req: dict) -> None:
        with self.lock:
            if self.state != "leader":
                if self.leader:
                    self.node.rpc(self.leader, req["body"], lambda res: self.node.reply(req, res["body"]))
                else:
                    self.node.reply(req, RPCError.temporarily_unavailable("no leader").as_dict())
                return

            self.log.extend([{ "term": self.term, "idx": self.log.last["idx"] + 1, "op": req }])
            #self.state_machine, res = self.state_machine.apply(op)
            #self.node.reply(req, res)

    def handle_append_entries(self, req: dict) -> None:
        """
        Handle a request from a current term or future term's leader to append entries to the log
        - In case the leader's prev_log_idx is greater than the last index in our log, the leader is mistaken on how up-to-date we are, and we reply with failure; the leader should decrement its next_idx and retry, to arrive at a common prefix.
        - In case the leader's prev_log_term does not match the term of the entry at prev_log_idx, the leader is missing some uncommitted entries in our log, and we reply with failure; the leader should decrement its next_idx and retry, to arrive at a common prefix.

        If we suceed in replicating the entries, we are guaranteed that we have a prefix of the entries presently in the leader's log, and so we can update our understanding of the committed entries.
        """
        body = req["body"]
        with self.lock:
            self.maybe_become_follower(body["term"])

            # Prepare default response indicating failure
            res = {
                "type": "append_entries_res",
                "term": self.term,
                "success": False
            }
            if body["term"] < self.term:
                self.node.reply(req, res)
                return

            self.leader = body["leader_id"]
            self.reset_election_deadline()

            prev_log_idx = body["prev_log_idx"]
            if prev_log_idx <= 0:
                raise RPCError.precondition_failed(f"invalid prev_log_idx {prev_log_idx}")
            if prev_log_idx > self.log.last["idx"]:
                self.node.reply(req, res)
                return
            if self.log[prev_log_idx]["term"] != body["prev_log_term"]:
                self.node.reply(req, res)
                return
            self.log.truncate(prev_log_idx)
            self.log.extend(body["entries"])

            if self.commit_idx < body["leader_commit"]:
                self.commit_idx = min(body["leader_commit"], self.log.last["idx"])
                self.advance_state_machine()

            res["success"] = True
            self.node.reply(req, res)


    # recurring tasks
    def check_candidacy(self) -> None:
        """
        If when we check we are not a leader, and the election deadline has passed, then we should become a candidate. The election deadline is reset
        - when we perform a state transition (RPC/periodic task entry point)
        - when we vote for a candidate
        - when a message is received from a leader

        Jitter is introduced into the check prodecure (distinct from the election timeout itself) to avoid the thundering herd problem.
        """
        time.sleep(random.uniform(0, 0.1))
        with self.lock:
            if self.election_deadline < time.time():
                if self.state != "leader":
                    self.become_candidate()
                self.reset_election_deadline()

    def check_step_down(self) -> None:
        """
        If when we check we are a leader, and the step down deadline has passed, then we should step down. The step down deadline is reset when we receive an ack from a follower.

        By stepping down, there will be no more leader for this term, and the nodes will proceed to elect a new leader.
        """
        with self.lock:
            if self.state == "leader" and self.step_down_deadline < time.time():
                self.node.log("Stepping down as leader: no recent acks")
                self.become_follower()

    def reset_election_deadline(self) -> None:
        """
        Reset the election deadline. Jitter is used to avoid the thundering herd problem.
        """
        with self.lock:
            self.node.log("reset election deadline")
            self.election_deadline = time.time() + random.uniform(self.election_timeout, self.election_timeout * 2)

    def reset_step_down_deadline(self) -> None:
        """
        Reset the step down deadline. No jitter is needed, since there is always at most one leader.
        """
        with self.lock:
            self.node.log("reset step down deadline")
            self.step_down_deadline = time.time() + self.step_down_deadline

    # state transitions
    def become_candidate(self) -> None:
        """
        State transition into becoming a candidate for the next term, hereby requesting votes from other nodes (this only happens for each node at most once per term just after we transition).
        """
        with self.lock:
            self.state = "candidate"
            self.advance_term(self.term + 1)
            self.voted_for = self.node.node_id
            self.leader = None
            self.reset_election_deadline()
            self.reset_step_down_deadline()
            self.node.log(f"became candidate for term {self.term}")
            self.request_votes()

    def become_follower(self) -> None:
        """
        State transition into becoming a follower for this term.
        """
        with self.lock:
            self.state = "follower"
            self.other_len = None
            self.next_idx = None
            self.leader = None
            self.reset_election_deadline()
            self.node.log(f"became follower for term {self.term}")

    def become_leader(self) -> None:
        """
        State transition into becoming a leader for this term, and initialize leader state.

        `commit_idx` is not reinitialized, pending information from other nodes.
        `next_idx` is initialized so that we begin by assuming that all other nodes are up-to-date with our log.
        `other_len` is initialized to 0 pending the first heartbeat.
        """

        with self.lock:
            if self.state != "candidate":
                raise RPCError.crash(f"cannot become leader in state {self.state}")

            self.state = "leader"
            self.leader = None
            self.last_rep_attempt = 0 # epoch is sentinel
            self.log.extend([{ "term": self.term, "idx": self.log.last["idx"] + 1, "op": None }])

            self.next_idx = {}
            self.other_len = {}
            
            for node_id in self.node.other_node_ids:
                self.next_idx[node_id] = self.log.last["idx"]
                self.other_len[node_id] = 1

            self.reset_step_down_deadline()
            self.node.log(f"became leader for term {self.term}")

    # helper functions for leader state
    def replicate_log(self, force = False) -> None:
        """
        Send a replication RPC to all other nodes in the cluster, if heartbeat should be sent or there are (for each server) unreplicated entries.

        Upon response:
        - if we discover that the other node is in a future term, we must join that term, and as a follower (we have lost the right to be a candidate for that future term).
        - the other node responds with body["success"] == False if the other node's log is less up-to-date than we expect. In this case, we decrement the index we assume we need to replicate from in the next replication attempt.
        - if we succeed, we update our understanding of the other node, keeping in mind that in the meantime due to out-of-order callback invocation, the response we get may be stale.
        """
        with self.lock:
            elapsed = time.time() - self.last_rep_attempt
            attempted = False

            if self.state == "leader" and self.min_replication_interval < elapsed:
                for node_id in self.node.other_node_ids:
                    ni = self.next_idx[node_id]
                    entries = self.log.from_index(ni)
                    if 0 < len(entries) or self.heartbeat_interval < elapsed:
                        self.node.log(f"replicating entry {ni} onward to {node_id}")
                        def callback(res: dict) -> None:
                            # Warning: it is an error to refer to the `node_id` in the outer scope, since it may have changed by the time the callback is invoked
                            src = res["src"]
                            body = res["body"]

                            with self.lock:
                                self.maybe_become_follower(body["term"])
                                if self.state == "follower":
                                    return
                                
                                self.reset_step_down_deadline()
                                if body["success"]:
                                    self.next_idx[src] = max(self.next_idx[src], ni + len(entries))
                                    self.other_len[src] = max(self.other_len[src], ni + len(entries) - 1)
                                    self.advance_commit_index()

                                else:
                                    self.next_idx[node_id] = min(self.next_idx[node_id], ni - 1)
                        attempted = True
                        self.node.rpc(node_id, {
                            "type": "append_entries",
                            "term": self.term,
                            "leader_id": self.node.node_id,
                            "prev_log_idx": ni - 1,
                            "prev_log_term": self.log[ni - 1]["term"],
                            "entries": entries,
                            "leader_commit": self.commit_idx
                        }, callback)
            if attempted:
                self.last_rep_attempt = time.time()

    def advance_commit_index(self):
        """
        Advance the commit index to the median of the last index replicated by a majority of nodes if possible.

        Note that it is not an error if the calculated median is less than the commit index, since `other_len` is reinitialized to 0 at the start of leader state, but commit index is persisted.

        However, we only advance the commit index if the entry at the median index is of the current term, since that is a clear way we can guarantee that said majority of nodes are a prefix of the current leader's log. This gives us no trouble since the leader appends a sentinel entry at the start of its term.
        """
        with self.lock:
            if self.state != "leader":
                return
            med = self.median(list(self.other_len.values()))
            self.node.log(f"median: {med}, self.log.last['idx']: {self.log.last['idx']}")
            # note that med is guaranteed to be not greater than the leader's index, due to the leader's own behavior and the circumstandes required for a majority of nodes to have voted for the leader
            if self.commit_idx < med and self.term == self.log[med]["term"]:
                self.node.log(f"advancing commit index to {med}")
                self.commit_idx = med
                self.advance_state_machine()

    @staticmethod
    def median(lst: list) -> int:
        """
        Return the median element in a nonempty list, rounded to the lower element.
        """
        if not lst:
            raise ValueError("empty list")
        lst.sort()
        return lst[(len(lst) - 1) // 2]

    # helper functions for candidate state
    def request_votes(self) -> None:
        """
        Request votes from other nodes in the cluster. As we do so, tally eligible votes (replies) with a variable in the closure. As replies are received,

        - if the other node is in a future term, then we must join that term, and as a follower (we have lost the right to be a candidate for that future term).
        - otherwise, we will have brought up the other node to at least our term, and consider ourselves to have their vote. By now we may already have become leader or follower for this term, or even be in a future term, in which case the vote is irrelevant, and we ignore it.
        - otherwise, we have received an eligible vote, and immediately perform a check to see if we have a majority of votes, in which case we become leader.
        """
        with self.lock:
            votes = set([self.node.node_id])
            term = self.term
            def request_votes_callback(res: dict) -> None:
                self.reset_step_down_deadline()
                with self.lock:
                    body = res["body"]
                    if self.maybe_become_follower(body["term"]):
                        return
                    # we are still a candidate, and self.term == body["term"]
                    # TODO: revisit since we assume here that the other node may respond with a body["term"] > term, and this check may be redundant
                    # but this vote response may be stale (though this shouldn't happen in this implementation where the callback is cleared on term change)
                    if self.state == "candidate" and self.term == term and body["vote_granted"]:
                        votes.add(res["src"])
                        self.node.log(f"have votes: {votes}")

                        if self.majority(len(self.node.node_ids)) <= len(votes):
                            self.become_leader()
            self.node.brpc({
                "type": "request_vote",
                "term": term,
                "candidate_id": self.node.node_id,
                "last_log_idx": self.log.last["idx"],
                "last_log_term": self.log.last["term"]
            }, request_votes_callback)

    @staticmethod
    def majority(n: int) -> int:
        return math.floor(n / 2) + 1

    # helper functions for follower state
    def vclock(self) -> int:
        """
        Return the vector clock maintained by this node. Correctly called only when leader.
        """
        m = copy(self.other_len)
        m[self.node.node_id] = self.log.last["idx"]
        return m

    # general helper functions
    def maybe_become_follower(self, term: int) -> bool:
        """
        If `term` is greater than the current term, then transition to the future term and become a follower.
        """
        with self.lock:
            if term > self.term:
                self.node.log(f"stepping down from term {self.term} to term {term}")
                self.advance_term(term)
                self.become_follower()
                return True
            return False

    def advance_term(self, term: int) -> None:
        """
        If `term` is greater than the current term, then advance the term and reset the vote.

        Moreover, all pending callbacks to sent RPCs should be cleared, since they are no longer relevant, and the other party may not even bother responding, and it is a memory leak to keep them around.
        """
        with self.lock:
            if term > self.term:
                self.term = term
                self.voted_for = None
                #self.node.clear_callbacks()
            else:
                raise RPCError.crash("term cannot decrease")

    def advance_state_machine(self):
        """
        Advance the state machine by applying uncommitted entries to the state machine.
        """
        with self.lock:
            while self.last_applied < self.commit_idx:
                self.last_applied += 1
                req = self.log[self.last_applied]["op"]
                self.node.log(f"applying req {req}")
                if req == None:
                    continue
                self.state_machine, res = self.state_machine.apply(req["body"])
                self.node.log(f"applied req {req} with result {res}")

                if self.state == "leader":
                    self.node.reply(req, res)
