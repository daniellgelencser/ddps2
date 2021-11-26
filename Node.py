"""
Implementation of raft node.
"""

import logging
import random
import time
import threading
from collections import namedtuple
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

logger = logging.getLogger(__name__)

RaftNode = namedtuple('RaftNode', ['id', 'url'])


class Node:
    def __init__(self, id, num_nodes):
        self.id = id
        self.state = "follower"
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitIndex = 0
        self.lastApplied = 0
        self.nextIndex = {}
        self.matchIndex = {}
        self.message_received = False
        self.electionTimeout = random.randint(150, 300)
        self.heartbeatTimeout = random.randint(150, 300)

        self.nodes = []
        for i in range(num_nodes):
            if i != id:
                self.nodes.append(RaftNode(i, "http://localhost:%d" % (8000 + i)))

        # start main loop in a separate thread
        thread = threading.Thread(target=self.main_loop)
        thread.daemon = True
        thread.start()

        # todo: is this the best way to do this?
        # start RPC server
        self.server = SimpleXMLRPCServer(("localhost", 8000 + id))

        # register two rpc functions
        self.server.register_function(self.request_vote_rpc, "request_vote_rpc")
        self.server.register_function(self.append_entries_rpc, "append_entries_rpc")

        # Run the server's main loop
        self.server.serve_forever()

    def main_loop(self):
        while True:
            if self.state == "follower":
                self.follower_loop()
            elif self.state == "candidate":
                self.candidate_loop()
            elif self.state == "leader":
                self.leader_loop()
            else:
                raise ValueError("Invalid state")

    def follower_loop(self):
        logger.info("[%s] Follower loop", self.id)
        while self.state == "follower":
            self.message_received = False
            time.sleep(self.electionTimeout / 1000)
            # if no message received, become candidate
            if not self.message_received:
                self.state = "candidate"

    def candidate_loop(self):
        logger.info("[%s] Candidate loop", self.id)
        self.currentTerm += 1
        self.votedFor = self.id
        self.state = "candidate"
        self.electionTimeout = random.randint(150, 300)
        votes = 1

        # send RequestVote RPCs to all other servers
        for node in self.nodes:
            if node.id != self.id:
                with ServerProxy(node.url) as proxy:
                    try:
                        response = proxy.request_vote_rpc(self.currentTerm, self.id, len(self.log), self.log[-1]['term'])
                        if response:
                            votes += 1
                    except Exception as e:
                        logger.error("[%s] Exception: %s", self.id, e)


        # if votes > majority, become leader
        if votes > len(self.nodes) / 2:
            self.state = "leader"
            self.nextIndex = {node.id: len(self.log) for node in self.nodes}
            self.matchIndex = {node.id: 0 for node in self.nodes}
            self.heartbeatTimeout = random.randint(150, 300)
            logger.info("[%s] Became leader", self.id)
        else:
            self.state = "follower"
            logger.info("[%s] Did not become leader", self.id)

    def leader_loop(self):
        logger.info("[%s] Leader loop", self.id)
        while self.state == "leader":
            time.sleep(self.heartbeatTimeout / 1000)
            self.heartbeatTimeout = random.randint(150, 300)
            self.send_heartbeats()

    def send_heartbeats(self):
        # send AppendEntries RPCs to all other servers
        for node in self.nodes:
            if node.id != self.id:
                self.send_append_entries_rpc(node)

    def send_append_entries_rpc(self, node):
        with ServerProxy(node.url) as proxy:
            response = proxy.append_entries_rpc(self.id,
                                                self.currentTerm,
                                                self.nextIndex[node.id],
                                                self.log[self.nextIndex[node.id] - 1].term,
                                                self.log[self.nextIndex[node.id] - 1].data)

            # if response is successful, update nextIndex and matchIndex
            if response[1]:
                self.nextIndex[node.id] += 1
                self.matchIndex[node.id] += 1

            # if fails because of log inconsistency, decrement nextIndex and retry
            else:
                self.nextIndex[node.id] -= 1
                self.send_append_entries_rpc(node)

    def append_entries_rpc(self, leader_id, prev_log_index, prev_log_term, entries, leader_commit) -> (int, bool):
        self.message_received = True

        # todo: is this correct see rules for servers
        # if AppendEntries RPC resent received form new leader, convert to follower
        if self.state == "candidate":
            self.currentTerm = prev_log_term
            self.state = "follower"
            return self.currentTerm, False

        # reply false if term < currentTerm
        if self.currentTerm < prev_log_term:
            return self.currentTerm, False

        # reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if len(self.log) > prev_log_index and self.log[prev_log_index].term != prev_log_term:
            return self.currentTerm, False

        # if an existing entry conflicts with a new one (same index but different terms),
        # delete the existing entry and all that follow it
        if len(self.log) > prev_log_index and self.log[prev_log_index].term == prev_log_term:
            self.log = self.log[:prev_log_index]

        # append any new entries not already in the log
        for entry in entries:
            self.log.append(entry)

        # update commitIndex
        if leader_commit > self.commitIndex:
            self.commitIndex = min(leader_commit, len(self.log) - 1)

        return self.currentTerm, True

    def request_vote_rpc(self, candidate_id, candidate_term, last_log_index, last_log_term) -> (int, bool):
        self.message_received = True

        # reply false if term < currentTerm
        if self.currentTerm < candidate_term:
            return self.currentTerm, False

        # reply false if log is more up-to-date than the candidate's
        if len(self.log) > last_log_index and self.log[last_log_index].term > last_log_term:
            return self.currentTerm, False

        # grant vote if candidate's log is at least as up-to-date as receiver's
        self.votedFor = candidate_id
        self.state = "follower"
        self.electionTimeout = random.randint(150, 300)
        return self.currentTerm, True

    def receive_message_from_client(self, message):
        # if not leader, send to leader
        if self.state != "leader":
            pass

        # if leader, append to log
        else:
            self.log.append({'term': self.currentTerm, 'data': message})
            self.commitIndex += 1
