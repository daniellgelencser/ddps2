"""
Implementation of raft node.
"""

import logging
import random
import time
import threading
import traceback
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer


class Node:
    def __init__(self, id, num_nodes, hostname):
        self.logger = logging.getLogger(f'Node({id})')
        logging.basicConfig(filename=f'raft({id}).log', level=logging.INFO)

        self.id = id
        self.leader_id = None
        self.state = "follower"
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitIndex = 0
        self.lastApplied = 0
        self.nextIndex = {}
        self.matchIndex = {}
        self.message_received = False
        self.electionTimeout = random.randint(1500, 5000)
        self.heartbeatTimeout = random.randint(150, 300)

        self.ip = hostname

        self.nodes = [{'id': i, 'url': f'http://{hostname}:{8000 + i}'} for i in range(num_nodes) if i != id]

        # start main loop in a separate thread
        thread = threading.Thread(target=self.main_loop)
        thread.daemon = True
        thread.start()

        # todo: is this the best way to do this?
        # start RPC server
        self.server = SimpleXMLRPCServer(("localhost", 8000 + id), allow_none=True)

        # register two rpc functions
        self.server.register_function(self.request_vote_rpc, "request_vote_rpc")
        self.server.register_function(self.append_entries_rpc, "append_entries_rpc")
        self.server.register_function(self.store_message, "store_message")

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
        self.logger.info("[%s] Follower loop", self.id)
        while self.state == "follower":
            self.message_received = False
            time.sleep(self.electionTimeout / 1000)
            # if no message received, become candidate
            if not self.message_received:
                self.state = "candidate"

    def candidate_loop(self):
        self.logger.info("[%s] Candidate loop", self.id)
        self.currentTerm += 1
        self.votedFor = self.id
        self.state = "candidate"
        self.electionTimeout = random.randint(1500, 5000)
        votes = [True]

        # send RequestVote RPCs to all other servers
        jobs = []
        for node in self.nodes:
            # start a new thread to send request vote RPC
            thread = threading.Thread(target=self.send_request_vote_rpc, args=(node, votes))
            thread.start()
            jobs.append(thread)

        # wait for all threads to finish
        for job in jobs:
            job.join()

        # if votes > majority, become leader
        if sum(votes) > (len(self.nodes) + 1) / 2:
            self.state = "leader"
            self.nextIndex = {node['id']: len(self.log) for node in self.nodes}
            self.matchIndex = {node['id']: 0 for node in self.nodes}
            self.heartbeatTimeout = random.randint(150, 300)
            self.logger.info("[%s] Became leader", self.id)
        else:
            self.state = "follower"
            self.logger.info("[%s] Did not become leader", self.id)

    def send_request_vote_rpc(self, node, votes):
        with ServerProxy(node['url']) as proxy:
            try:
                last_log_term = self.log[-1]['term'] if self.log else 0
                last_log_index = self.log[-1]['index'] if self.log else 0
                response = proxy.request_vote_rpc(self.currentTerm, self.id, last_log_index, last_log_term)

                # if response contains term T > currentTerm, convert to follower
                if response[0] > self.currentTerm:
                    self.update_current_term_and_become_follower(response[0])
                    return votes.append(False)

                # if response contains voteGranted, increment votes
                if response[1]:
                    return votes.append(True)

            except Exception as e:
                self.logger.error("[%s] Error in request_vote_rpc: %s %s", self.id, e, traceback.format_exc())

        return votes.append(False)

    def leader_loop(self):
        self.logger.info("[%s] Leader loop", self.id)
        while self.state == "leader":
            time.sleep(self.heartbeatTimeout / 1000)
            self.heartbeatTimeout = random.randint(150, 300)
            self.send_heartbeats()

    def update_current_term_and_become_follower(self, term):
        self.votedFor = None
        self.currentTerm = term
        self.state = "follower"
        self.logger.info("[%s] Updated current term to %d", self.id, self.currentTerm)

    def send_heartbeats(self):
        # send AppendEntries RPCs to all other servers
        jobs = []
        for node in self.nodes:
            # start a new thread to send append entries RPC
            thread = threading.Thread(target=self.send_append_entries_rpc, args=(node,))
            thread.start()

        # wait for all threads to finish
        for job in jobs:
            job.join()

    def send_append_entries_rpc(self, node):
        with ServerProxy(node['url']) as proxy:
            try:
                prev_log_index = self.log[-1]['index'] if self.log else 0
                prev_log_term = self.log[-1]['term'] if self.log else 0
                term = self.currentTerm
                entries = self.log[self.nextIndex[node['id']]:] if self.nextIndex[node['id']] < len(self.log) else []

                response = proxy.append_entries_rpc(term,
                                                    self.id,
                                                    prev_log_index,
                                                    prev_log_term,
                                                    entries,
                                                    self.commitIndex)

                # if response contains term T > currentTerm set currentTerm = T and convert to follower
                if response[0] > self.currentTerm:
                    self.update_current_term_and_become_follower(response[0])
                    return

                # if response is successful, update nextIndex and matchIndex
                if response[1]:
                    self.nextIndex[node['id']] = self.log[-1]['index'] + 1 if self.log else 0
                    self.matchIndex[node['id']] = self.log[-1]['index'] if self.log else 0

                # if fails because of log inconsistency, decrement nextIndex and retry
                else:
                    self.nextIndex[node.id] -= 1
                    return self.send_append_entries_rpc(node)

                # if there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
                # and log[N].term == currentTerm set commitIndex = N
                for N in range(self.commitIndex + 1, len(self.log)):
                    majority = 1
                    for follower_node in self.nodes:
                        if self.matchIndex[follower_node['id']] >= N:
                            majority += 1

                    if majority > len(self.nodes) + 1 / 2:
                        self.commitIndex = N
                        self.logger.info("[%s] Updated commitIndex to %d", self.id, self.commitIndex)

            except Exception as e:
                self.logger.error("[%s] communication with %s Exception: %s %s", self.id, node.id, e,
                                  traceback.format_exc())

    def append_entries_rpc(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit) -> (int, bool):
        self.message_received = True

        try:
            # todo: is this correct see rules for servers
            # if AppendEntries RPC request received form new leader, convert to follower
            if self.state == "candidate" and leader_id != self.id:
                self.state = "follower"

            self.leader_id = leader_id

            # reply false if term < currentTerm
            if term < self.currentTerm:
                return self.currentTerm, False

            # reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            if -1 < prev_log_index < len(self.log) and self.log[prev_log_index]['term'] != prev_log_term:
                return self.currentTerm, False

            # if an existing entry conflicts with a new one (same index but different terms),
            # delete the existing entry and all that follow it
            for entry in entries:
                if self.log and entry['index'] < len(self.log) and self.log[entry['index']]['term'] != entry['term']:
                    self.log = self.log[:entry['index']]
                    break

            # append any new entries not already in the log
            if entries:
                self.log += entries[len(self.log) - prev_log_index - 1:]

            # update commitIndex
            if leader_commit > self.commitIndex:
                self.commitIndex = min(leader_commit, self.log[-1]['index'])
                self.logger.info("[%s] Updated commitIndex to %d", self.id, self.commitIndex)

        except Exception as e:
            self.logger.error("[%s] Exception in append entries rpc: %s %s", self.id, e, traceback.format_exc())

        return self.currentTerm, True

    def request_vote_rpc(self, term, candidate_id, last_log_index, last_log_term) -> (int, bool):
        self.message_received = True

        # reply false if term < currentTerm
        if term < self.currentTerm:
            return self.currentTerm, False

        # if request contains term T > currentTerm, set currentTerm = T and convert to follower
        elif term > self.currentTerm:
            self.update_current_term_and_become_follower(term)

        # reply false if votedFor is already set
        if self.votedFor is not None:
            return self.currentTerm, False

        # reply false if log is more up-to-date than the candidate's
        if self.log and self.log[-1]['index'] > last_log_index and self.log[last_log_index].term > last_log_term:
            return self.currentTerm, False

        # grant vote if candidate's log is at least as up-to-date as receiver's
        # and has not been voted for in the election
        self.votedFor = candidate_id
        self.state = "follower"
        return self.currentTerm, True

    def store_message(self, message):
        # if not leader, send to leader
        if self.state != "leader":
            return self.leader_id, False

        # if leader, append to log
        else:
            # add ingestion time to the message
            message['ingestion_time'] = time.time()
            self.log.append({'index': len(self.log), 'term': self.currentTerm, 'data': message})
            # log message and commitIndex
            self.logger.info("[%s] Stored message %s at index %d", self.id, message, self.commitIndex)
            return self.id, True
