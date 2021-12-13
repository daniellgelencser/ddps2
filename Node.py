"""
Implementation of raft node.
Notes:
    - rpc loop takes between 0.004 and 0.006 seconds to execute.
"""
import json
import logging
import os
import random
import socket
import time
import threading
import argparse
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer


class Node:
    def __init__(self, run, name, hostname, port, peers):
        self.logger = logging.getLogger(f'Node({name})')
        os.makedirs(f'results/{run}', exist_ok=True)
        self.state_machine_file = f'./results/{run}/{name}.csv'
        os.makedirs(f'logs/{run}', exist_ok=True)
        logging.basicConfig(filename=f'logs/{run}/raft({name}).log', level=logging.INFO)

        self.name = name
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
        self.electionTimeout = 0
        self.update_election_timeout()
        self.heartbeatTimeout = 500
        socket.setdefaulttimeout(self.heartbeatTimeout / 1000 / 2)

        self.ip = hostname

        self.nodes = [{'name': peer['name'], 'url': f'http://{peer["hostname"]}:{peer["port"]}'} for peer in peers]

        # start main loop in a separate thread
        thread = threading.Thread(target=self.main_loop)
        thread.daemon = True
        thread.start()

        # todo: is this the best way to do this?
        # start RPC server
        self.server = SimpleXMLRPCServer((hostname, port), allow_none=True)

        # register two rpc functions
        self.server.register_function(self.request_vote_rpc, "request_vote_rpc")
        self.server.register_function(self.append_entries_rpc, "append_entries_rpc")
        self.server.register_function(self.store_message, "store_message")

        # Run the server's main loop
        self.server.serve_forever()

    def update_election_timeout(self):
        self.electionTimeout = random.randint(1000, 1500)

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
        self.logger.info("[%s] Follower loop", self.name)
        while self.state == "follower":
            self.message_received = False
            time.sleep(self.electionTimeout / 1000)
            # if no message received, become candidate
            if not self.message_received:
                self.state = "candidate"

    def candidate_loop(self):
        self.logger.info("[%s] Candidate loop", self.name)
        self.currentTerm += 1
        self.votedFor = self.name
        self.state = "candidate"
        self.update_election_timeout()
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
            next_index = self.log[-1]['index'] + 1 if self.log else 0
            self.nextIndex = {node['name']: next_index for node in self.nodes}
            self.matchIndex = {node['name']: 0 for node in self.nodes}
            self.logger.info("[%s] Became leader", self.name)
        else:
            self.state = "follower"
            self.logger.info("[%s] Did not become leader", self.name)

    def send_request_vote_rpc(self, node, votes):
        with ServerProxy(node['url']) as proxy:
            try:
                last_log_term = self.log[-1]['term'] if self.log else 0
                last_log_index = self.log[-1]['index'] if self.log else 0
                response = proxy.request_vote_rpc(self.currentTerm, self.name, last_log_index, last_log_term)

                # if response contains term T > currentTerm, convert to follower
                if response[0] > self.currentTerm:
                    self.update_current_term_and_become_follower(response[0])
                    return votes.append(False)

                # if response contains voteGranted, increment votes
                if response[1]:
                    return votes.append(True)

            except Exception as e:
                self.logger.error("[%s] Error in request_vote_rpc to %s: %s", self.name, node['url'], e)

        return votes.append(False)

    def leader_loop(self):
        self.logger.info("[%s] Leader loop", self.name)
        while self.state == "leader":
            time.sleep(self.heartbeatTimeout / 1000)
            self.send_heartbeats()

    def update_current_term_and_become_follower(self, term):
        self.votedFor = None
        self.currentTerm = term
        self.state = "follower"
        self.logger.info("[%s] Updated current term to %d", self.name, self.currentTerm)

    def send_heartbeats(self):
        # send AppendEntries RPCs to all other servers
        jobs = []
        for node in self.nodes:
            # start a new thread to send append entries RPC
            thread = threading.Thread(target=self.send_append_entries_rpc, args=(node,))
            thread.start()

        # wait for all threads to finish
        for job in jobs:
            job.join(timeout=self.heartbeatTimeout / 1000 / 2)

    def commit_entry(self):
        prev_commit_index = self.commitIndex
        # if there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
        # and log[N].term == currentTerm set commitIndex = N
        for N in range(self.commitIndex, len(self.log)):
            majority = 1
            for follower_node in self.nodes:
                if self.matchIndex[follower_node['name']] >= N:
                    majority += 1

            if majority > (len(self.nodes) + 1) / 2:
                self.commitIndex = N

        # if commitIndex changed, apply log entries starting from commitIndex
        if self.commitIndex > prev_commit_index:
            self.logger.info("[%s] Updated commitIndex to %d", self.name, self.commitIndex)
            self.apply_log_entries(prev_commit_index, self.commitIndex)

    def send_append_entries_rpc(self, node):
        with ServerProxy(node['url']) as proxy:
            try:
                prev_log_index = self.log[-1]['index'] if self.log else 0
                prev_log_term = self.log[-1]['term'] if self.log else 0
                term = self.currentTerm

                if self.nextIndex[node['name']] < len(self.log):
                    entries = self.log[self.nextIndex[node['name']]:]
                else:
                    entries = []

                response = proxy.append_entries_rpc(term,
                                                    self.name,
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
                    if entries:
                        self.nextIndex[node['name']] = entries[-1]['index'] + 1 if self.log else 0
                        self.matchIndex[node['name']] = entries[-1]['index'] if self.log else 0

                # if fails because of log inconsistency, decrement nextIndex and retry
                else:
                    self.nextIndex[node['name']] -= 1
                    return

            except Exception as e:
                self.logger.error("[%s] communication with %s Exception: %s", self.name, node['name'], e)

        self.commit_entry()

    def append_entries_rpc(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit) -> (str, bool):
        self.message_received = True

        try:
            # todo: is this correct see rules for servers
            # if AppendEntries RPC request received form new leader, convert to follower
            if self.state == "candidate" and leader_id != self.name:
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
            for entry in entries:
                if len(self.log) == 0 or entry['index'] > self.log[-1]['index']:
                    self.log.append(entry)

            # update commitIndex
            prev_commit_index = self.commitIndex
            if leader_commit > self.commitIndex:
                self.commitIndex = min(leader_commit, self.log[-1]['index'])

            # if commitIndex changed, apply log entries starting from commitIndex
            if self.commitIndex > prev_commit_index:
                self.logger.info("[%s] Updated commitIndex to %d", self.name, self.commitIndex)
                self.apply_log_entries(prev_commit_index, self.commitIndex)

        except Exception as e:
            self.logger.error("[%s] Exception in append entries rpc: %s", self.name, e)

        return self.currentTerm, True

    def request_vote_rpc(self, term, candidate_id, last_log_index, last_log_term) -> (str, bool):
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
        if self.log and self.log[-1]['index'] > last_log_index and self.log[last_log_index]['term'] > last_log_term:
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
            next_index = self.log[-1]['index'] + 1 if self.log else 0
            self.log.append({'index': next_index, 'term': self.currentTerm, 'data': message})
            # log message and commitIndex
            self.logger.info("[%s] Reserved message %s for index %d", self.name, message, next_index)
            return self.name, True

    def apply_log_entries(self, prev_commit_index, commitIndex):
        # append the index, generation_time, ingestion_time and commit_time to a csv file
        file = open(self.state_machine_file, 'a')
        for entry in self.log[prev_commit_index:commitIndex]:
            # append commit time to the message
            entry['data']['commit_time'] = time.time()
            data = entry['data']
            file.write(
                f'{str(entry["index"])},{data["id"]},{data["rate"]},{self.state},{data["generation_time"]},{data["ingestion_time"]},{data["commit_time"]}\n')

        file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', default=0, type=str, help='id of the server in the cluster, used for logging')
    parser.add_argument('--local', action='store_true', help='Run locally')
    parser.add_argument('--port', default=8000, type=int, help='Listening port')
    parser.add_argument('--cluster', nargs='+', type=str, default=[], help='List of peers in the cluster')
    parser.add_argument('--run', type=int, default=0, help='Run id of the test')
    args = parser.parse_args()

    # make the log directory
    os.makedirs('logs', exist_ok=True)

    # id: node102
    # hostname: node102.cm.cluster
    # port: 7077
    # peers: [node102, node103, node104, node105, node106]
    # --id node102 --hostname node102.cm.cluster --port 7077 --cluster node102 node103 node104 node105 node106

    peers = [
        {
            'name': peer,
            'hostname': 'localhost' if args.local else f'{peer}.cm.cluster',
            'port': args.port + id
        }
        for id, peer in enumerate(args.cluster)
    ]

    # own port and hostname
    hostname = 'localhost' if args.local else f'{args.name}.cm.cluster'
    port = args.port + args.cluster.index(args.name)

    # filter out self
    peers = [peer for peer in peers if peer['name'] != args.name]

    # create a node
    node = Node(args.run, args.name, hostname, port, peers)
