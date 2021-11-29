"""
program that generates state to send via an RCP to the Raft Implementation
"""

import xmlrpc.client
import time
import random


class Client:
    def __init__(self, nodes):
        self.nodes = nodes
        self.leader_id = None

    def generate(self):
        vars = ['x', 'y', 'x']
        value = random.randint(0, 100)
        return {
            "timestamp": time.time(),
            "var": random.choice(vars),
            "value": value
        }

    def send_state(self, state):
        # if leader_id is None, then we need to find the leader
        if self.leader_id is None:
            self.leader_id = 0

        with xmlrpc.client.ServerProxy(self.nodes[self.leader_id]) as proxy:
            response = proxy.store_message(state)
            if not response[1]:
                self.leader_id = response[0]
                # resend state
                time.sleep(1)
                self.send_state(state)


if __name__ == '__main__':
    hostname = 'localhost'
    nodes = [f"http://{hostname}:{i + 8000:d}" for i in range(5)]

    client = Client(nodes)

    while True:
        state = client.generate()
        client.send_state(state)
        time.sleep(3)

