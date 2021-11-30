"""
program that generates state to send via an RCP to the Raft Implementation
"""
import argparse
import os
import xmlrpc.client
import time
import random
import logging

logging.basicConfig(filename=f'logs/generator.log', level=logging.INFO)

class Client:
    def __init__(self, nodes):
        self.nodes = nodes
        self.leader_id = None
        self.rate = 1/10

        # initialize logger
        self.logger = logging.getLogger("generator")

    def run(self):
        while True:
            state = self.generate()
            self.send_state(state)
            time.sleep(1 / self.rate)

    def generate(self):
        return {
            "generation_time": time.time(),
        }

    def send_state(self, state):
        # if leader_id is None, then we need to find the leader
        if self.leader_id is None:
            # random element of nodes
            self.leader_id = random.choice(list(self.nodes.keys()))

        self.logger.info(f"Sending state {state}")

        with xmlrpc.client.ServerProxy(self.nodes[self.leader_id]) as proxy:
            response = proxy.store_message(state)
            if not response[1]:
                self.leader_id = response[0]
                # resend state
                time.sleep(1)
                self.send_state(state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=8000, type=int, help='Listening port')
    parser.add_argument('--local', default=True, action='store_true', help='Run locally')
    parser.add_argument('--cluster', nargs='+', type=int, default=[], help='List of peers in the cluster')
    args = parser.parse_args()

    if args.local:
        nodes = {peer: f"http://localhost:{i + args.port:d}" for i, peer in enumerate(args.cluster)}
    else:
        nodes = {peer: f"http://{peer}.cm.cluster:{i + args.port:d}" for i, peer in enumerate(args.cluster)}

    # make the results directory
    os.makedirs('results', exist_ok=True)

    client = Client(nodes)
    client.run()
