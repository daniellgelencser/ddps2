"""
program that generates state to send via an RCP to the Raft Implementation
"""
import argparse
import os
import xmlrpc.client
import time
import random
import logging

class Client:
    def __init__(self, run, rate, messages, is_constant, nodes):
        self.id = -1
        self.messages = messages
        self.nodes = nodes
        self.leader_id = None
        self.rate = rate
        self.constant = is_constant

        # initialize logger
        os.makedirs(f"logs/{run}", exist_ok=True)
        logging.basicConfig(filename=f'logs/{run}/generator.log', level=logging.INFO)
        self.logger = logging.getLogger("generator")

    def run(self):
        i = 1
        while True:
            state = self.generate()
            self.send_state(state)
            time.sleep(1 / self.rate)

            if not self.constant:
                self.rate += 1

            if i % self.messages == 0:
                break

            i += 1

    def generate(self):
        self.id += 1
        return {
            "rate": self.rate,
            "id": self.id,
            "generation_time": time.time(),
        }


    def send_state(self, state):
        # if leader_id is None, then we need to find the leader
        if self.leader_id is None:
            # random element of nodes
            self.leader_id = random.choice(list(self.nodes.keys()))

        self.logger.info(f"Sending state {state} to {self.nodes[self.leader_id]}")

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
    parser.add_argument('--rate', default=1, type=int, help='generation rate')
    parser.add_argument('--messages', default=1000, type=int, help='number of messages to send')
    parser.add_argument('--local', action='store_true', help='Run locally')
    parser.add_argument('--constant', action='store_true', help='Run constant rate')
    parser.add_argument('--cluster', nargs='+', type=str, default=[], help='List of peers in the cluster')
    parser.add_argument('--run', type=int, default=0, help='Run id of the test')
    args = parser.parse_args()

    if args.local:
        nodes = {peer: f"http://localhost:{i + args.port:d}" for i, peer in enumerate(args.cluster)}
    else:
        nodes = {peer: f"http://{peer}.cm.cluster:{i + args.port:d}" for i, peer in enumerate(args.cluster)}

    # make the results directory
    os.makedirs('results', exist_ok=True)

    client = Client(args.run, args.rate, args.messages, args.constant, nodes)
    client.run()
