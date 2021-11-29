import time

from Node import Node
from multiprocessing import Process
import socket

if __name__ == "__main__":
    # number of nodes
    n = 5

    processes = []
    for i in range(n):
        # start a Process with a node
        p = Process(target=Node, args=(i, n, "localhost"))
        p.start()
        processes.append(p)

    # wait for all nodes to finish
    for i in range(n):
        processes[i].join()


