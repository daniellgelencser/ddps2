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

    time.sleep(2)

    # send a message to all nodes
    # for i in range(n):
    #     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     s.connect(("localhost", 9000 + i))
    #     s.sendall(b"Hello")
    #     s.close()

    # wait for all nodes to finish
    for i in range(n):
        processes[i].join()


