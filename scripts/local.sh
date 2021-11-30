#!/bin/bash

# initialize array of nodes
workers=("1" "2" "3" "4" "5" "6")

# for the 5 nodes in the cluster
for n in "${workers[@]:1}"
do
 python3 Node.py --name "$n" --local --port 8000 --cluster "${workers[@]:1}" &
done

# wait a few seconds for the nodes to start
sleep 10

# start the client
python3 Client.py --local --port 8000 --cluster "${workers[@]:1}"
