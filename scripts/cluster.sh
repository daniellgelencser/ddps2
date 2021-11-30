#!/bin/bash

preserve -# 6 -t 00:05:00

# wait a few seconds for the workers to be reserved
sleep 5

worker_list=$(preserve -llist | grep ddps2105 | awk '{print $9,$10,$11,$12,$13,$14}')
read -r -a workers <<< "$worker_list"
echo "reserved the following nodes: ${workers[*]}"

# for the 5 nodes in the cluster
for n in "${workers[@]:1}"
do
 echo Node.py --name "$n" --port 8000 --cluster "${workers[@]:1}" &
done

# wait a few seconds for the nodes to start
sleep 10

# start the client
echo Client.py --port 8000 --cluster "${workers[@]:1}"
