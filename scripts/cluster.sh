#!/bin/bash

CODE_PATH=/var/scratch/ddps2105/ddps2

preserve -# 6 -t 00:01:00

# wait a few seconds for the workers to be reserved
sleep 5

worker_list=$(preserve -llist | grep ddps2105 | awk '{print $9,$10,$11,$12,$13,$14}')
read -r -a workers <<< "$worker_list"
echo "reserved the following nodes: ${workers[*]}"

# for the 5 nodes in the cluster
for n in "${workers[@]:1}"
do
  echo "" | ssh "$n" python3 $CODE_PATH/Node.py --name "$n" --port 8000 --cluster "${workers[@]:1}" &
done

# wait a few seconds for the nodes to start
sleep 10

# start the client
echo "" ssh "${workers[0]}" python3 $CODE_PATH/Client.py --port 8000 --cluster "${workers[@]:1}"
