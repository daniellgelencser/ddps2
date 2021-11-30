#!/bin/bash

CODE_PATH=/var/scratch/ddps2105/ddps2

module load prun

preserve -# 6 -t 00:01:00

# wait a few seconds for the workers to be reserved
sleep 5

worker_list=$(preserve -llist | grep ddps2105 | awk '{print $9,$10,$11,$12,$13,$14}')
read -r -a workers <<< "$worker_list"
echo "reserved the following nodes: ${workers[*]}"

# for the 5 nodes in the cluster
for n in "${workers[@]:1}"
do
  echo "$n" /var/scratch/ddps2105/Python-3.9.7/python $CODE_PATH/Node.py --name "$n" --port 8000 --cluster "${workers[@]:1}"
  echo "" | ssh "$n" /var/scratch/ddps2105/Python-3.9.7/python $CODE_PATH/Node.py --name "$n" --port 8000 --cluster "${workers[@]:1}" &
done

# wait a few seconds for the nodes to start
sleep 10

# start the client
echo "${workers[0]}" /var/scratch/ddps2105/Python-3.9.7/python $CODE_PATH/Client.py --port 8000 --cluster "${workers[@]:1}"
echo "" | ssh "${workers[0]}" /var/scratch/ddps2105/Python-3.9.7/python $CODE_PATH/Client.py --port 8000 --cluster "${workers[@]:1}"
