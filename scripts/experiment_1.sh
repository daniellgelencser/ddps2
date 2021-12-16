#!/bin/bash

CODE_PATH=/var/scratch/ddps2105/ddps2

module load prun

preserve -# 6 -t 01:00:00

# wait a few seconds for the workers to be reserved
sleep 5

worker_list=$(preserve -llist | grep ddps2105 | awk '{print $9,$10,$11,$12,$13,$14}')
read -r -a workers <<< "$worker_list"
echo "reserved the following nodes: ${workers[*]}"

# repeat the experiment 10 times
for i in {1..10}
do

  # for the 5 nodes in the cluster
  for n in "${workers[@]:1}"
  do
    echo "$n" /var/scratch/ddps2105/Python-3.9.7/python $CODE_PATH/Node.py --run "$i" --name "$n" --port 8000 --cluster "${workers[@]:1}"
    echo "" | ssh "$n" /var/scratch/ddps2105/Python-3.9.7/python $CODE_PATH/Node.py --run "$i" --name "$n" --port 8000 --cluster "${workers[@]:1}" &
  done

  # wait a few seconds for the nodes to start
  sleep 10

  # start the client
  echo "${workers[0]}" /var/scratch/ddps2105/Python-3.9.7/python $CODE_PATH/Client.py --run "$i" --port 8000 --cluster "${workers[@]:1}"
  echo "" | ssh "${workers[0]}" /var/scratch/ddps2105/Python-3.9.7/python $CODE_PATH/Client.py --run "$i" --messages 100000 --port 8000 --cluster "${workers[@]:1}"

  # wait for the client to finish and terminate the nodes
  echo "" | ssh "${workers[0]}" pkill -f Client.py

  for n in "${workers[@]:1}"
  do
    echo "" | ssh "$n" pkill -f Node.py
  done

done