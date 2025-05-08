#!/bin/bash
# Usage: ./run-cluster.sh <role> <nodeCount>
ROLE=$1
NODES=$2

for ((i=1;i<=NODES;i++)); do
  java -jar ../target/distributed-algorithms-1.0-SNAPSHOT.jar --role=$ROLE --id=$i --peers=$(seq -s, 1 $NODES | sed 's/,/.,/g')
done
