#!/bin/bash
start-slave.sh $MASTER -c $CORES -m $MEM
echo "Started slave on master $MASTER with $CORES cores and $MEM memory"
tail -f /dev/null
