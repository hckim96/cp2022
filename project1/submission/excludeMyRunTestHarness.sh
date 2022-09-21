#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

WORKLOADS_DIR=${1-$DIR/workloads}
WORKLOADS_DIR=$(echo $WORKLOADS_DIR | sed 's:/*$::')

cd $WORKLOADS_DIR

for workloadDir in */ ; do
    WORKLOAD_DIR=${1-$DIR/workloads/$workloadDir}
    WORKLOAD_DIR=$(echo $WORKLOAD_DIR | sed 's:/*$::')
    cd $WORKLOAD_DIR

    WORKLOAD=$(basename "$PWD")
    echo execute $WORKLOAD ...
    $DIR/build/release/harness *.init *.work *.result ../../run.sh
done
# for workload in 

# WORKLOAD_DIR=${1-$DIR/workloads/small}
# WORKLOAD_DIR=$(echo $WORKLOAD_DIR | sed 's:/*$::')
# cd $WORKLOAD_DIR

# WORKLOAD=$(basename "$PWD")
# echo execute $WORKLOAD ...
# $DIR/build/release/harness *.init *.work *.result ../../run.sh
