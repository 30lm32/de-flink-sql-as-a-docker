#!/usr/bin/env bash

set -a

DATA_DIR=data
NETWORK_NAME=`pwd | tr '\/' '\n' | tail -1`
JOB_MANAGER_RPC_ADDRESS=jobmanager
JOB_MANAGER_RPC_PORT=8081

set +a