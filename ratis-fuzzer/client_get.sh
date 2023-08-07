#!/usr/bin/env bash
BIN=/Users/berkay/Documents/Research/ratis-fuzzing/ratis-examples/src/main/bin
PEERS=$2

${BIN}/client.sh arithmetic get --name $1 --peers ${PEERS}