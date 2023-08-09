#!/usr/bin/env bash
BIN=../ratis-examples/src/main/bin
PEERS=$2
PORT=$3

ID=$1; ${BIN}/server.sh arithmetic server --id ${ID} --storage /tmp/ratis/${ID} --fuzzerclientport ${PORT} --peers ${PEERS}