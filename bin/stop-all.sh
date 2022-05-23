#!/bin/bash

BASEDIR=$(dirname "$0")
echo "$BASEDIR"

source "${BASEDIR}/env.sh"

for entry in "$PID_DIR"/*
do
  if NODE_PID=$(pgrep -F "$entry")
  then
    echo "Stopping service $entry having pid $NODE_PID"
    kill "$NODE_PID"
  fi

  rm -f "$entry"

done