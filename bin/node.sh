#!/bin/bash

BASEDIR=$(dirname "$0")
echo "$BASEDIR"

source "${BASEDIR}/env.sh"

echo "$JAVA"
echo "$PID_DIR"
NODE_ID=$1

if [[ -z "$NODE_ID" ]]
then
  NODE_ID="node-0"
fi

if NODE_PID=$(pgrep -F "${PID_DIR}/${NODE_ID}.pid")
then
  echo "Node service with node-id: $NODE_ID is already running with PID $NODE_PID"
#  tail -f "$LOG_DIR/${NODE_ID}.log"
else
  echo "Starting Node Service with node-id : $NODE_ID ..."
  nohup "$JAVA"  \
      -Dname=minikafkamaster \
      -Dmode=node \
      -Dconfig="$CONFIG_DIR" \
      -jar "$JARS_DIR/Server-1.0.jar" ${NODE_ID} \
      "$@" > "$LOG_DIR/${NODE_ID}.log" \
      & echo $! > "$PID_DIR/$NODE_ID.pid"
#      & tail -f "$LOG_DIR/$NODE_ID.log"
fi

if RETENTION_PID=$(pgrep -F "${PID_DIR}/${NODE_ID}_retention.pid")
then
  echo "RetentionService service for node-id: $NODE_ID is already running with PID $RETENTION_PID"
else
  echo "Starting Retention Service for node-id : $NODE_ID ..."
  nohup "$JAVA"  \
      -Dconfig="$CONFIG_DIR" \
      -jar "$JARS_DIR/RetentionService-1.0-SNAPSHOT.jar" \
      "$@" > "$LOG_DIR/${NODE_ID}_retention.log" \
      & echo $! > "$PID_DIR/${NODE_ID}_retention.pid"
#      & tail -f "$LOG_DIR/${NODE_ID}_retention.log"
fi

tail -f "$LOG_DIR/$NODE_ID.log"

#nohup /home/roshan/.jdks/openjdk-17.0.1/bin/java  \
#    -Dname=minikafkamaster \
#    -Dmode=node \
#    -Dconfig=/home/roshan/workspace/ekbana/study/MiniKafkaSystem/config \
#    -jar /home/roshan/workspace/ekbana/study/MiniKafkaSystem/Server/build/libs/Server-1.0.jar node\
#    "$@" > node.log \
#& echo $! > node.pid \
#& nohup /home/roshan/.jdks/openjdk-17.0.1/bin/java \
#      -Dservice=retention \
#      -Dconfig=/home/roshan/workspace/ekbana/study/MiniKafkaSystem/config \
#      -jar ./RetentionService/build/libs/RetentionService-1.0-SNAPSHOT.jar \
#      "@$" > retention.log \
#& echo $! > retention.pid \
#& tail -f node.log