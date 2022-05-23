#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
echo "$BASEDIR"

source "${BASEDIR}/env.sh"

echo "$JAVA"
echo "$PID_DIR"

if MASTER_PID=$(pgrep -F "${PID_DIR}/master.pid")
then
  echo "Master service is already running with PID $MASTER_PID"
  tail -f "$LOG_DIR/master.log"
else
  echo "Starting Master Service ..."
  nohup "$JAVA"  \
      -Dname=minikafkamaster \
      -Dmode=leader \
      -Dconfig="$CONFIG_DIR" \
      -jar "$JARS_DIR/Server-1.0.jar" \
      "$@" > "$LOG_DIR/master.log" \
      & echo $! > "$PID_DIR/master.pid"
fi

tail -f "$LOG_DIR/master.log"

#echo \$ $$ "PID of the script"


#& \
#/home/roshan/.jdks/openjdk-17.0.1/bin/java  \
#    -Dname=minikafkamaster \
#    -Dmode=node \
#    -Dconfig=/home/roshan/workspace/ekbana/study/MiniKafkaSystem/config \
#    -jar /home/roshan/workspace/ekbana/study/MiniKafkaSystem/Server/build/libs/Server-1.0.jar \
#    "$@"
#&&

#kill $(ps aux | grep 'Server-1.0.jar')
#ps aux | grep bash
#if pgrep -x "minikafkamaster" > /dev/null
#then
#  echo "Running"
#else
#  echo "stopped"
#fi
