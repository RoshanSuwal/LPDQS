#!/bin/bash

JAVA=/home/roshan/.jdks/openjdk-17.0.1/bin/java
ROOT_PATH=/home/roshan/workspace/ekbana/study/MiniKafkaSystem/output
CONFIG_PATH=/home/roshan/workspace/ekbana/study/MiniKafkaSystem/output/config
PID_PATH=/home/roshan/workspace/ekbana/study/MiniKafkaSystem

function check_if_node_process_exists() {
  if NODE_PID=$(pgrep -F "${PID_PATH}/node.pid")
  then
    echo "Node service is running with PID $NODE_PID"
  else
    echo "Node service is not running"
  fi
}

function check_if_retention_process_exists() {
  if RETENTION_PID=$(pgrep -F /home/roshan/workspace/ekbana/study/MiniKafkaSystem/retention.pid)
  then
    echo "Retention service is running with PID $RETENTION_PID"
  else
    echo "Retention service is not running"
  fi
}

check_if_node_process_exists
check_if_retention_process_exists

#function check_retention_service () {
##  if  RET_PID=$(pgrep -f 'RetentionService-1.0-SNAPSHOT.jar')
#  if  RET_PID=$(pgrep -f service=ret *)
#  then
#      echo "Retention Service is running with PID $RET_PID"
#  else
#      echo "Starting Retention Service"
#  fi
#}
#
#function check_master_service() {
#  if  MASTER_PID=$(pgrep -f 'Server-1.0.jar master')
#  then
#    echo "Master Service is Running with PID $MASTER_PID"
#  else
#    echo "Master service is not Running"
#  fi
#}
#
#function check_node_service() {
#  if  NODE_PID=$(pgrep -f 'Server-1.0.jar node')
#  then
#    echo "Node Service is Running with PID $NODE_PID"
#  else
#    echo "Node service is not Running"
#  fi
#}
#
#echo $JAVA $CONFIG_PATH
#check_retention_service
#check_master_service
#check_node_service
#$JAVA \
#  -Dconfig=$CONFIG_PATH \
#   -jar \
#   /home/roshan/workspace/ekbana/study/MiniKafkaSystem/RetentionService/build/libs/RetentionService-1.0-SNAPSHOT.jar \
#   "$@" & \


