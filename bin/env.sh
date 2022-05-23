#!/bin/bash

ROOT_DIR="/home/roshan/workspace/ekbana/study/MiniKafkaSystem/output"

JAVA="/home/roshan/.jdks/openjdk-17.0.1/bin/java"

BIN_DIR="$ROOT_DIR/bin"
PID_DIR="$ROOT_DIR/pids"
CONFIG_DIR="${ROOT_DIR}/config/"
LOG_DIR="$ROOT_DIR/logs"
JARS_DIR="$ROOT_DIR/jars"
PLUGIN_DIR="$ROOT_DIR/plugins"

if [[ ! -d "$LOG_DIR" ]]
 then
   mkdir "$LOG_DIR"
fi

if [[ ! -d "$PID_DIR" ]]
 then
   mkdir "$PID_DIR"
fi

if [[ ! -d "$ROOT_DIR/topic" ]]
 then
   mkdir "${ROOT_DIR}/topic"
fi

if [[ ! -d "$ROOT_DIR/data" ]]
 then
   mkdir "${ROOT_DIR}/data"
fi

if [[ ! -d "$ROOT_DIR/consumer" ]]
 then
   mkdir "${ROOT_DIR}/consumer"
fi

