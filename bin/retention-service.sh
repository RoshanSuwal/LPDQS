#!/bin/bash

JAVA=${0}
CONFIG_PATH=${1}

JAVA=/home/roshan/.jdks/openjdk-17.0.1/bin/java
CONFIG_PATH=/home/roshan/workspace/ekbana/study/MiniKafkaSystem/config

echo JAVA CONFIG_PATH

$JAVA \
  -Dservice=retention \
  -Dconfig=$CONFIG_PATH \
   -jar \
   ./RetentionService/build/libs/RetentionService-1.0-SNAPSHOT.jar