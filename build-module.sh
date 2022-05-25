#!/bin/bash

DEST_DIR=$1

GRADLE_HOME="/home/roshan/.gradle/wrapper/dists/gradle-7.3-bin/ddwl0k7mt9g6ak16i1m905vyv/gradle-7.3/bin/gradle"
JAVA_HOME="/usr/lib/jvm/java-16-oracle"
#JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

#gradle_version=$("$GRADLE_HOME" --version)
#echo "$gradle_version"

echo "JAVA_HOME = $JAVA_HOME"
echo "GRADLE_HOME = $GRADLE_HOME"
echo "DEST_DIR = $DEST_DIR"

if [[ "$JAVA_HOME" ]];then
  version=$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ "$version" > "16.0.0" ]] && [[ "$version" < "17.0.0" ]]
   then
     echo "$version"
    else
      echo "java version must be greater than 16 but provide $version"
      exit
  fi
fi

if [ -z "$DEST_DIR" ]
then
  echo "please specify output directory"
  echo "command : ./build-module.sh [output_dir_path]"
  exit
fi

if [[ -d "$DEST_DIR" ]]
 then
   echo "output directory exists"
 else
   echo "directory does not exists"
   exit
fi

if [[ -d "$DEST_DIR/jars" ]]
 then
   echo "jars directory exists"
 else
   echo "directory does not exists"
   mkdir "${DEST_DIR}/jars"
fi

if [[ -d "$DEST_DIR/bin" ]]
 then
   echo "directory exists"
 else
   echo "directory does not exists"
   mkdir "${DEST_DIR}/bin"
fi

if [[ -d "$DEST_DIR/config" ]]
 then
   echo "directory exists"
 else
   echo "directory does not exists"
   mkdir "${DEST_DIR}/config"
fi

if [[ ! -d "$DEST_DIR/logs" ]]
 then
   mkdir "${DEST_DIR}/logs"
fi

if [[ ! -d "$DEST_DIR/pids" ]]
 then
   mkdir "${DEST_DIR}/pids"
fi

if [[ ! -d "$DEST_DIR/topic" ]]
 then
   mkdir "${DEST_DIR}/topic"
fi

if [[ ! -d "$DEST_DIR/data" ]]
 then
   mkdir "${DEST_DIR}/data"
fi

if [[ ! -d "$DEST_DIR/consumer" ]]
 then
   mkdir "${DEST_DIR}/consumer"
fi


#exit
echo "Building Plugin Api"
$GRADLE_HOME -Dorg.gradle.java.home=$JAVA_HOME plugin-api:jar
$GRADLE_HOME -Dorg.gradle.java.home=$JAVA_HOME plugin-core:jar

echo "Building Server"
$GRADLE_HOME -Dorg.gradle.java.home=$JAVA_HOME Server:bootJar
$GRADLE_HOME -Dorg.gradle.java.home=$JAVA_HOME Server:copyToLib
echo "Building RetentionService"
$GRADLE_HOME -Dorg.gradle.java.home=$JAVA_HOME RetentionService:bootJar
echo "Building Connector"
$GRADLE_HOME -Dorg.gradle.java.home=$JAVA_HOME Connector:bootJar
echo "Building Client"
$GRADLE_HOME -Dorg.gradle.java.home=$JAVA_HOME Client:jar

#echo "Building Plugin Api"
#gradle plugin-api:jar
#gradle plugin-core:jar
#
#echo "Building Server"
#gradle Server:bootJar
#gradle Server:copyToLib
#echo "Building RetentionService"
#gradle RetentionService:bootJar
#echo "Building Connector"
#gradle Connector:bootJar
#echo "Building Client"
#gradle Client:jar

## copy jar to specific directory
cp ./plugin-api/build/libs/*.jar "$DEST_DIR/jars"
cp ./plugin-core/build/libs/*.jar "$DEST_DIR/jars"
cp ./Server/build/libs/*.jar "$DEST_DIR/jars"
cp ./Server/build/output/lib/*.jar "$DEST_DIR/jars"
cp ./RetentionService/build/libs/*.jar "$DEST_DIR/jars"
cp ./Connector/build/libs/*.jar "$DEST_DIR/jars"
cp ./Client/build/libs/*.jar "$DEST_DIR/jars"

cp ./config/*.* "$DEST_DIR/config/"
cp ./bin/*.* "$DEST_DIR/bin/"

## copy configuration file to specific directory

##