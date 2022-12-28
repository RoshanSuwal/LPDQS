# Lightweight Distributed Message Queue System 


LWDMQS is resource-light distributed event streaming platform for data pipelines, stream analytics, and data integration. 

## Broker
contains all the logic for storing and fetching producerRecords from particular topic

## Client 
**4 differnet apis**
- TopicCreator Api : api used to create topic
- TopicDeleter Api : api used to delete topic
- Producer Api : api used to insert records in topic
- Consumer Api : api used to read records from topic

## Run 
    java -Dconfig=path-to-config-dir -Dmode=mode-to-run Server.jar

## 2 Modes of server
1. **leader** mode
- used to run server in master/leader.
- manages the data nodes
- manages the client connections

2. **node** mode
- works as data node.
- used to manages the topic.
- used to manages the data.

## BUILD METHOD
```
./build-module.sh [output-path]
```
Note : Must specify the **GRADLE_HOME** path and **JAVA**  path inside the ***build.sh*** file.<br/><br/>
 After being executed, following directories are created with necessary files
 - /output-path/bin  - contains necessary shell scripts to run master and slave services
 - /output-path/config  - contains the config files
 - /output-path/jars - contains the project jars and dependencies
 - /output-path/logs - path to store logs
 - /output-path/plugins - contains the plugins
 - /output-path/pids - store the pids of the services

### Shell Scripts (/bin/)
 - env.sh : configure the environment variables and necessary path and directories
 - master.sh : executes the master service
 - node.sh : executes the node service along with retention service
 - stop-all.sh : stop all the running services

### Config files
- kafka.properties : properties file for kafka server, for both node and leader
- logback.xml : configuration for log 

### How to run service 

#### 1. Master Service
    ./bin/master.sh
#### 2. Node Services
    ./bin.node.sh [node-id]




