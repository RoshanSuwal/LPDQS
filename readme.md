# Mini Kafka System

MiniKafka system is a simple message broker system (lite version of Kafka) working in distributed architecture. 

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

