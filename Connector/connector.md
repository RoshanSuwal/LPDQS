# MINI KAFKA Connectors

## RDMS CONNECTOR
RDMS Connector is a support application build to connect the **relational databases** and **Mini Kafka System**.
It can be used either to stream data from RDMS to Kafka Topic or vice-versa

### RDMS Source connector
RDMS source connector produces the records from the RDMS table to kafka topic.

### RDMS Sink Connector
RDMS Sink Connector consumes the records from the Kafka topic and insert into the RDMS table.

### Configuration of RDMS Connector
```JSON
{
  "connectorName": "name of connector",
  "connectorType": "source / sink",
  "jdbcDriver": "name of RDMS driver",
  "jdbcUrl": "jdbc:postgresql://localhost:5432/rad",
  "jdbcUsername": "RDMS username",
  "jdbcPassword": "RDMS password",
  "table": "name of RDMS table",
  "incrementalColumnName": "name of table column with incremental property",
  "incrementalColumnValue": "incremental column value",
  "prevIncrementalColumnValue": "previous incremental column value, optional",
  "incrementalTimeStampColumnName": "table column with incremental timestamp property",
  "incrementalTimeStampColumnValue": "incremental timestamp column value",
  "prevIncrementalTimeStampColumnValue": "previous incremental timestamp value",
  "fetchSize": "size of records to be fetched in each RDMS query",
  "batchSize": "batch size of records",
  "poolingInterval": "Interval to poll records from RDMS table",
  "kafkaServerAddress": "kafka server address",
  "kafkaServerPort": "kafka server port",
  "kafkaAuthUsername": "kafka auth username",
  "kafkaAuthPassword": "kafka auth password",
  "topicName": "name of kafka topic",
  "topicPartitionId": "partition ID of topic",
  "lastFetchedTopicOffset": {
    "partition-0": "record fetched offset in partition-0",
    "partition-1": "record fetched offset in partition-1"
  }
}
```

### Example configuration 
#### Source Configuration
```JSON
{
  "connectorName": "testSourceConnector",
  "connectorType": "source",
  "jdbcDriver": "org.postgresql.Driver",
  "jdbcUrl": "jdbc:postgresql://localhost:5432/rad",
  "jdbcUsername": "postgres",
  "jdbcPassword": "postgres",
  "table": "tweet1",
  "incrementalColumnName": "id",
  "incrementalColumnValue": 1480508162143027201,
  "incrementalTimeStampColumnValue": 0,
  "fetchSize": 20,
  "batchSize": 10,
  "poolingInterval": 5,
  "kafkaServerAddress": "localhost",
  "kafkaServerPort": "9999",
  "kafkaAuthUsername": "user",
  "kafkaAuthPassword": "password",
  "topicName": "test",
  "topicPartitionId": "-1",
  "prevIncrementalColumnValue": 1480508162143027201,
  "prevIncrementalTimeStampColumnValue": 0,
  "lastFetchedTopicOffset": {}
}
```

#### Sink Configuration
```JSON
{
  "connectorName": "test source connector",
  "connectorType": "sink",
  "jdbcDriver": "org.postgresql.Driver",
  "jdbcUrl": "jdbc:postgresql://localhost:5432/rad",
  "jdbcUsername": "postgres",
  "jdbcPassword": "postgres",
  "table": "tweet1",
  "incrementalColumnValue": 0,
  "incrementalTimeStampColumnValue": 0,
  "fetchSize": 1,
  "batchSize": 10,
  "poolingInterval": 5,
  "kafkaServerAddress": "localhost",
  "kafkaServerPort": "9999",
  "kafkaAuthUsername": "user",
  "kafkaAuthPassword": "password",
  "topicName": "test",
  "topicPartitionId": "-1",
  "prevIncrementalColumnValue": 0,
  "prevIncrementalTimeStampColumnValue": 0,
  "lastFetchedTopicOffset": {
    "0": 1299
  }
}
```

## How to run Connector
```
path-to-java-17/java \
    -Dpath = path-to-store-connector-configurations-and-logs \
    -Dconfig = configuration-of-connector.json \
    -jar Connector-1.0.jar 
```

