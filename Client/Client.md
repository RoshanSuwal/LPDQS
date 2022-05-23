# MINI KAFKA CLIENT

APIs to connect to **Mini Kafka System**

## Types Of APIs

### 1. Topic Creation API

Api used to create topic.

```JAVA
public class TopicCreateApplication {
    public void creteTopic() {
        Properties properties = new Properties();
        properties.setProperty("kafka.server.address", "localhost");
        properties.setProperty("kafka.server.port", "9999");
        properties.setProperty("kafka.topic.name", "test");
        properties.setProperty("kafka.topic.numberOfPartitions", "1");
        final KafkaTopicCreator kafkaTopicCreator = new KafkaTopicCreator(properties);
        kafkaTopicCreator.create();
    }
}
```

### 2. Topic Deletion API
Api used to delete topic
```JAVA
public class TopicDeleteApplication {
    public void deleteTopic() {
        Properties properties = new Properties();
        properties.setProperty("kafka.server.address", "localhost");
        properties.setProperty("kafka.server.port", "9999");
        properties.setProperty("kafka.topic.name", "test");
        final KafkaTopicDeleter kafkaTopicDeleter = new KafkaTopicDeleter(properties);
        kafkaTopicDeleter.delete();
    }
}
```

### 3. Producer API
Api used to send records to topic
```JAVA
public class ProducerApplication {
    public void producer() {
        Properties properties = new Properties();
        properties.setProperty("kafka.server.address", "localhost");
        properties.setProperty("kafka.server.port", "9999");
        properties.setProperty("kafka.topic.name", "test");
        properties.setProperty("kafka.topic.partition","0"); // optional,
        properties.setProperty("kafka.topic.key","key"); // optional
        
        KafkaProducer kafkaProducer=new KafkaProducer(properties);
        kafkaProducer.connect();
        for (int i=0;i<10;i++) {
            kafkaProducer.send("hello world");
            kafkaProducer.send("second message");
        }
        kafkaProducer.stopAfterCompletion(); // terminates the application after all records are sent
    }
}
```

### 4. Consumer API

Api used to fetch records from topic.

```JAVA

public class ConsumerApplication {
    public void consumer() {
        Properties properties = new Properties();
        properties.setProperty("kafka.server.address", "localhost");
        properties.setProperty("kafka.server.port", "9999");
        properties.setProperty("kafka.topic.name", "test");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.connect();

        while (true){
            final JsonObject records = kafkaConsumer.getRecords();
            kafkaConsumer.sendNextRequestToServer();
            System.out.println("read records : "+records);
            Thread.sleep(1000);
        }
    }
}

```