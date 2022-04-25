package org.ekbana.server.v2;

import org.ekbana.broker.Broker;
import org.ekbana.broker.utils.KafkaBrokerProperties;
import org.ekbana.server.KafkaLoader;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.Serializer;
import org.ekbana.server.v2.datanode.DataNodeController;
import org.ekbana.server.v2.datanode.DataNodeServerClient;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataNodeApplication {
    public static void main(String[] args) throws IOException {
        final Properties followerProperties = new Properties();
        followerProperties.setProperty("kafka.server.node.id", "node-1");
        followerProperties.setProperty("kafka.storage.data.path", "log2/");
        final KafkaProperties kafkFollowerProperties = new KafkaProperties(followerProperties);

        final Properties brokerPro = new Properties();
        brokerPro.setProperty("kafka.broker.root.path","log2/");
        final KafkaBrokerProperties kafkaBrokerProperties = new KafkaBrokerProperties(brokerPro);

        Serializer serializer=new Serializer();
        Deserializer deserializer=new Deserializer();
        ExecutorService brokerExecutorService= Executors.newFixedThreadPool(10);
        ExecutorService executorService= Executors.newFixedThreadPool(10);

        KafkaLoader kafkaLoader = new KafkaLoader("plugins");
        kafkaLoader.load();

        Broker broker=new Broker(kafkaBrokerProperties,
                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.batch.policy")).buildPolicy(kafkaBrokerProperties),
                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.retention.policy")).buildPolicy(kafkaBrokerProperties),
                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.consumer.record.batch.policy")).buildPolicy(kafkaBrokerProperties),
                brokerExecutorService);
        broker.load();

        DataNodeServerClient dataNodeServerClient=new DataNodeServerClient(kafkFollowerProperties);
        DataNodeController dataNodeController=new DataNodeController(
                serializer,
                deserializer,
                broker,kafkFollowerProperties,
                dataNodeServerClient,
                executorService
        );

        dataNodeServerClient.connect(executorService);

    }
}
