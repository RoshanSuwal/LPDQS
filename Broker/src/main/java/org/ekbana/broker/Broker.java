package org.ekbana.broker;

import ch.qos.logback.classic.util.ContextInitializer;
import org.ekbana.broker.consumer.Consumer;
import org.ekbana.broker.producer.Producer;
import org.ekbana.broker.topic.Topic;
import org.ekbana.broker.utils.BrokerConfig;
import org.ekbana.broker.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Broker {

    private final Logger log= LoggerFactory.getLogger(Broker.class);

    private final BrokerConfig brokerConfig;
    private final HashMap<String, Topic> topicHashMap = new HashMap<>();
    private final ExecutorService producerExecutorService;

    public Broker(BrokerConfig brokerConfig,ExecutorService producerExecutorService) {
        this.brokerConfig = brokerConfig;
        this.producerExecutorService=producerExecutorService;
    }

    public void load() {
        loadTopics();
    }

    private void loadTopics() {
        log.info("Loading topics");
        try {
            FileUtil.getDirectories(brokerConfig.rootPath()+brokerConfig.dataPath())
                    .map(File::getName)
//                    .peek(topicName->log.info("Loading Topic : "+topicName))
                    .forEach(topicName -> topicHashMap.put(topicName, new Topic(brokerConfig,topicName, false, producerExecutorService)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTopic(String topicName, int partitionId) {
        log.info("Creating topic : "+topicName+"-"+partitionId );
        removeTopic(topicName, partitionId);
        topicHashMap.put(topicName + "-" + partitionId, new Topic(brokerConfig,topicName + "-" + partitionId, true, producerExecutorService));
    }

    public void removeTopic(String topicName, int partitionId) {
        log.info("removing topic : "+topicName+"-"+partitionId );
        FileUtil.deleteDirectory(brokerConfig.rootPath()+brokerConfig.dataPath() + topicName + "-" + partitionId);
        topicHashMap.remove(topicName);
    }

    public Producer getProducer(String topicName, int partition) {
        return topicHashMap.get(topicName + "-" + partition).getProducer();
    }

    public Consumer getConsumer(String topicName, int partition) {
        return topicHashMap.get(topicName + "-" + partition).getConsumer();
    }

    public Topic getTopic(String topicName) {
        return topicHashMap.get(topicName);
    }

    public void print() {
        topicHashMap.keySet().forEach(System.out::println);
    }

    public void terminate() {
        producerExecutorService.shutdown();
    }

    public static void main(String[] args) {
        System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "/config/logback.xml");
        ExecutorService executorService=Executors.newFixedThreadPool(10);
        Broker broker=new Broker(new BrokerConfig(new Properties()),executorService);
        broker.createTopic("test",0);
        broker.createTopic("tweet",0);
    }
}
