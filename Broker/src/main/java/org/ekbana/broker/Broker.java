package org.ekbana.broker;

import org.ekbana.broker.consumer.Consumer;
import org.ekbana.broker.producer.Producer;
import org.ekbana.broker.topic.Topic;
import org.ekbana.broker.utils.FileUtil;
import org.ekbana.broker.utils.KafkaBrokerProperties;
import org.ekbana.minikafka.common.ConsumerRecords;
import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;

public class Broker {

    private final Logger log = LoggerFactory.getLogger(Broker.class);

//    private final BrokerConfig brokerConfig;
    private final HashMap<String, Topic> topicHashMap = new HashMap<>();
    private final ExecutorService producerExecutorService;

    private final KafkaBrokerProperties kafkaBrokerProperties;
    private final Policy<SegmentMetaData> segmentBatchPolicy;
    private final Policy<SegmentMetaData> segmentRetentionPolicy;
    private final Policy<ConsumerRecords> consumerRecordBatchPolicy;

//    public Broker(BrokerConfig brokerConfig, ExecutorService producerExecutorService) {
//        this.brokerConfig = brokerConfig;
//        this.producerExecutorService = producerExecutorService;
//    }

    public Broker(KafkaBrokerProperties kafkaBrokerProperties,
                  Policy<?> segmentBatchPolicy,
                  Policy<?> segmentRetentionPolicy,
                  Policy<?> consumerRecordBatchPolicy,
                  ExecutorService executorService) {
        this.kafkaBrokerProperties=kafkaBrokerProperties;
        this.segmentBatchPolicy= (Policy<SegmentMetaData>) segmentBatchPolicy;
        this.segmentRetentionPolicy= (Policy<SegmentMetaData>) segmentRetentionPolicy;
        this.consumerRecordBatchPolicy= (Policy<ConsumerRecords>) consumerRecordBatchPolicy;
        this.producerExecutorService=executorService;


    }

    public void load() {
        loadTopics();
    }

    private void loadTopics() {
        log.info("Loading topics");
        try {

//            FileUtil.getDirectories(brokerConfig.rootPath() + brokerConfig.dataPath())
//                    .map(File::getName)
////                    .peek(topicName->log.info("Loading Topic : "+topicName))
//                    .forEach(topicName -> topicHashMap.put(topicName, new Topic(brokerConfig, topicName, false, producerExecutorService)));
            FileUtil.getDirectories(kafkaBrokerProperties.getRootPath()+kafkaBrokerProperties.getDataPath())
                    .map(File::getName)
                    .peek((file)->log.info("Loaded topic : {}",file))
                    .forEach(topicName->topicHashMap.put(topicName,new Topic(kafkaBrokerProperties,topicName,
                            segmentBatchPolicy,
                            segmentRetentionPolicy,
                            consumerRecordBatchPolicy,
                            false,producerExecutorService)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTopic(String topicName, int partitionId) {
        removeTopic(topicName, partitionId);
        log.info("Creating topic : {}_{} ", topicName, partitionId);
        topicHashMap.put(topicName+"-"+partitionId,new Topic(kafkaBrokerProperties,topicName+"-"+partitionId,
                segmentBatchPolicy,
                segmentRetentionPolicy,
                consumerRecordBatchPolicy,
                true,producerExecutorService));
//        topicHashMap.put(topicName + "-" + partitionId, new Topic(brokerConfig, topicName + "-" + partitionId, true, producerExecutorService));
    }

    public void removeTopic(String topicName, int partitionId) {
        log.info("removing topic : {}_{}", topicName, partitionId);
        FileUtil.deleteDirectory(kafkaBrokerProperties.getRootPath() + kafkaBrokerProperties.getDataPath() + topicName + "-" + partitionId);
        topicHashMap.remove(topicName);
    }

    public Producer getProducer(String topicName, int partition) {
//        System.out.println(topicName+":"+partition);
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

    public void onStop(){
        terminate();
        topicHashMap.values().forEach(topic -> topic.saveToDisk());
    }

    public static void main(String[] args) {
//        System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "/config/logback.xml");
//        ExecutorService executorService = Executors.newFixedThreadPool(10);
//        Broker broker = new Broker(new BrokerConfig(new Properties()), executorService);
//        broker.createTopic("test", 0);
//        broker.createTopic("tweet", 0);
    }
}
