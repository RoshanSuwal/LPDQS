package org.ekbana.broker;

import org.ekbana.broker.consumer.Consumer;
import org.ekbana.broker.producer.Producer;
import org.ekbana.broker.topic.Topic;
import org.ekbana.broker.utils.BrokerConfig;
import org.ekbana.broker.utils.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Broker {

    private final HashMap<String, Topic> topicHashMap=new HashMap<>();

    private final ExecutorService producerExecutorService= Executors.newFixedThreadPool(10);
//    private final ExecutorService consumerExecutorService= Executors.newFixedThreadPool(10);

    public Broker() {
        loadTopics();
    }

    private void loadTopics(){
        try {
            FileUtil.getFiles(BrokerConfig.getInstance().getDATA_PATH())
                    .map(File::getName)
                    .forEach(topicName -> topicHashMap.put(topicName, new Topic(topicName, false,producerExecutorService)));
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTopic(String topicName){
        removeTopic(topicName);
        topicHashMap.put(topicName,new Topic(topicName,true,producerExecutorService));
    }

    public void removeTopic(String topicName){
        FileUtil.deleteDirectory(BrokerConfig.getInstance().getDATA_PATH()+topicName);
        topicHashMap.remove(topicName);
    }

    public Producer getProducer(String topicName){
        return topicHashMap.get(topicName).getProducer();
    }

    public Consumer getConsumer(String topicName){
        return topicHashMap.get(topicName).getConsumer();
    }

    public Topic getTopic(String topicName){
        return topicHashMap.get(topicName);
    }

    public void print(){
        topicHashMap.keySet().forEach(System.out::println);
    }

    public void terminate(){
        producerExecutorService.shutdown();
    }
}
