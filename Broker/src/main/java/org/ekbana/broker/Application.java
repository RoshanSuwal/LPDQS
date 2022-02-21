package org.ekbana.broker;

import org.ekbana.broker.consumer.Consumer;
import org.ekbana.broker.producer.Producer;
import org.ekbana.broker.record.Record;
import org.ekbana.broker.record.Records;
import org.ekbana.broker.segment.SegmentMetaData;
import org.ekbana.broker.segment.SegmentService;
import org.ekbana.broker.topic.Topic;
import org.ekbana.broker.topic.TopicMetaData;
import org.ekbana.broker.utils.FileUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class Application {

    public static void main2(String[] args) throws InterruptedException {
        String topicName="test-1";
        SegmentMetaData activeSegment=SegmentMetaData.builder()
                .segmentId(6)
                .build();
        TopicMetaData topicMetaData=TopicMetaData.builder()
                .activeSegmentMetaData(activeSegment)
                .build();
        Topic topic=new Topic(topicName,true);
        final Producer producer = topic.getProducer();
//        Producer producer=new Producer("test",topicMetaData,10,new DefaultSegmentPolicy(),new ActiveSegment(activeSegment,new InMemoryStorage<>()));
//        Producer producer=new Producer(topicName,topicMetaData,10,new DefaultSegmentPolicy(),SegmentService.createActiveSegment("test-1",topicMetaData.getActiveSegment()));
        producer.addRecords(new Records(Arrays.asList(
                new Record(topicName,"message0",1),
                new Record(topicName,"message1",1),
                new Record(topicName,"message 2",1))));

        producer.addRecords(new Records(Arrays.asList(
                new Record(topicName,"message 3",1),
                new Record(topicName,"message 4",1))));

        producer.addRecords(new Records(Arrays.asList(
                new Record(topicName,"message 5",1),
                new Record(topicName,"message 6",1))));

        Thread.sleep(5000);
        System.out.println(Thread.currentThread().getName()+":"+topic.getTopicMetaData().getActiveSegmentMetaData());

        System.out.println();
        topic.getConsumer().getRecords(0,false).stream().forEach(System.out::println);
//        topic.getConsumer().getRecords(6,false).stream().forEach(System.out::println);
        System.out.println(Thread.activeCount());
        SegmentService.terminate();
    }

    public static void main3(String[] args) throws IOException {
//        FileUtil.getFiles("data/").forEach(path -> System.out.println(path.getName()));
//        FileUtil.getFiles("data")
        HashMap<String,Topic> topicHashMap=new HashMap<>();
//        FileUtil.getFiles("data").forEach(file -> topicHashMap.put(file.getName(),new Topic(file.getName(),false)));
        FileUtil.getFiles("data").map(path ->new Topic(path.getName().toString(), false)).forEach(topic -> topicHashMap.put(topic.getTopicName(),topic));
//        topics.forEach(topic -> System.out.println(topic.getTopicMetaData().getPassiveSegmentMetaData()));
        topicHashMap.values().forEach(topic -> {
            System.out.println(topic.getTopicName());
            System.out.println(topic.getTopicMetaData().getActiveSegmentMetaData());
            System.out.println(topic.getTopicMetaData().getPassiveSegmentMetaData());
        });
        topicHashMap.get("test-1").getConsumer()
                .getRecords(1,false)
                .stream()
                .forEach(System.out::println);
    }

    public static void main(String[] args) throws InterruptedException, IOException {
//        main2(args);
        String topicName="test-3";
        Broker broker=new Broker();
        broker.print();
        broker.createTopic(topicName,0);

        final Producer producer = broker.getProducer(topicName,0);
        producer.addRecords(new Records(Arrays.asList(
                new Record(topicName,"message0",1),
                new Record(topicName,"message1",1),
                new Record(topicName,"message 2",1))));

        producer.addRecords(new Records(Arrays.asList(
                new Record(topicName,"message 3",1),
                new Record(topicName,"message 4",1))));

        producer.addRecords(new Records(Arrays.asList(
                new Record(topicName,"message 5",1),
                new Record(topicName,"message 6",1))));

        Thread.sleep(5000);
        Consumer consumer=broker.getConsumer(topicName,0);

        final Records records = consumer.getRecords(0, false);
        records.stream().forEach(System.out::println);

        producer.addRecords(new Records(Arrays.asList(
                new Record(topicName,"message 7",1),
                new Record(topicName,"message 8",1))));

        producer.addRecords(new Records(Arrays.asList(
                new Record(topicName,"message 9",1),
                new Record(topicName,"message 10",1))));

        Thread.sleep(2000);
        final Records records1 = consumer.getRecords(3, false);
        records1.stream().forEach(System.out::println);


        System.out.println("Thread Count !!!!!!!!!!!!!!!!!!!!!!!\n"+Thread.activeCount());
        SegmentService.terminate();
        System.out.println(Thread.activeCount());
        broker.terminate();
        System.out.println(Thread.activeCount());
        System.exit(0);
    }
}
