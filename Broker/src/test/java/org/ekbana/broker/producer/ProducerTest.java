package org.ekbana.broker.producer;

import org.ekbana.broker.segment.SegmentMetaData;
import org.ekbana.broker.topic.TopicMetaData;
import org.junit.Test;

public class ProducerTest {

    @Test
    public void createNewProducer(){
        SegmentMetaData activeSegment=SegmentMetaData.builder()
                .segmentId(0L)
                .build();
        TopicMetaData topicMetaData=TopicMetaData.builder()
                .activeSegmentMetaData(activeSegment)
                .build();
//        Producer producer=new Producer("test",topicMetaData,10,new DefaultSegmentPolicy(),new ActiveSegment(activeSegment,new InMemoryStorage<>()));
//        producer.addRecords(new Records(Arrays.asList(new Record("message1"),new Record("message 2"))));
//
//        Assertions.assertEquals(producer.getTopicMetaData().getActiveSegmentMetaData().getOffsetCount(),2);
    }
}
