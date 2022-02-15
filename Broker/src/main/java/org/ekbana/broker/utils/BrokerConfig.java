package org.ekbana.broker.utils;

import lombok.Getter;
import org.ekbana.broker.Policy.ConsumerRecordBatchPolicy;
import org.ekbana.broker.Policy.DefaultSegmentPolicy;
import org.ekbana.broker.Policy.Policy;
import org.ekbana.broker.Policy.SegmentRetentionPolicy;
import org.ekbana.broker.record.Records;
import org.ekbana.broker.segment.Segment;

@Getter
public class BrokerConfig {
    private static BrokerConfig brokerConfig=null;
    public static BrokerConfig getInstance(){
        if (brokerConfig==null) brokerConfig=new BrokerConfig();
        return brokerConfig;
    }

    private final String DATA_PATH="data/";
    private final String SEGMENT_FILE_NAME="segment.txt";
    private final String TOPIC_METADATA_FILE_NAME="topicMetaData.txt";

    // determines the capacity of internal node in segment search tree
    private final int SEGMENT_SEARCH_TREE_NODE_CAPACITY=3;

    public Policy<Segment> segmentPolicy(){
        return new DefaultSegmentPolicy();
    }

    public Policy<Records> consumerRecordBatchPolicy(){
        return new ConsumerRecordBatchPolicy();
    }

    public SegmentRetentionPolicy retentionPolicy(){
//        return new SegmentRetentionPolicy();
        return null;
    }
}
