package org.ekbana.broker.utils;

import lombok.Getter;
import org.ekbana.broker.Policy.ConsumerRecordBatchPolicy;
import org.ekbana.broker.Policy.DefaultSegmentPolicy;
import org.ekbana.broker.Policy.Policy;
import org.ekbana.broker.Policy.SegmentRetentionPolicy;
import org.ekbana.broker.record.Records;
import org.ekbana.broker.segment.Segment;

import java.util.Properties;

@Getter
public class BrokerConfig {

    private final Properties properties;
    public BrokerConfig(Properties properties) {
        this.properties = properties;
    }

//    private final String DATA_PATH="log/data/";
//    private final String SEGMENT_FILE_NAME="segment.txt";
//    private final String TOPIC_METADATA_FILE_NAME="topicMetaData.txt";

    // determines the capacity of internal node in segment search tree
//    private final int SEGMENT_SEARCH_TREE_NODE_CAPACITY=3;

    public int segmentSearchTreeNodeCapacity(){
        return Integer.parseInt(properties.getProperty("broker.data.segment.searchTree.node.capacity","3"));
    }
    public String rootPath(){
        return properties.getProperty("broker.rootPath","log/");
    }

    public String dataPath(){
        return properties.getProperty("broker.data.path","data/");
    }

    public String topicMataDataFileName(){
        return  properties.getProperty("broker.data.topic.metadata.filename","topicMetaData.txt");
    }

    public String segmentFileName(){
        return properties.getProperty("broker.data.segment.file.name","segment.txt");
    }

    public Long segmentSize(){
        return Long.parseLong(properties.getProperty("broker.data.segment.file.size","5242880"));// 5MB
    }

    public Long segmentLifespan(){
        return Long.parseLong(properties.getProperty("broker.data.segment.file.lifespan","86400"));// 1 day
    }

    public Long consumerBatchSize(){
        return Long.parseLong(properties.getProperty("broker.data.consumer.batch.size","100000"));// in bytes
    }


    public Policy<Segment> segmentPolicy(){
        return new DefaultSegmentPolicy();
    }

    public Policy<Records> consumerRecordBatchPolicy(){
        return new ConsumerRecordBatchPolicy();
    }

    public SegmentRetentionPolicy retentionPolicy(){
        return new SegmentRetentionPolicy();
    }
}
