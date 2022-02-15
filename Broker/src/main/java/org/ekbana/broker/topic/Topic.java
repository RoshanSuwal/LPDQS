package org.ekbana.broker.topic;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.ekbana.broker.Policy.ConsumerRecordBatchPolicy;
import org.ekbana.broker.Policy.DefaultSegmentPolicy;
import org.ekbana.broker.consumer.Consumer;
import org.ekbana.broker.producer.Producer;
import org.ekbana.broker.record.Recorder;
import org.ekbana.broker.record.Records;
import org.ekbana.broker.record.RecordsQueue;
import org.ekbana.broker.segment.SegmentMetaData;
import org.ekbana.broker.segment.SegmentSearchTree;
import org.ekbana.broker.utils.BrokerConfig;
import org.ekbana.broker.utils.FileUtil;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

@Getter @Setter
@ToString
public class Topic {
    private final String topicName;
    private TopicMetaData topicMetaData;
    private Recorder recorder;
    // producer
    private Producer producer;
    // consumer
    private Consumer consumer;
    // records queue
    private RecordsQueue<Records> recordsQueue;

    public Topic(String topicName,boolean isNew) {
        this(topicName,isNew,null);
    }

    public Topic(String topicName, boolean isNew, ExecutorService executorService){
        this.topicName = topicName;
        this.topicMetaData=isNew?createTopicMetaData():loadTopicMetaData();
        recorder=new Recorder(topicName,topicMetaData,BrokerConfig.getInstance().segmentPolicy(),BrokerConfig.getInstance().consumerRecordBatchPolicy(),getOrCreateSegmentSearchTree(isNew));
        recordsQueue = new RecordsQueue<>(100,recorder,executorService);

        producer=new Producer(recordsQueue,recorder);
        consumer=new Consumer(recorder,topicName);
    }

    private SegmentSearchTree getOrCreateSegmentSearchTree(boolean isNew){
        final SegmentSearchTree segmentSearchTree = new SegmentSearchTree(BrokerConfig.getInstance().getSEGMENT_SEARCH_TREE_NODE_CAPACITY(), BrokerConfig.getInstance().retentionPolicy());
        if (!isNew){
            try {
                FileUtil.readAllLines(BrokerConfig.getInstance().getDATA_PATH() +topicName+"/"+BrokerConfig.getInstance().getSEGMENT_FILE_NAME(),SegmentMetaData.class)
                        .forEach(smd->segmentSearchTree.addSegment((SegmentMetaData) smd));
                segmentSearchTree.reEvaluate();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return segmentSearchTree;
    }

    private TopicMetaData createTopicMetaData(){
        /*
        * all the steps needed to create new topic
        * create topic metadata
        * creates file and directories for given topic
        * delete existing topic data
        * */
        return TopicMetaData.builder()
                .activeSegmentMetaData(SegmentMetaData.builder()
                        .segmentId(0)
                        .build())
                .build();
    }

    public TopicMetaData loadTopicMetaData(){
        /*
         * all the necessary steps need to be applied in server startup
         * loads topic metadata
         * loads producer
         * loads consumer instance
         * */
        try {
           return (TopicMetaData) FileUtil.readObjectFromFile(BrokerConfig.getInstance().getDATA_PATH() + topicName + "/" + BrokerConfig.getInstance().getTOPIC_METADATA_FILE_NAME());
        }catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return createTopicMetaData();
        }
    }
}
