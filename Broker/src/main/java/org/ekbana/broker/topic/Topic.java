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
    private final BrokerConfig brokerConfig;
    private final String topicName;
    private TopicMetaData topicMetaData;
    private Recorder recorder;
    // producer
    private Producer producer;
    // consumer
    private Consumer consumer;
    // records queue
    private RecordsQueue<Records> recordsQueue;

    public Topic(BrokerConfig brokerConfig,String topicName,boolean isNew) {
        this(brokerConfig,topicName,isNew,null);
    }

    public Topic(BrokerConfig brokerConfig,String topicName, boolean isNew, ExecutorService executorService){
        this.brokerConfig=brokerConfig;
        this.topicName = topicName;
        this.topicMetaData=isNew?createTopicMetaData():loadTopicMetaData();
        recorder=new Recorder(brokerConfig,topicName,topicMetaData,brokerConfig.segmentPolicy(),brokerConfig.consumerRecordBatchPolicy(),getOrCreateSegmentSearchTree(isNew));
        recordsQueue = new RecordsQueue<>(100,recorder,executorService);

        producer=new Producer(recordsQueue,recorder);
        consumer=new Consumer(recorder,topicName);
    }

    private SegmentSearchTree getOrCreateSegmentSearchTree(boolean isNew){
        final SegmentSearchTree segmentSearchTree = new SegmentSearchTree(brokerConfig.segmentSearchTreeNodeCapacity(), brokerConfig.retentionPolicy());
        if (!isNew){
            try {
                FileUtil.readAllLines(brokerConfig.rootPath()+brokerConfig.dataPath() +topicName+"/"+brokerConfig.segmentFileName(),SegmentMetaData.class)
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
           return (TopicMetaData) FileUtil.readObjectFromFile(brokerConfig.rootPath()+brokerConfig.dataPath() + topicName + "/" + brokerConfig.topicMataDataFileName());
        }catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return createTopicMetaData();
        }
    }
}
