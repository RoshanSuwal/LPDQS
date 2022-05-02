package org.ekbana.broker.record;

import lombok.Getter;
import org.ekbana.broker.segment.*;
import org.ekbana.broker.storage.Storage;
import org.ekbana.broker.topic.TopicMetaData;
import org.ekbana.broker.utils.BrokerLogger;
import org.ekbana.broker.utils.FileUtil;
import org.ekbana.broker.utils.KafkaBrokerProperties;
import org.ekbana.minikafka.common.ConsumerRecords;
import org.ekbana.minikafka.common.ProducerRecords;
import org.ekbana.minikafka.common.Record;
import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Recorder class<br/>
 * implements RecordsCallback<Records>,SegmentCallback<br/>
 * manages all the records
 * */

@Getter
public class Recorder implements RecordsCallback<ProducerRecords>, SegmentCallback {
//    private final BrokerConfig brokerConfig;
    private final String topicName;
    private final TopicMetaData topicMetaData;
    private final Policy<SegmentMetaData> segmentPolicy;
    private final Policy<ConsumerRecords> consumerRecordBatchPolicy;
    private Segment activeSegment;
    private Segment passiveSegment;
    private final SegmentSearchTree segmentSearchTree;
    
    private final KafkaBrokerProperties kafkaBrokerProperties;

//    public Recorder(BrokerConfig brokerConfig,String topicName, TopicMetaData topicMetaData, Policy<Segment> segmentPolicy, Policy<Records> consumerRecordBatchPolicy, SegmentSearchTree segmentSearchTree) {
//        this.brokerConfig=brokerConfig;
//        this.topicName = topicName;
//        this.topicMetaData = topicMetaData;
//        this.segmentPolicy = segmentPolicy;
//        this.consumerRecordBatchPolicy = consumerRecordBatchPolicy;
//        this.activeSegment = SegmentService.createActiveSegment(topicName,topicMetaData.getActiveSegmentMetaData());
//        this.passiveSegment = topicMetaData.getPassiveSegmentMetaData()!=null?SegmentService.createActiveSegment(topicName,topicMetaData.getPassiveSegmentMetaData()):null;
//        this.segmentSearchTree=segmentSearchTree;
//        updateTopicMetaData();
//    }

    public Recorder(KafkaBrokerProperties kafkaBrokerProperties, String topicName, TopicMetaData topicMetaData, Policy<SegmentMetaData> segmentPolicy, Policy<ConsumerRecords> consumerRecordBatchPolicy, SegmentSearchTree segmentSearchTree) {
        this.kafkaBrokerProperties=kafkaBrokerProperties;
        this.topicName = topicName;
        this.topicMetaData = topicMetaData;
        this.segmentPolicy = segmentPolicy;
        this.consumerRecordBatchPolicy = consumerRecordBatchPolicy;
        this.activeSegment = SegmentService.createActiveSegment(kafkaBrokerProperties,topicName,topicMetaData.getActiveSegmentMetaData());
        this.passiveSegment = topicMetaData.getPassiveSegmentMetaData()!=null?SegmentService.createActiveSegment(kafkaBrokerProperties,topicName,topicMetaData.getPassiveSegmentMetaData()):null;
        this.segmentSearchTree=segmentSearchTree;
        updateTopicMetaData();
    }

    @Override
    public void records(ProducerRecords producerRecords) {
        producerRecords.stream().forEach(record -> {
            BrokerLogger.producerLogger.debug(" [Inserting data to topic - {}] : {}",topicName,activeSegment.addRecord(record));
            if (!segmentPolicy.validate(activeSegment.getSegmentMetaData())){
                long offset=topicMetaData.getActiveSegmentMetaData().getCurrentOffset();
                topicMetaData.setPassiveSegmentMetaData(topicMetaData.getActiveSegmentMetaData());
                final SegmentMetaData activeSegmentMetaData = SegmentMetaData.builder()
                        .segmentId(offset + 1)
                        .build();
                topicMetaData.setActiveSegmentMetaData(activeSegmentMetaData);
                // set passive segment as current active segment
                passiveSegment=activeSegment;
                //creation of new active record segment
                activeSegment=SegmentService.createActiveSegment(kafkaBrokerProperties,topicName,topicMetaData.getActiveSegmentMetaData());
                // update the topicMetaData
                updateTopicMetaData();

                SegmentService.registerTask(new SegmentTask(passiveSegment.getSegmentMetaData(),this));

                BrokerLogger.producerLogger.debug(" [Topic : {}] [passive segment metadata : {}] [active segment Metadata : {}]",topicName,passiveSegment.getSegmentMetaData(),activeSegment.getSegmentMetaData());
//                System.out.println(passiveSegment.getSegmentMetaData());
//                System.out.println(activeSegment.getSegmentMetaData());
                // move current active segment to passive segment

            }
        });
    }

    /**
     * update the TopicMetaData <br/>
     * writes the topic info to the disk
     * */
    public void updateTopicMetaData(){
        try {
//            FileUtil.writeObjectToFile(brokerConfig.rootPath()+brokerConfig.dataPath()+topicName+"/"+brokerConfig.topicMataDataFileName(),topicMetaData);
            FileUtil.writeObjectToFile(kafkaBrokerProperties.getRootPath()+kafkaBrokerProperties.getDataPath()+topicName+"/"+kafkaBrokerProperties.getTopicMataDataFileName(),topicMetaData);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * get records <br/>
     * selection of segment is performed
     * @param offset        offset from which record is to be fetched
     * @param isTimeOffset  determine the type of offset
     * @return records
     * */
    public ConsumerRecords getRecords(long offset,boolean isTimeOffset){

        if ((offset>=activeSegment.getSegmentMetaData().getCurrentOffset() && isTimeOffset)
            ||(offset>activeSegment.getSegmentMetaData().getCurrentTimeStamp() && isTimeOffset)){
            return null;
        }else {
            if (topicMetaData.getActiveSegmentMetaData().getOffsetCount()>0
                    && ((offset>=topicMetaData.getActiveSegmentMetaData().getStartingOffset() && !isTimeOffset)
                || (offset>=topicMetaData.getActiveSegmentMetaData().getStartingTimeStamp() && isTimeOffset))){
                // search in active segment
                return fetchRecords(offset,isTimeOffset,topicMetaData.getActiveSegmentMetaData());
            }else if (passiveSegment!=null &&
                    ((offset>=topicMetaData.getPassiveSegmentMetaData().getStartingOffset() && !isTimeOffset)
                    || (offset>=topicMetaData.getPassiveSegmentMetaData().getStartingTimeStamp() && isTimeOffset))){
                // search in active segment
                return fetchRecords(offset,isTimeOffset,topicMetaData.getPassiveSegmentMetaData());
            }else {
                final SegmentMetaData segmentMetaData = segmentSearchTree.searchSegment(offset, isTimeOffset);
                return segmentMetaData==null
                        ? (passiveSegment != null
                            ? fetchRecords(offset, isTimeOffset, topicMetaData.getActiveSegmentMetaData())
                            : fetchRecords(offset, isTimeOffset, topicMetaData.getActiveSegmentMetaData()))
                        :fetchRecords(offset,isTimeOffset, segmentMetaData);
            }
        }
    }

    /**
    * fetch records
    * @param offset                 offset from which record is be fetched
     * @param isTimeStamp           determine the type of offset
     * @param segmentMetaData       information about segment from which record is to be fetched
     * @return records
    **/
    private ConsumerRecords fetchRecords(long offset,boolean isTimeStamp,SegmentMetaData segmentMetaData){
//        System.out.println("\n\nreader : "+segmentMetaData);
        BrokerLogger.consumerLogger.debug("Topic reader : {}",segmentMetaData);
        ConsumerRecords records=new ConsumerRecords(new ArrayList<>());
        if (segmentMetaData==null) return records;
        Storage<Record> storage=SegmentService.getStorage(kafkaBrokerProperties,topicName,segmentMetaData);

        if (isTimeStamp){
            // TODO :
            //  find the nearest offset
            offset=segmentMetaData.getStartingOffset();
        }

        offset=Math.max(offset,segmentMetaData.getStartingOffset());
        while (offset<=segmentMetaData.getCurrentOffset() && consumerRecordBatchPolicy.validate(records)){
            records.addRecord(storage.get(offset));
            offset=offset+1;
        }
        return records;
    }

    @Override
    public void onSegmentProcessSuccess(SegmentMetaData segmentMetaData) {
        // TODO :
        // clear the passive segment data
        // update the segment-offset data
//        System.out.println("segment service : " + segmentMetaData);
        BrokerLogger.brokerLogger.debug(" Adding segment - topic : {}  segment meta data : {} ",topicName,segmentMetaData);
        segmentSearchTree.addSegment(segmentMetaData);
        segmentSearchTree.reEvaluate();
//        System.out.println("\n\n Segment transversal");
//        segmentSearchTree.transverse().forEach(System.out::println);

//        try {
////            FileUtil.writeStreamToFile(brokerConfig.rootPath()+brokerConfig.dataPath()+topicName+"/"+brokerConfig.segmentFileName(),segmentSearchTree.transverse(),SegmentMetaData.class);
//            FileUtil.writeStreamToFile(kafkaBrokerProperties.getRootPath()+kafkaBrokerProperties.getDataPath()+topicName+"/"+kafkaBrokerProperties.getSegmentFileName(),segmentSearchTree.transverse(),SegmentMetaData.class);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        // write tree to file
        segmentSearchTree.dumpTreeToFile(kafkaBrokerProperties.getRootPath()+kafkaBrokerProperties.getDataPath()+topicName+"/"+kafkaBrokerProperties.getSegmentFileName());
    }

    @Override
    public void onSegmentProcessFail() {
        // notify the failure of  segment movement
    }


}
