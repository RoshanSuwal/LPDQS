package org.ekbana.broker.producer;

import lombok.Getter;
import lombok.Setter;
import org.ekbana.broker.record.RecordsCallback;
import org.ekbana.broker.record.RecordsQueue;
import org.ekbana.broker.segment.Segment;
import org.ekbana.broker.record.Records;

@Getter @Setter
public class Producer {
//    private final String topicName;
//    private final TopicMetaData topicMetaData;
    private final RecordsQueue<Records> recordsQueue;
//    private final SegmentPolicy segmentPolicy;
//    private final SegmentCallback segmentCallback;
//    private Segment activeSegment;

    private RecordsCallback<Records> recordsCallback;

    public Producer(RecordsQueue<Records> recordsQueue,RecordsCallback<Records> recordsCallback) {
        this.recordsQueue=recordsQueue;
        this.recordsCallback = recordsCallback;
    }

//    public Producer(String topicName, TopicMetaData topicMetaData) {
//        this(topicName,topicMetaData,100);
//    }
//    public Producer(String topicName,TopicMetaData topicMetaData,int queueSize){
//        this(topicName,topicMetaData,queueSize,new DefaultSegmentPolicy());
//    }
//
//    public Producer(String topicName,TopicMetaData topicMetaData,int queueSize,SegmentPolicy segmentPolicy){
//        // TODO: logic for loading active segment
//        // Active segment is never null
//        this(topicName,topicMetaData,queueSize,segmentPolicy,null);
//    }
//    public Producer(String topicName,TopicMetaData topicMetaData,int queueSize,SegmentPolicy segmentPolicy,ActiveSegment activeSegment){
//        this(topicName,topicMetaData,queueSize,segmentPolicy,activeSegment,null);
//    }
//
//    public Producer(String topicName,TopicMetaData topicMetaData,int queueSize,SegmentPolicy segmentPolicy,ActiveSegment activeSegment,SegmentCallback segmentCallback){
//        this(topicName,topicMetaData,queueSize,segmentPolicy,activeSegment,segmentCallback,null);
//    }

//    public Producer(String topicName,TopicMetaData topicMetaData, int queueSize,SegmentPolicy segmentPolicy,ActiveSegment activeSegment,SegmentCallback segmentCallback,ExecutorService executorService) {
//        this.topicName=topicName;
//        this.topicMetaData = topicMetaData;
//        this.segmentPolicy=segmentPolicy;
//        this.activeSegment=activeSegment;
//        this.segmentCallback=segmentCallback;
//        this.recordsQueue = new RecordsQueue<>(queueSize,this,executorService);
//    }
//
    public void addRecords(Records records){
        this.recordsQueue.add(records);
    }
//
//    @Override
//    public void records(Records records) {
//        // do all the processing stuff of records
//        // segment management
//        System.out.println(Thread.currentThread().getName()+" : records "+records.size());
//        records.stream().forEach(record -> {
//            activeSegment.addRecord(record);
////            topicMetaData.getActiveSegment().addRecordMetaData(activeSegment.addRecord(record));
//            // check with policy
//            if (!segmentPolicy.validate(activeSegment)){
//                // create new Active Segment
//                long offset=topicMetaData.getActiveSegmentMetaData().getCurrentOffset();
//                topicMetaData.setPassiveSegmentMetaData(topicMetaData.getActiveSegmentMetaData());
//                final SegmentMetaData activeSegmentMetaData = SegmentMetaData.builder()
//                        .segmentId(offset + 1)
//                        .build();
//                topicMetaData.setPassiveSegmentMetaData(activeSegmentMetaData);
//                System.out.println(Thread.currentThread().getName()+" : vali : "+activeSegmentMetaData);
//                System.out.println(Thread.currentThread().getName()+" : vali : "+topicMetaData.getPassiveSegmentMetaData());
//
////                topicMetaData.moveActiveToInactiveAndCreateNewActiveSegment();
//                this.activeSegment=SegmentService.createActiveSegment(topicName,topicMetaData.getActiveSegmentMetaData());
//                // give task to segment manager to move passive segment along with handler
//                // TODO : run in separate process
//                //  communicate using TCP
//                //  handle using callbacks
//                SegmentService.registerTask(new SegmentTask(topicMetaData.getActiveSegmentMetaData(),segmentCallback));
//            }
//            try {
//                Thread.sleep(50);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//        //
//    }
//
//    public ActiveSegment getActiveSegment(){
//        return activeSegment;
//    }
//
//    public void onSegmentProcessSuccess(long segmentId) {
//        // validating the passive segment using segmentId == segment starting offset
//        if (topicMetaData.getPassiveSegmentMetaData().getStartingOffset()==segmentId)
//            topicMetaData.setPassiveSegmentMetaData(null);
//        // update segment meta data
//
//    }
}
