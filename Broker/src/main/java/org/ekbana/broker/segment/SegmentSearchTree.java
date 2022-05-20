package org.ekbana.broker.segment;

import org.ekbana.broker.segment.search.Leaf;
import org.ekbana.broker.segment.search.Node;
import org.ekbana.broker.segment.search.RootNode;
import org.ekbana.broker.utils.BrokerLogger;
import org.ekbana.broker.utils.FileUtil;
import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SegmentSearchTree {
    private final RootNode rootNode;
    private final Policy<SegmentMetaData> segmentRetentionPolicy;

    public SegmentSearchTree(int n, Policy<SegmentMetaData> segmentRetentionPolicy) {
        this.segmentRetentionPolicy = segmentRetentionPolicy;
        rootNode=new RootNode(n);
    }

    public void addSegment(SegmentMetaData segmentMetaData){
        rootNode.addLeaf(
                createNode(segmentMetaData),
                new Leaf(segmentMetaData,createNode(segmentMetaData))
        );
        // update segment retention policy criteria value if needed
    }

    private Node createNode(SegmentMetaData segmentMetaData){
        return Node.builder()
                .segmentMetaData(segmentMetaData)
                .criteria(segmentMetaData.getStartingTimeStamp()) // may be size, timestamp, offset
                .status(new AtomicBoolean(true))
                .build();
    }

    public SegmentMetaData searchSegment(long searchValue, boolean isTimeStamp){
//        if (segmentRetentionPolicy!=null)segmentRetentionPolicy.setCompareWith(Instant.now().toEpochMilli());
//        return rootNode.search(searchValue,isTimeStamp,segmentRetentionPolicy);
        return rootNode.search(searchValue,isTimeStamp);
    }

    public void reEvaluate(){
        if (segmentRetentionPolicy!=null) {
//            segmentRetentionPolicy.setCompareWith(Instant.now().toEpochMilli());
            BrokerLogger.searchTreeLogger.debug("Segment Search Tree : Re-evaluation started..");
            rootNode.reEvaluate(segmentRetentionPolicy);
            BrokerLogger.searchTreeLogger.debug("Segment Search Tree : Re-evaluation ended..");
        }
    }

    public void evaluateNodeAvailabilityStatus(){
        if (segmentRetentionPolicy!=null){
            BrokerLogger.searchTreeLogger.debug("Evaluation of Node Availability Status Started...");
            rootNode.evaluateNodeAvailabilityStatus(segmentRetentionPolicy);
            BrokerLogger.searchTreeLogger.debug("Evaluation of Node Availability Status Ended..");
        }
    }

    public void removeUnAvailableNodes(){
        BrokerLogger.searchTreeLogger.debug("Removing Unavailable Status Started...");
        rootNode.removeUnAvailableNodes();
        BrokerLogger.searchTreeLogger.debug("Removing Unavailable ended...");

    }

    public List<SegmentMetaData> transverse(){
        return rootNode.transverse().collect(Collectors.toList());
    }

    public void dumpTreeToFile(String path){
        try {
            FileUtil.writeStreamToFile(path,this.transverse(),SegmentMetaData.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
