package org.ekbana.broker.segment;

import org.ekbana.broker.segment.search.Leaf;
import org.ekbana.broker.segment.search.Node;
import org.ekbana.broker.segment.search.RootNode;
import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;

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
                new Leaf(segmentMetaData)
        );
        // update segment retention policy criteria value if needed
        reEvaluate();
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
        return rootNode.search(searchValue,isTimeStamp,segmentRetentionPolicy);
    }

    public void reEvaluate(){
        if (segmentRetentionPolicy!=null) {
//            segmentRetentionPolicy.setCompareWith(Instant.now().toEpochMilli());
            rootNode.reEvaluate(segmentRetentionPolicy);
        }
    }

    public List<SegmentMetaData> transverse(){
        return rootNode.transverse().collect(Collectors.toList());
    }
}
