package org.ekbana.broker.segment.search;

import lombok.Getter;
import lombok.Setter;
import org.ekbana.broker.utils.BrokerLogger;
import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;

import java.util.concurrent.atomic.AtomicBoolean;

@Getter
@Setter
public class Leaf {
    private SegmentMetaData segmentMetaData;
    private Node node;

    public Leaf(SegmentMetaData segmentMetaData,Node node) {
        this.segmentMetaData = segmentMetaData;
        this.node=node;
    }

    public SegmentMetaData search(long offset, boolean isTimeStamp) {
        if (node.getStatus().get()) {
            if (!isTimeStamp)
                return offset <= segmentMetaData.getCurrentOffset() ? segmentMetaData : null;
            else
                return offset <= segmentMetaData.getCurrentTimeStamp() ? segmentMetaData : null;
        }else {
            return null;
        }
    }

    public void print() {
        System.out.println(segmentMetaData.getStartingOffset());
    }

    public SegmentMetaData transverse(){
        return  segmentMetaData;
    }

    public void evaluateLeafAvailabilityStatus(Policy<SegmentMetaData> policy) {
        if (!policy.validate(node.getSegmentMetaData())){
            this.node.setStatus(false);
        }
        BrokerLogger.searchTreeLogger.info("leaf availability status : {}",node.getStatus().get());
    }

    public void setLeafAvailabilityStatus(boolean status){
        this.node.setStatus(false);
    }
}
