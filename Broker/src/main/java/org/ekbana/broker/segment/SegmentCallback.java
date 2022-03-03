package org.ekbana.broker.segment;

import org.ekbana.minikafka.common.SegmentMetaData;

public interface SegmentCallback {
    //called when passive segment successfully moved to inactive segment
    void onSegmentProcessSuccess(SegmentMetaData segmentMetaData);
    //called when passive segment failed to move to inactive segment
    void onSegmentProcessFail();
}
