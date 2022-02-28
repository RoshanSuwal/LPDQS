package org.ekbana.minikafka.plugins.policy;

import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;


public class SizeBasedSegmentBatchPolicy implements Policy<SegmentMetaData> {
    private final Long segmentSize;
    public SizeBasedSegmentBatchPolicy(Long segmentSize) {
        this.segmentSize = segmentSize;
    }

    @Override
    public boolean validate(SegmentMetaData segmentMetaData) {
        return segmentMetaData.getSegmentSize()<segmentSize;
    }
}
