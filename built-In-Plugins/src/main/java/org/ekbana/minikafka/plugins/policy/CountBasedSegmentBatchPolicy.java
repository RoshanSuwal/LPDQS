package org.ekbana.minikafka.plugins.policy;

import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;

public class CountBasedSegmentBatchPolicy implements Policy<SegmentMetaData> {
    private final int offsetCount;

    public CountBasedSegmentBatchPolicy(int offsetCount) {
        this.offsetCount = offsetCount;
    }

    @Override
    public boolean validate(SegmentMetaData segmentMetaData) {
        return segmentMetaData.getOffsetCount()<offsetCount;
    }
}
