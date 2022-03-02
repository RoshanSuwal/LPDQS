package org.ekbana.minikafka.plugins.policy;

import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;

import java.time.Instant;

public class TimeBasedSegmentRetentionPolicy implements Policy<SegmentMetaData> {

    private final long segmentLifeSpan;

    public TimeBasedSegmentRetentionPolicy(long segmentLifeSpan) {
        this.segmentLifeSpan = segmentLifeSpan;
    }

    @Override
    public boolean validate(SegmentMetaData segmentMetaData) {
        return Instant.now().toEpochMilli()-segmentMetaData.getCurrentTimeStamp() < segmentLifeSpan;
    }
}
