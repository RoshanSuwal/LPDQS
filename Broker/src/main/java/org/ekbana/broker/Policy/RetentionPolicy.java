package org.ekbana.broker.Policy;

import org.ekbana.broker.segment.SegmentMetaData;

import java.time.Instant;

public class RetentionPolicy implements Policy<SegmentMetaData> {

    long lifeSpan=60*1000;
    @Override
    public boolean validate(SegmentMetaData segmentMetaData) {
        return Instant.now().toEpochMilli()-segmentMetaData.getCurrentTimeStamp() < lifeSpan;
//        return false;
    }
}
