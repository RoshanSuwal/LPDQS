package org.ekbana.broker.Policy;

import org.ekbana.broker.segment.SegmentMetaData;

public class RetentionPolicy implements Policy<SegmentMetaData> {

    @Override
    public boolean validate(SegmentMetaData segmentMetaData) {
        return false;
    }
}
