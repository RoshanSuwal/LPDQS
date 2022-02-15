package org.ekbana.broker.segment;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SegmentTask implements Runnable {
    private final SegmentMetaData segmentMetaData;
    private final SegmentCallback segmentCallback;

    // has separate index file writer and data file writer

    @Override
    public void run() {
        segmentCallback.onSegmentProcessSuccess(segmentMetaData);
        System.out.println(Thread.currentThread().getName()+" : "+SegmentTask.class.getSimpleName()+" : "+segmentMetaData.toString());
    }
}
