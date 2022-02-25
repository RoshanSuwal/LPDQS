package org.ekbana.broker.segment.search;

import lombok.Getter;
import lombok.Setter;
import org.ekbana.broker.segment.SegmentMetaData;

@Getter
@Setter
public class Leaf {
    private SegmentMetaData segmentMetaData;

    public Leaf(SegmentMetaData segmentMetaData) {
        this.segmentMetaData = segmentMetaData;
    }

    public SegmentMetaData search(long offset, boolean isTimeStamp) {
        if (!isTimeStamp)
            return  offset <= segmentMetaData.getCurrentOffset() ? segmentMetaData : null;
        else
            return offset <= segmentMetaData.getCurrentTimeStamp() ? segmentMetaData : null;
    }

    public void print() {
        System.out.println(segmentMetaData.getStartingOffset());
    }

    public SegmentMetaData transverse(){
        return  segmentMetaData;
    }
}
