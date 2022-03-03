//package org.ekbana.broker.Policy;
//
//import lombok.Getter;
//import lombok.RequiredArgsConstructor;
//import org.ekbana.broker.segment.Segment;
//
//@Getter
//@RequiredArgsConstructor
//public class SegmentPolicy implements Policy<Segment>{
//    private final long segmentSizeInBytes;
//    private final long segmentTimeInSec;
//    private final long segmentOffsetCount;
//
//    @Override
//    public boolean validate(Segment activeSegment) {
////        return activeSegment.getSegmentMetaData().getOffsetCount()<segmentOffsetCount;
////        return activeSegment.getSegmentMetaData().getSegmentSize()<segmentSizeInBytes;
//        return activeSegment.getSegmentMetaData().getOffsetCount()<segmentOffsetCount;
//    }
//}
