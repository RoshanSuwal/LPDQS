package org.ekbana.broker.segment;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.time.Instant;

@Getter @Setter
@Builder
@ToString
public class SegmentMetaData implements Serializable {
    private long segmentId;
    private long startingOffset;
    private long startingTimeStamp;
    private long currentOffset;
    private long currentTimeStamp;
    private long segmentSize;
    private long offsetCount;

    public long addRecordMetaData(long size_of_record){
        if (offsetCount==0){
            startingOffset=segmentId;
            currentOffset=segmentId;
            startingTimeStamp= Instant.now().toEpochMilli();
        }else {
            currentOffset=currentOffset+1;
        }
        currentTimeStamp= Instant.now().toEpochMilli();

        this.offsetCount=this.offsetCount+1;
        this.segmentSize=this.segmentSize+size_of_record;

        return currentOffset;
    }
}
