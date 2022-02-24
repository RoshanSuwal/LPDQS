package org.ekbana.server.common.mb;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@ToString
public class ConsumerRecords implements Serializable {
    private final int partitionId;
    private final int count;
    private final long startingOffset;
    private final long endingOffset;
    private final List<?> records;

    public ConsumerRecords(int partitionId,int count, long startingOffset, long endingOffset, List<?> records) {
        this.partitionId=partitionId;
        this.count = count;
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.records = records;
    }
}
