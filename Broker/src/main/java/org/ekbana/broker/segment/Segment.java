package org.ekbana.broker.segment;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.ekbana.broker.storage.Storage;
import org.ekbana.broker.record.Record;
import org.ekbana.broker.record.Records;

import java.util.Collections;

@RequiredArgsConstructor
@Getter
@Setter
public class Segment {
    private final SegmentMetaData segmentMetaData;
    private final Storage<Record> storage;

    public void addRecord(Record record) {
        System.out.println(Thread.currentThread().getName() + " :" + record.getData() + " : " + storage.size());
        final long offset = segmentMetaData.addRecordMetaData(record.size());
        record.setOffset(offset);
        storage.store(record);
    }

    public boolean hasRecord(long offset) {
        return segmentMetaData.getOffsetCount() > 0 && (offset >= segmentMetaData.getStartingOffset() && offset <= segmentMetaData.getCurrentOffset());
    }

    public Records getRecords(long offset,boolean isTimeOffset){
        //convert timestamp to record offset
        return new Records(Collections.singletonList(storage.get(offset)));
    }

    public Record getRecord(long offset){
        return storage.get(offset);
    }
}
