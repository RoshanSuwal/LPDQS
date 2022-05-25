package org.ekbana.minikafka.common;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.stream.Stream;

@Getter
@Setter
public class ConsumerRecords {
    private final List<Record> records;
    private int size;
    private long startingOffset=-1;
    private long endingOffset=-1;

    public long getEndingOffset() {
        return endingOffset;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public ConsumerRecords(List<Record> records){
        this.records=records;
        this.size=records.stream().mapToInt(Record::size).sum();
    }

    public Stream<Record> stream(){
        return records.stream();
    }

    public void addRecord(Record record){
        if(record==null) return;
        this.records.add(record);
        size=size+record.size();

        if (startingOffset==-1){
            startingOffset=record.getOffset();
        }
        endingOffset=record.getOffset();

    }

    public int count(){return records.size();}
    public int size() {
        return size;
    }
}
