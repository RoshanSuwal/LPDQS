package org.ekbana.broker.record;

import java.util.List;
import java.util.stream.Stream;

public class Records {
    private final List<Record> records;
    private int size;

    public Records(List<Record> records){
        this.records=records;
        this.size=records.stream().mapToInt(Record::size).sum();
    }
    public Stream<Record> stream(){
        return records.stream();
    }

    public void addRecord(Record record){
        this.records.add(record);
        size=size+record.size();
    }

    public int count(){return records.size();}
    public int size() {
        return size;
    }
}
