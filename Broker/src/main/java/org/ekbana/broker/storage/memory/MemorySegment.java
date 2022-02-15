package org.ekbana.broker.storage.memory;

import org.ekbana.broker.record.Record;

import java.util.ArrayList;
import java.util.List;

public class MemorySegment<T extends Record> {
    private final List<T> segmentList=new ArrayList<>();
    private long size=0;

    public void save(T t){
        segmentList.add(t);
        size=size+t.size();
    }

    public long count(){
        return segmentList.size();
    }

    public T get(long position){
        return segmentList.get((int) position);
    }
}
