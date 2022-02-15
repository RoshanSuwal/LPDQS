package org.ekbana.broker.storage.memory;

import org.ekbana.broker.record.Record;
import org.ekbana.broker.storage.Storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class InMemoryStorage<T extends Record> implements Storage<T> {
    private final List<T> arrayList=new ArrayList<>();
    private long size=0;

    // policy

    HashMap<Integer,MemorySegment<T>> memorySegmentHashMap=new HashMap<>();
    private MemorySegment<T> activeMemorySegment;

    @Override
    public void store(T t) {
        arrayList.add(t);
        this.size=this.size+t.size();
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public long count() {
        return arrayList.size();
    }

    @Override
    public T get(long position) {
        return arrayList.get((int) position);
    }
}
