package org.ekbana.broker.storage;

import org.ekbana.minikafka.common.Record;

public interface Storage<T extends Record> {

    void store(T t);
    long size();
    long count();
    T get(long position);
}
