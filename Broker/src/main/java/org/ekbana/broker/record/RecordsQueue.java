package org.ekbana.broker.record;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter @Setter
public class RecordsQueue<T> {
    private final BlockingDeque<T> blockingDeque;
    private final AtomicBoolean atomicBoolean;
    private final RecordsCallback<T> recordsCallback;
    private final ExecutorService executorService;

    public RecordsQueue(RecordsCallback<T> recordsCallback){
        this(1000,recordsCallback);
    }

    public RecordsQueue(int queueSize,RecordsCallback<T> recordsCallback) {
        this(queueSize,recordsCallback,null);
    }

    public RecordsQueue(int queueSize,RecordsCallback<T> recordsCallback,ExecutorService executorService) {
        this.blockingDeque = new LinkedBlockingDeque<>(queueSize);
        this.atomicBoolean = new AtomicBoolean(false);
        this.recordsCallback=recordsCallback;
        this.executorService=executorService;
    }

    public void add(T t){
        blockingDeque.add(t);
        if (recordsCallback!=null && !atomicBoolean.get()) process();
    }

    private void startPolling(){
        System.out.println("polling started");
        this.atomicBoolean.set(true);
        T t;
        try {
            while ((t = blockingDeque.poll(5, TimeUnit.SECONDS)) != null) {
                recordsCallback.records(t);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            System.out.println("polling finished");
            this.atomicBoolean.set(false);
        }

        System.out.println("closing thread "+Thread.currentThread().getName());
    }

    private void process() {
        if (executorService!=null)
            executorService.execute(this::startPolling);
        else new Thread(this::startPolling).start();

        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
