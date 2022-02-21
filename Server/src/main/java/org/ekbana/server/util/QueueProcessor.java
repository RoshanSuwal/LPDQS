package org.ekbana.server.util;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueProcessor<T> {
    private final BlockingDeque<T> blockingDeque;
    private final ExecutorService executorService;

    private AtomicBoolean atomicBoolean;
    private final QueueProcessorListener<T> processorListener;

    public QueueProcessor(int queueSize, QueueProcessorListener<T> processorListener, ExecutorService executorService) {
        this.blockingDeque = new LinkedBlockingDeque<>(queueSize);
        this.processorListener = processorListener;
        this.executorService = executorService;
        atomicBoolean=new AtomicBoolean(false);
    }

    public void push(T t, boolean onTop){
        try {
            if (onTop) blockingDeque.putFirst(t);
            else blockingDeque.putLast(t);

            if (!atomicBoolean.get() && processorListener!=null){
                atomicBoolean.set(true);
                executorService.execute(this::startPolling);
            }
        }catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startPolling(){
        while (blockingDeque.size()>0){
            processorListener.process(blockingDeque.poll());
        }
        atomicBoolean.set(false);
    }

    public interface QueueProcessorListener<T>{
        void process(T t);
    }
}
