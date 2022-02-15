package org.ekbana.server.transaction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Scheduler {
    // schedules the activities at intervals
    HashMap<Long, List<Long>> scheduleTasks = new HashMap<>();
    HashMap<Long, ScheduleTask> scheduleTaskMap = new HashMap<>();
    private static long taskId = 0;
    private final AtomicBoolean atomicBoolean=new AtomicBoolean(false);

    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    public void register(long interval, SchedulerCallBack schedulerCallBack) {
        scheduleTaskMap.put(taskId, new ScheduleTask(schedulerCallBack, interval));
        scheduleRegistry(taskId);
        taskId = taskId + 1;

    }

    public void scheduleRegistry(long taskId) {
        final ScheduleTask scheduleTask = scheduleTaskMap.get(taskId);
        final long key = Instant.now().getEpochSecond() + scheduleTask.interval;
        if (!scheduleTasks.containsKey(key)) {
            scheduleTasks.put(key, new ArrayList<>());
        }
        final List<Long> value = scheduleTasks.get(key);
        value.add(taskId);
        scheduleTasks.put(key, value);

        if (!atomicBoolean.get()) scheduler();
    }

    private void scheduler() {
        executorService.execute(() -> {
            atomicBoolean.set(true);
            while (scheduleTasks.keySet().size() > 0) {
                long current = Instant.now().getEpochSecond();
                scheduleTasks.keySet().stream()
                        .filter(ket -> ket <= current)
                        .map(key -> {
                            final List<Long> longs = scheduleTasks.get(key);
                            scheduleTasks.remove(key);
                            return longs;
                        })
                        .flatMap(Collection::stream)
                        .forEach(key ->
                            CompletableFuture.supplyAsync(() -> scheduleTaskMap.get(key).getSchedulerCallBack().scheduleExecute(key), executorService)
                                    .thenAccept(this::scheduleRegistry));
            }
            atomicBoolean.set(false);
        });
    }

    private static class ScheduleTask {
        private final SchedulerCallBack schedulerCallBack;
        private final long interval;

        public ScheduleTask(SchedulerCallBack schedulerCallBack, long interval) {
            this.schedulerCallBack = schedulerCallBack;
            this.interval = interval;
        }

        public SchedulerCallBack getSchedulerCallBack() {
            return schedulerCallBack;
        }

        public long getInterval() {
            return interval;
        }
    }

    public abstract static class SchedulerCallBack{
        private long scheduleExecute(long taskId){
            schedule();
            return taskId;
        }
        protected abstract void schedule();
    }
}
