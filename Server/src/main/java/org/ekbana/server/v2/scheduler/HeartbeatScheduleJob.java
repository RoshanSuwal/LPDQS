package org.ekbana.server.v2.scheduler;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class HeartbeatScheduleJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // get all the details of active nodes

    }
}
