package org.ekbana.server.v2.scheduler;

import org.ekbana.broker.Broker;
import org.ekbana.server.util.KafkaLogger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class BrokerSchedulerJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        KafkaLogger.nodeLogger.debug("Broker Schedule Job Started..");
        final JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        final Broker broker = (Broker) jobDataMap.get("broker");
        broker.saveAllTopicToDisk();
        KafkaLogger.nodeLogger.debug("Broker Schedule Job Ended..");
    }
}
