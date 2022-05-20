package org.ekbana.bigdata.retention_service;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import static org.ekbana.bigdata.retention_service.RetentionService.logger;

public class RetentionServiceScheduleJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        logger.info("retention schedule job started...");
        final JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        final RetentionService retentionService = (RetentionService) jobDataMap.get("retention");
        retentionService.run();
        logger.info("retention schedule job ended...");

    }
}
