package org.ekbana.bigdata.retention_service;

import org.ekbana.minikafka.common.FileUtil;
import org.ekbana.minikafka.common.SegmentMetaData;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RetentionService {
    /**
     * 1. scan through the data path and grab all the topics
     * 2. get all topic segment.txt files
     * 3. scan each row and validate with current time stamp
     * 4. remove all the files not fulfilling the retention policy
     */

    public static final Logger logger= LoggerFactory.getLogger(RetentionService.class);
    private RetentionProperties retentionProperties;

    public RetentionService(RetentionProperties retentionProperties) {
        this.retentionProperties = retentionProperties;
    }

    public List<String> getAllTopics() {
        logger.info("fetching all topics");
        try {
            return FileUtil.getDirectories(retentionProperties.getRetentionProperty("kafka.storage.data.path") + "data/")
                    .map(file->file.getName())
                    .peek(file->logger.info("Found Topic : {}",file))
                    .toList();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    public SegmentMetaData getOldestSegmentMetaDataForTopic(String topic){
        try {
            final List<?> objects = FileUtil.readAllLines(
                    retentionProperties.getRetentionProperty("kafka.storage.data.path")
                            +retentionProperties.getRetentionProperty("kafka.broker.data.path")
                            + topic + "/"
                            + retentionProperties.getRetentionProperty("kafka.topic.segment.filename"),
                    SegmentMetaData.class
            );

            if (objects.size()>0) return (SegmentMetaData) objects.get(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void removeAllFilesBelowGivenOffset(String topic,SegmentMetaData segmentMetaData){
        try {
            FileUtil.getDirectories(
                    retentionProperties.getRetentionProperty("kafka.storage.data.path")
                            + "data/"
                            + topic + "/"
            ).filter(file -> Long.parseLong(file.getName())<segmentMetaData.getStartingOffset())
                    .peek(file ->logger.info(" Deleting segment : {} of topic :{}",file.getName(),topic) )
                    .forEach(file -> FileUtil.deleteDirectory(file.getPath()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        getAllTopics().forEach(topic->{
            final SegmentMetaData oldestSegmentMetaDataForTopic = getOldestSegmentMetaDataForTopic(topic);
            if (oldestSegmentMetaDataForTopic!=null) removeAllFilesBelowGivenOffset(topic,oldestSegmentMetaDataForTopic);
        });
    }

    public static void main(String[] args) {
        logger.info("Starting RetentionService");
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping RetentionService...");
        }));

        final String config = System.getProperty("config");

        if (config==null){
            logger.error("Config path in null, please specify it");
            System.exit(0);
        }

        try {
            RetentionProperties retentionProperties=new RetentionProperties(config+"/kafka.properties");
            RetentionService retentionService=new RetentionService(retentionProperties);
//            retentionService.run();

            final int retentionScheduleInterval = Integer.parseInt(retentionProperties.getRetentionProperty("kafka.retention.schedule.period"));
            logger.info("RetentionService scheduled at interval of {} secs",retentionScheduleInterval);

            final Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();

            final SimpleTrigger brokerTrigger = TriggerBuilder.newTrigger()
                    .withIdentity("RetentionService scheduler")
                    .startNow()
                    .withSchedule(
                            SimpleScheduleBuilder
                                    .simpleSchedule()
                                    .withIntervalInSeconds(retentionScheduleInterval)
                    .repeatForever()
                ).build();

            JobDataMap jobDataMap=new JobDataMap();
            jobDataMap.putIfAbsent("retention",retentionService);
            JobDetail jobDetail=JobBuilder
                    .newJob(RetentionServiceScheduleJob.class)
                    .setJobData(jobDataMap)
                    .withIdentity("retention Schedule Job")
                    .build();

            scheduler.scheduleJob(jobDetail,brokerTrigger);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }
}
