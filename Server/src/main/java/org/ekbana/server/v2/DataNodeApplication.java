package org.ekbana.server.v2;

import ch.qos.logback.classic.util.ContextInitializer;
import org.ekbana.broker.Broker;
import org.ekbana.broker.utils.BrokerLogger;
import org.ekbana.broker.utils.KafkaBrokerProperties;
import org.ekbana.minikafka.common.FileUtil;
import org.ekbana.server.KafkaLoader;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.KafkaLogger;
import org.ekbana.server.util.Serializer;
import org.ekbana.server.v2.datanode.DataNodeController;
import org.ekbana.server.v2.datanode.DataNodeServerClient;
import org.ekbana.server.v2.scheduler.BrokerSchedulerJob;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataNodeApplication {

    public static void run(String[] args) throws IOException, SchedulerException {

        KafkaLogger.nodeLogger.info("Running as Data Node");

        final String configPath = System.getProperty("config");
//        if (!FileUtil.exists(configPath)) {
//            KafkaLogger.leaderLogger.error("Config-path : [{}] does not exists ", configPath);
//            System.exit(0);
//        }
//
//        System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, configPath+"/logback-classic.xml");

        final KafkaProperties kafkaProperties;
        final KafkaBrokerProperties kafkaBrokerProperties;

        if (!FileUtil.exists(configPath + "/kafka.properties")) {
            final Properties properties = new Properties();
//            properties.setProperty("kafka.server.node.id", "node-0");
//            properties.setProperty("kafka.storage.data.path", "log2/");
            kafkaProperties = new KafkaProperties(properties);
            kafkaBrokerProperties = new KafkaBrokerProperties(properties);
        } else {
            kafkaProperties = new KafkaProperties(configPath + "/kafka.properties");
            kafkaBrokerProperties = new KafkaBrokerProperties(configPath + "/kafka.properties");
        }

//        if (!FileUtil.exists(configPath+"broker.properties")){
//            final Properties brokerProperties = new Properties();
//            brokerProperties.setProperty("kafka.broker.root.path","log/");
//            kafkaBrokerProperties=new KafkaBrokerProperties(brokerProperties);
//        }else {
//            kafkaBrokerProperties=new KafkaBrokerProperties(configPath+"/broker.properties");
//        }

        Serializer serializer = new Serializer();
        Deserializer deserializer = new Deserializer();
        ExecutorService brokerExecutorService = Executors.newFixedThreadPool(10);
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        KafkaLoader kafkaLoader = new KafkaLoader("plugins");
        kafkaLoader.load();

        Broker broker = new Broker(kafkaBrokerProperties,
                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.batch.policy")).buildPolicy(kafkaBrokerProperties),
                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.retention.policy")).buildPolicy(kafkaBrokerProperties),
                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.consumer.record.batch.policy")).buildPolicy(kafkaBrokerProperties),
                brokerExecutorService);
        broker.load();

        DataNodeServerClient dataNodeServerClient = new DataNodeServerClient(kafkaProperties);
        DataNodeController dataNodeController = new DataNodeController(
                serializer,
                deserializer,
                broker,
                kafkaProperties,
                dataNodeServerClient,
                executorService
        );


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            BrokerLogger.brokerLogger.info("Stopping Data Node Application started");
            broker.onStop();
            BrokerLogger.brokerLogger.info("Stopping Data Node Application completed");
        }));

        new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            while (true) {
                final String nextLine = scanner.nextLine();
                System.out.println("line : " + nextLine);
                if (nextLine.equals("C")) break;
            }
            System.exit(0);
        }).start();


        final Scheduler scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.start();

        KafkaLogger.nodeLogger.debug("Broker Scheduler scheduled at interval of {} sec",kafkaBrokerProperties.getBrokerProperty("kafka.broker.schedule.interval"));
        final SimpleTrigger brokerTrigger = TriggerBuilder.newTrigger()
                .withIdentity("broker scheduler")
                .startNow()
                .withSchedule(
                        SimpleScheduleBuilder
                                .simpleSchedule()
                                .withIntervalInSeconds(Integer.parseInt(kafkaBrokerProperties.getBrokerProperty("kafka.broker.schedule.interval")))
                                .repeatForever()
                ).build();

        JobDataMap jobDataMap=new JobDataMap();
        jobDataMap.putIfAbsent("broker",broker);
        JobDetail jobDetail=JobBuilder
                .newJob(BrokerSchedulerJob.class)
                .setJobData(jobDataMap)
                .withIdentity("Broker Schedule Job")
                .build();

        scheduler.scheduleJob(jobDetail,brokerTrigger);

        while (true) {
            try {
                KafkaLogger.networkLogger.info("Connecting to server");
                dataNodeServerClient.connect(executorService);
            } catch (IOException e) {
//                e.printStackTrace();
                KafkaLogger.networkLogger.error(e.getMessage());
            }
            final long reconnectInterval = Long.parseLong(kafkaBrokerProperties.getBrokerProperty("kafka.server.reconnect.interval"));
            KafkaLogger.dataNodeLogger.info("Reconnecting after {} sec",reconnectInterval);
            try {
                Thread.sleep(reconnectInterval*1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
