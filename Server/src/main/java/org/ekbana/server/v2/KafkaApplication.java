package org.ekbana.server.v2;

import ch.qos.logback.classic.util.ContextInitializer;
import org.ekbana.minikafka.common.FileUtil;
import org.ekbana.server.util.KafkaLogger;
import org.quartz.SchedulerException;

import java.io.IOException;

public class KafkaApplication {
    public static void main(String[] args) throws IOException, SchedulerException {
        final String mode = System.getProperty("mode");
        System.out.println("mode : "+mode);
        if (mode!=null){

            final String configPath = System.getProperty("config");
            if (!FileUtil.exists(configPath)) {
                KafkaLogger.kafkaLogger.error("Config-path : [{}] does not exists ", configPath);
                System.exit(0);
            }

            if (FileUtil.exists(configPath+"/logback.xml")) {
                System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, configPath + "/logback.xml");
            }else {
                KafkaLogger.kafkaLogger.error("[Failed to configure log] : {} file does not exists",configPath + "/logback.xml");
                System.exit(0);
            }

            if (mode.trim().equals("leader")){
                LeaderApplication.run(args);
            } else if (mode.trim().equals("node")){
                DataNodeApplication.run(args);
            }else {
                System.out.println("Invalid mode specified. Must be either leader or node");
            }
        }else {
            System.out.println("running mode not specified.");
        }
    }
}
