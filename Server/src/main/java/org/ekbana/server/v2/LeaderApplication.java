package org.ekbana.server.v2;

import org.ekbana.minikafka.common.FileUtil;
import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancerFactory;
import org.ekbana.server.KafkaLoader;
import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.KafkaLogger;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.util.Serializer;
import org.ekbana.server.v2.client.KafkaClientController;
import org.ekbana.server.v2.client.KafkaClientRequestParser;
import org.ekbana.server.v2.client.KafkaClientServer;
import org.ekbana.server.v2.common.KafkaRouter;
import org.ekbana.server.v2.leader.LeaderController;
import org.ekbana.server.v2.leader.TopicController;
import org.ekbana.server.v2.node.NodeController;
import org.ekbana.server.v2.node.NodeManager;
import org.ekbana.server.v2.node.NodeServer;
import org.ekbana.server.v2.node.TransactionManager;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LeaderApplication {

    public static void run(String[] args) throws IOException {

        KafkaLogger.leaderLogger.info("Running As Leader");

        final String configPath = System.getProperty("config");
//        if (!FileUtil.exists(configPath)) {
//            KafkaLogger.leaderLogger.error("Config-path : [{}] does not exists ", configPath);
//            System.exit(0);
//        }
//
//        System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, configPath+"/logback-classic.xml");

        final KafkaProperties kafkaProperties;

        if (!FileUtil.exists(configPath + "/kafka.properties")) {
            final Properties properties = new Properties();
//            properties.setProperty("kafka.server.node.id", "node-0");
//            properties.setProperty("kafka.storage.data.path", "log2/");
            kafkaProperties = new KafkaProperties(properties);
        } else {
            kafkaProperties = new KafkaProperties(configPath + "/kafka.properties");
        }

        System.setProperty("bufferSize",kafkaProperties.getKafkaProperty("kafka.socket.buffer.size"));

        if (!FileUtil.exists(kafkaProperties.getRootPath())){
            KafkaLogger.kafkaLogger.error("Root directory : [{}]  does not exists",kafkaProperties.getRootPath());
            System.exit(0);
        }

        if (!FileUtil.exists(kafkaProperties.getRootPath()+"data/")){
            KafkaLogger.kafkaLogger.info("Creating Directory :{} ",kafkaProperties.getRootPath()+"data/");
            FileUtil.createDirectory(kafkaProperties.getRootPath()+"data/");
        }
        if (!FileUtil.exists(kafkaProperties.getRootPath()+"consumer/")){
            KafkaLogger.kafkaLogger.info("Creating Directory :{} ",kafkaProperties.getRootPath()+"consumer/");
            FileUtil.createDirectory(kafkaProperties.getRootPath()+"consumer/");
        }

        if (!FileUtil.exists(kafkaProperties.getRootPath()+"topic/")){
            KafkaLogger.kafkaLogger.info("Creating Directory :{} ",kafkaProperties.getRootPath()+"topic/");
            FileUtil.createDirectory(kafkaProperties.getRootPath()+"topic/");
        }

        KafkaLoader kafkaLoader = new KafkaLoader(kafkaProperties.getKafkaProperty("kafka.plugin.dir.path"));
        kafkaLoader.load();

        Serializer serializer = new Serializer();
        Deserializer deserializer = new Deserializer();
        ExecutorService executorService = Executors.newFixedThreadPool(20);

        KafkaClientRequestParser kafkaClientRequestParser = new KafkaClientRequestParser();

        KafkaRouter kafkaRouter = new KafkaRouter();
        KafkaServer kafkaServer = new KafkaServer();

        KafkaClientController kafkaClientController = new KafkaClientController(
                kafkaClientRequestParser,
                kafkaRouter,
                executorService
        );

        kafkaRouter.register(kafkaClientController);

        TransactionManager transactionManager = new TransactionManager(new Mapper<>());

        LoadBalancerFactory<KafkaProperties, Node, LBRequest> nodeLoadBalancerFactory = (LoadBalancerFactory<KafkaProperties, Node, LBRequest>) kafkaLoader.getLoadBalancerFactory(kafkaProperties.getKafkaProperty("kafka.loadbalancer.policy"));
        NodeManager nodeManager = new NodeManager(kafkaProperties, nodeLoadBalancerFactory);

        final TopicController topicController = new TopicController(kafkaProperties, nodeManager, nodeLoadBalancerFactory);

        NodeController nodeController = new NodeController(
                transactionManager,
                kafkaRouter,
                serializer,
                deserializer,
                executorService,
                nodeManager,
                topicController
        );

        kafkaRouter.register(nodeController);

        LeaderController leaderController = new LeaderController(kafkaRouter, topicController, nodeManager);

        kafkaRouter.register(leaderController);

        KafkaClientServer kafkaClientServer = new KafkaClientServer(kafkaProperties, kafkaClientController);
        NodeServer nodeServer = new NodeServer(kafkaProperties, nodeController);

        kafkaServer.register(kafkaClientServer.port(), kafkaClientServer);
        kafkaServer.register(nodeServer.port(), nodeServer);

        new Thread(() -> {
            final Scanner scanner = new Scanner(System.in);
            while (true) {
                final String nextLine = scanner.nextLine();
                System.out.println("line : " + nextLine);
                if (nextLine.equals("C")) break;
            }
            System.exit(0);
        }).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("shutdown hook called");
            System.out.println("Closing the leader application...");
            topicController.onClose();
            System.out.println("Leader Application Closed Successfully");

        }));

        topicController.onStart();
        kafkaServer.start();
    }
}
