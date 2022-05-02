package org.ekbana.server;

import org.ekbana.broker.Broker;
import org.ekbana.broker.utils.KafkaBrokerProperties;
import org.ekbana.minikafka.plugin.policy.PolicyType;
import org.ekbana.server.broker.KafkaBrokerController;
import org.ekbana.server.client.*;
import org.ekbana.server.common.KafkaRouter;
import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.common.mb.RequestTransaction;
import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.follower.Follower;
import org.ekbana.server.follower.FollowerController;
import org.ekbana.server.leader.LeaderController;
import org.ekbana.server.leader.LeaderServer;
import org.ekbana.server.leader.TransactionManager;
import org.ekbana.server.replica.ReplicaController;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.util.Serializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Kafka {

    private  final Serializer serializer;
    private  final Deserializer deserializer;

    private final ExecutorService kafkaExecutorService;
    private final ExecutorService brokerExecutorService;

    private final KafkaClientRequestParser kafkaClientRequestParser;
    private final KafkaClientProcessor kafkaClientProcessor;

    private final Mapper<Long, RequestTransaction> brokerTransactionMapper = new Mapper<>();
    private final Mapper<Long, KafkaClient> kafkaClientMapper = new Mapper<>();
    private final TransactionManager transactionManager = new TransactionManager(new Mapper<>());
    private final Mapper<String, Topic> topicMapper = new Mapper<>();


    private  KafkaClientController kafkaClientController;
    private  KafkaClientServer kafkaClientServer;

    private Broker broker;
    private KafkaBrokerController kafkaBrokerController;

    private  final Follower follower;
    private  FollowerController kafkaFollowerController;

    private ReplicaController replicaController;

    private  LeaderController leaderController;
    private  LeaderServer leaderServer;


    private final KafkaProperties kafkaProperties;
    private final KafkaBrokerProperties kafkaBrokerProperties;

    private final KafkaServer kafkaServer;
    private final KafkaLoader kafkaLoader;
    private final   KafkaRouter kafkaRouter;

    public Kafka(KafkaProperties kafkaProperties,KafkaBrokerProperties kafkaBrokerProperties,KafkaLoader kafkaLoader) {
        this.kafkaProperties = kafkaProperties;
        this.kafkaBrokerProperties=kafkaBrokerProperties;
        this.kafkaLoader = kafkaLoader;

        serializer=new Serializer();
        deserializer=new Deserializer();

        kafkaClientRequestParser=new KafkaClientRequestParser();
        kafkaClientProcessor=new KafkaClientProcessor();

        kafkaExecutorService=Executors.newFixedThreadPool(100);
        brokerExecutorService=Executors.newFixedThreadPool(10);

        follower=new Follower(kafkaProperties);

        kafkaRouter=new KafkaRouter();
        kafkaServer=new KafkaServer();

    }

    public void configureLeader(){
        leaderController=new LeaderController(kafkaProperties,
                deserializer,serializer,
                new Mapper<>(),
                transactionManager,replicaController,kafkaExecutorService);
        leaderServer=new LeaderServer(kafkaProperties,leaderController);

        kafkaServer.register(leaderServer.port(),leaderServer);
    }

    public void configureBroker(){
        broker=new Broker(kafkaBrokerProperties,
                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.batch.policy")).buildPolicy(kafkaBrokerProperties),
                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.retention.policy")).buildPolicy(kafkaBrokerProperties),
                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.consumer.record.batch.policy")).buildPolicy(kafkaBrokerProperties),
                brokerExecutorService);
        broker.load();

        kafkaBrokerController=new KafkaBrokerController(kafkaProperties,broker,kafkaExecutorService,kafkaRouter,brokerTransactionMapper);
        kafkaRouter.register(kafkaBrokerController);
    }


    public void configureFollower(){
        kafkaFollowerController=new FollowerController(kafkaProperties,follower,serializer,deserializer,kafkaRouter,kafkaExecutorService);
        kafkaRouter.register(kafkaFollowerController);
    }

    public void configureClient(){
        kafkaClientController=new KafkaClientController(kafkaProperties, kafkaClientRequestParser, kafkaClientProcessor, kafkaRouter, kafkaClientMapper, kafkaExecutorService);
        kafkaClientServer=new KafkaClientServer(kafkaProperties,kafkaClientController);

        kafkaRouter.register(kafkaClientController);
        kafkaServer.register(kafkaClientServer.port(),kafkaClientServer);
    }

    public void configureReplica(){
        replicaController=new ReplicaController(kafkaProperties, new Mapper<>(), new Mapper<>(), new Mapper<>(), kafkaRouter);
        replicaController.load();
        kafkaRouter.register(replicaController);
    }

    public void start() throws IOException, InterruptedException {

        kafkaExecutorService.submit(() -> {
            try {
                Thread.sleep(1000);
                follower.connect(kafkaExecutorService);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        kafkaServer.start();
    }

//    public static void main(String[] args) throws IOException, InterruptedException {
////        Kafka kafka=new Kafka();
////        kafka.load();
////        kafka.start();
//        KafkaLoader kafkaLoader = new KafkaLoader("plugins");
//        kafkaLoader.load();
//
//        // creation of broker properties and its validation
//        KafkaBrokerProperties kafkaBrokerProperties = new KafkaBrokerProperties(new Properties());
//        kafkaLoader.validatePolicy(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.batch.policy"), PolicyType.SEGMENT_BATCH_POLICY);
//        kafkaLoader.validatePolicy(kafkaBrokerProperties.getBrokerProperty("kafka.broker.consumer.record.batch.policy"), PolicyType.CONSUMER_RECORD_BATCH_POLICY);
//        kafkaLoader.validatePolicy(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.retention.policy"), PolicyType.SEGMENT_RETENTION_POLICY);
//
//        // creating broker and passing broker properties and policies
//
//        ExecutorService brokerExecutorService=Executors.newFixedThreadPool(10);
//        final Broker broker = new Broker(kafkaBrokerProperties,
//                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.batch.policy")).buildPolicy(kafkaBrokerProperties),
//                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.segment.retention.policy")).buildPolicy(kafkaBrokerProperties),
//                kafkaLoader.getPolicyFactory(kafkaBrokerProperties.getBrokerProperty("kafka.broker.consumer.record.batch.policy")).buildPolicy(kafkaBrokerProperties),
//                brokerExecutorService);
//
//        broker.load();
//
//    }
}
