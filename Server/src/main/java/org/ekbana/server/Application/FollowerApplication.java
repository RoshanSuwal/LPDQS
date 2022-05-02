//package org.ekbana.server.Application;
//
//import org.ekbana.broker.utils.KafkaBrokerProperties;
//import org.ekbana.server.Kafka;
//import org.ekbana.server.KafkaLoader;
//import org.ekbana.server.config.KafkaProperties;
//
//import java.io.IOException;
//import java.util.Properties;
//
//public class FollowerApplication {
//
////    private final Serializer serializer=new Serializer();
////    private final Deserializer deserializer=new Deserializer();
////
////    private final ExecutorService executorService= Executors.newFixedThreadPool(20);
////
////
////    private final KafkaProperties kafkaProperties=new KafkaProperties(new Properties());
////    private final KafkaRouter kafkaRouter=new KafkaRouter();
////
////    private final Follower follower=new Follower(kafkaProperties);
////
//////    private final BrokerConfig brokerConfig=new BrokerConfig(new Properties());
////    ExecutorService producerExecutorService= Executors.newFixedThreadPool(10);
////    private final Broker broker=null;//new Broker(brokerConfig,producerExecutorService);
////    private final Mapper<Long, RequestTransaction> brokerTransactionMapper=new Mapper<>();
////    private final KafkaBrokerController kafkaBrokerController=new KafkaBrokerController(kafkaProperties,broker,executorService,kafkaRouter,brokerTransactionMapper);
////
////    private final KafkaClientConfig kafkaClientConfig=new KafkaClientConfig();
////    private final KafkaClientRequestParser kafkaClientRequestParser=new KafkaClientRequestParser();
////    private final KafkaClientProcessor kafkaClientProcessor=new KafkaClientProcessor();
////    private final Mapper<Long, KafkaClient> kafkaClientMapper=new Mapper<>();
////    private final KafkaClientController kafkaClientController=new KafkaClientController(kafkaClientConfig,kafkaClientRequestParser,kafkaClientProcessor,kafkaRouter,kafkaClientMapper,executorService);
////    private final KafkaClientServer kafkaClientServer=new KafkaClientServer(kafkaClientConfig,kafkaClientController);
////
////    private final FollowerController kafkaFollowerController=new FollowerController(kafkaProperties,follower,serializer,deserializer,kafkaRouter,executorService);
////
////    private final ReplicaController replicaController=new ReplicaController(kafkaProperties,new Mapper<>(),new Mapper<>(),new Mapper<>(),kafkaRouter);
//////    private final KafkaServer kafkaServer=new KafkaServer();
////
////    public void start() throws IOException {
////        kafkaRouter.register(kafkaClientController);
////        kafkaRouter.register(kafkaBrokerController);
////        kafkaRouter.register(kafkaFollowerController);
////        kafkaRouter.register(replicaController);
////
////        broker.load();
////        replicaController.load();
////
////        follower.connect(executorService);
//////        kafkaServer.register(kafkaClientServer.port(),kafkaClientServer);
//////        kafkaServer.start();
//////        kafkaClientServer.startServer(new int[]{9999});
////    }
//
//    public static void main(String[] args) throws IOException, InterruptedException {
////        new FollowerApplication().start();
//
//        final Properties followerProperties = new Properties();
//        followerProperties.setProperty("kafka.server.node.id", "node-1");
//        followerProperties.setProperty("kafka.storage.data.path", "log2/");
//        final KafkaProperties kafkFollowerProperties = new KafkaProperties(followerProperties);
//
//        final Properties brokerPro = new Properties();
//        brokerPro.setProperty("kafka.broker.root.path","log2/");
//        final KafkaBrokerProperties kafkaBrokerProperties = new KafkaBrokerProperties(brokerPro);
//
//        KafkaLoader kafkaLoader = new KafkaLoader("plugins");
//        kafkaLoader.load();
//
//        Kafka kafka = new Kafka(kafkFollowerProperties, kafkaBrokerProperties, kafkaLoader);
//        kafka.configureBroker();
//        kafka.configureReplica();
////        kafka.configureLeader();
//        kafka.configureFollower();
////        kafka.configureClient();
//        kafka.start();
//    }
//}
