package org.ekbana.server.Application;

import org.ekbana.broker.utils.KafkaBrokerProperties;
import org.ekbana.server.Kafka;
import org.ekbana.server.KafkaLoader;
import org.ekbana.server.config.KafkaProperties;

import java.io.IOException;
import java.util.Properties;

public class LeaderApplication {

    //    private final Serializer serializer=new Serializer();
//    private final Deserializer deserializer=new Deserializer();
//
//    private final ExecutorService executorService= Executors.newFixedThreadPool(20);
//
//
//    private final KafkaProperties kafkaProperties=new KafkaProperties(new Properties());
//
//    private final TransactionManager transactionManager=new TransactionManager(new Mapper<>());
//    private final Mapper<String, Topic> topicMapper=new Mapper<>();
//
//    private final KafkaRouter kafkaRouter=new KafkaRouter();
//
//    private final KafkaClientConfig kafkaClientConfig=new KafkaClientConfig();
//    private final KafkaClientRequestParser kafkaClientRequestParser=new KafkaClientRequestParser();
//    private final KafkaClientProcessor kafkaClientProcessor=new KafkaClientProcessor();
//    private final Mapper<Long, KafkaClient> kafkaClientMapper=new Mapper<>();
//    private final KafkaClientController kafkaClientController=new KafkaClientController(kafkaProperties,kafkaClientRequestParser,kafkaClientProcessor,kafkaRouter,kafkaClientMapper,executorService);
//    private final KafkaClientServer kafkaClientServer=new KafkaClientServer(kafkaProperties,kafkaClientController);
//
//
//    private final Follower follower=new Follower(kafkaProperties);
//
//    ExecutorService producerExecutorService= Executors.newFixedThreadPool(10);
//    private final Broker broker=null;//;new Broker(brokerConfig,producerExecutorService);
//    private final Mapper<Long, RequestTransaction> brokerTransactionMapper=new Mapper<>();
//    private final KafkaBrokerController kafkaBrokerController=new KafkaBrokerController(kafkaProperties,broker,executorService,kafkaRouter,brokerTransactionMapper);
//
//    private final FollowerController kafkaFollowerController=new FollowerController(kafkaProperties,follower,serializer,deserializer,kafkaRouter,executorService);
//
//    private final ReplicaController replicaController=new ReplicaController(kafkaProperties,new Mapper<>(),new Mapper<>(),new Mapper<>(),kafkaRouter);
//
//    private final LeaderController leaderController=new LeaderController(kafkaProperties, deserializer, serializer, new Mapper<>(), transactionManager, replicaController,executorService);
//
//    private final LeaderServer leaderServer=new LeaderServer(kafkaProperties,leaderController);
//    private final KafkaServer kafkaServer=new KafkaServer();
//
//    public void start() throws IOException {
//
//        KafkaBrokerProperties kafkaBrokerProperties=new KafkaBrokerProperties(new Properties());
////        broker=new Broker(kafkaBrokerProperties,);
//        kafkaRouter.register(kafkaClientController);
//        kafkaRouter.register(replicaController);
//        kafkaRouter.register(kafkaFollowerController);
//        kafkaRouter.register(kafkaBrokerController);
//
//        broker.load();
//        replicaController.load();
//
//        kafkaServer.register(leaderServer.port(),leaderServer);
//        kafkaServer.register(kafkaClientServer.port(),kafkaClientServer);
////        kafkaServer.start();
//
//        executorService.submit(()-> {
//            try {
//                Thread.sleep(5000);
//                follower.connect(executorService);
//            } catch (IOException | InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//        kafkaServer.start();
//
//
//    }
    public static void main(String[] args) throws IOException, InterruptedException {
        final Properties properties = new Properties();
        properties.setProperty("kafka.server.node.id", "node-0");
        properties.setProperty("kafka.storage.data.path", "log/");
        final KafkaProperties kafkaProperties = new KafkaProperties(properties);

        final Properties brokerPro = new Properties();
        brokerPro.setProperty("kafka.broker.root.path","log/");
        final KafkaBrokerProperties kafkaBrokerProperties = new KafkaBrokerProperties(brokerPro);

        KafkaLoader kafkaLoader = new KafkaLoader("plugins");
        kafkaLoader.load();

        Kafka kafka = new Kafka(kafkaProperties, kafkaBrokerProperties, kafkaLoader);
        kafka.configureBroker();
        kafka.configureReplica();
        kafka.configureLeader();
        kafka.configureFollower();
        kafka.configureClient();
        kafka.start();

    }
}
