package org.ekbana.server;

import org.ekbana.broker.Broker;
import org.ekbana.broker.utils.BrokerConfig;
import org.ekbana.server.broker.KafkaBrokerController;
import org.ekbana.server.client.*;
import org.ekbana.server.client.KafkaClient;
import org.ekbana.server.common.KafkaRouter;
import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.common.mb.RequestTransaction;
import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.follower.Follower;
import org.ekbana.server.follower.FollowerController;
import org.ekbana.server.leader.*;
import org.ekbana.server.replica.ReplicaController;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.util.Serializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Kafka {

    private final Serializer serializer=new Serializer();
    private final Deserializer deserializer=new Deserializer();

    private final ExecutorService executorService= Executors.newFixedThreadPool(100);

    private final KafkaServerConfig kafkaServerConfig=new KafkaServerConfig("log/","node-0");
    private final KafkaRouter kafkaRouter=new KafkaRouter();

    private final Follower follower=new Follower(kafkaServerConfig);

    private final BrokerConfig brokerConfig=new BrokerConfig(new Properties());
    ExecutorService producerExecutorService= Executors.newFixedThreadPool(10);
    private final Broker broker=new Broker(brokerConfig,producerExecutorService);
    private final Mapper<Long, RequestTransaction> brokerTransactionMapper=new Mapper<>();
    private final KafkaBrokerController kafkaBrokerController=new KafkaBrokerController(kafkaServerConfig,broker,executorService,kafkaRouter,brokerTransactionMapper);

    private final KafkaClientConfig kafkaClientConfig=new KafkaClientConfig();
    private final KafkaClientRequestParser kafkaClientRequestParser=new KafkaClientRequestParser();
    private final KafkaClientProcessor kafkaClientProcessor=new KafkaClientProcessor();
    private final Mapper<Long, KafkaClient> kafkaClientMapper=new Mapper<>();
    private final KafkaClientController kafkaClientController=new KafkaClientController(kafkaClientConfig,kafkaClientRequestParser,kafkaClientProcessor,kafkaRouter,kafkaClientMapper,executorService);
    private final KafkaClientServer kafkaClientServer=new KafkaClientServer(kafkaClientConfig,kafkaClientController);


    private final FollowerController kafkaFollowerController=new FollowerController(kafkaServerConfig,follower,serializer,deserializer,kafkaRouter,executorService);

    private final TransactionManager transactionManager=new TransactionManager(new Mapper<>());
    private final Mapper<String, Topic> topicMapper=new Mapper<>();

    private final ReplicaController replicaController=new ReplicaController(kafkaServerConfig,new Mapper<>(),new Mapper<>(),new Mapper<>(),kafkaRouter);

    private final LeaderController leaderController=new LeaderController(kafkaServerConfig, deserializer, serializer, new Mapper<>(), transactionManager, replicaController,executorService);

    private final LeaderServer leaderServer=new LeaderServer(kafkaServerConfig,leaderController);


    private final KafkaServer kafkaServer=new KafkaServer();

    public void load(){
        // load all the plugins : policies, load balancer

        // validation of configuration file with detail info
    }


    public void start() throws IOException, InterruptedException {
        kafkaRouter.register(kafkaClientController);
        kafkaRouter.register(kafkaBrokerController);
        kafkaRouter.register(kafkaFollowerController);

        kafkaServer.register(leaderServer.port(),leaderServer);
        kafkaServer.register(kafkaClientServer.port(),kafkaClientServer);

        executorService.submit(()-> {
            try {
                Thread.sleep(1000);
                follower.connect(executorService);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
//        leaderServer.startServer(new int[]{9998});

//        kafkaClientServer.startServer(new int[]{9998});
        kafkaServer.start();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Kafka kafka=new Kafka();
        kafka.load();
        kafka.start();
    }
}
