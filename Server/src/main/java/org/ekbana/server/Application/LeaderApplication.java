package org.ekbana.server.Application;

import org.ekbana.broker.Broker;
import org.ekbana.broker.utils.BrokerConfig;
import org.ekbana.server.broker.KafkaBrokerController;
import org.ekbana.server.client.*;
import org.ekbana.server.common.KafkaRouter;
import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.common.mb.RequestTransaction;
import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.follower.Follower;
import org.ekbana.server.follower.FollowerController;
import org.ekbana.server.leader.KafkaServerConfig;
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

public class LeaderApplication {

    private final Serializer serializer=new Serializer();
    private final Deserializer deserializer=new Deserializer();

    private final ExecutorService executorService= Executors.newFixedThreadPool(20);


    private final KafkaServerConfig kafkaServerConfig=new KafkaServerConfig("log/","node-0");

    private final TransactionManager transactionManager=new TransactionManager(new Mapper<>());
    private final Mapper<String, Topic> topicMapper=new Mapper<>();

    private final KafkaRouter kafkaRouter=new KafkaRouter();

    private final KafkaClientConfig kafkaClientConfig=new KafkaClientConfig();
    private final KafkaClientRequestParser kafkaClientRequestParser=new KafkaClientRequestParser();
    private final KafkaClientProcessor kafkaClientProcessor=new KafkaClientProcessor();
    private final Mapper<Long, KafkaClient> kafkaClientMapper=new Mapper<>();
    private final KafkaClientController kafkaClientController=new KafkaClientController(kafkaClientConfig,kafkaClientRequestParser,kafkaClientProcessor,kafkaRouter,kafkaClientMapper,executorService);
    private final KafkaClientServer kafkaClientServer=new KafkaClientServer(kafkaClientConfig,kafkaClientController);


    private final Follower follower=new Follower(kafkaServerConfig);

    private final BrokerConfig brokerConfig=new BrokerConfig(new Properties());
    ExecutorService producerExecutorService= Executors.newFixedThreadPool(10);
    private final Broker broker=new Broker(brokerConfig,producerExecutorService);
    private final Mapper<Long, RequestTransaction> brokerTransactionMapper=new Mapper<>();
    private final KafkaBrokerController kafkaBrokerController=new KafkaBrokerController(kafkaServerConfig,broker,executorService,kafkaRouter,brokerTransactionMapper);

    private final FollowerController kafkaFollowerController=new FollowerController(kafkaServerConfig,follower,serializer,deserializer,kafkaRouter,executorService);

    private final ReplicaController replicaController=new ReplicaController(kafkaServerConfig,new Mapper<>(),new Mapper<>(),new Mapper<>(),kafkaRouter);

    private final LeaderController leaderController=new LeaderController(kafkaServerConfig, deserializer, serializer, new Mapper<>(), transactionManager, replicaController,executorService);

    private final LeaderServer leaderServer=new LeaderServer(kafkaServerConfig,leaderController);
    private final KafkaServer kafkaServer=new KafkaServer();

    public void start() throws IOException {
        kafkaRouter.register(kafkaClientController);
        kafkaRouter.register(replicaController);
        kafkaRouter.register(kafkaFollowerController);
        kafkaRouter.register(kafkaBrokerController);

        broker.load();
        replicaController.load();

        kafkaServer.register(leaderServer.port(),leaderServer);
        kafkaServer.register(kafkaClientServer.port(),kafkaClientServer);
//        kafkaServer.start();

        executorService.submit(()-> {
            try {
                Thread.sleep(5000);
                follower.connect(executorService);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        kafkaServer.start();


    }
    public static void main(String[] args) throws IOException {
        new LeaderApplication().start();
    }
}
