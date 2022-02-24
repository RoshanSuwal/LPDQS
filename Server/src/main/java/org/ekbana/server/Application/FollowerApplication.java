package org.ekbana.server.Application;

import org.ekbana.broker.Broker;
import org.ekbana.server.broker.KafkaBrokerController;
import org.ekbana.server.client.*;
import org.ekbana.server.client.KafkaClient;
import org.ekbana.server.common.KafkaRouter;
import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.common.mb.RequestTransaction;
import org.ekbana.server.follower.Follower;
import org.ekbana.server.follower.FollowerController;
import org.ekbana.server.leader.*;
import org.ekbana.server.replica.ReplicaController;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.util.Serializer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FollowerApplication {

    private final Serializer serializer=new Serializer();
    private final Deserializer deserializer=new Deserializer();

    private final ExecutorService executorService= Executors.newFixedThreadPool(20);


    private final KafkaServerConfig kafkaServerConfig=new KafkaServerConfig("log2/","node-1");
    private final KafkaRouter kafkaRouter=new KafkaRouter();

    private final Follower follower=new Follower(kafkaServerConfig);

    private final Broker broker=new Broker();
    private final Mapper<Long, RequestTransaction> brokerTransactionMapper=new Mapper<>();
    private final KafkaBrokerController kafkaBrokerController=new KafkaBrokerController(kafkaServerConfig,broker,executorService,kafkaRouter,brokerTransactionMapper);

    private final KafkaClientConfig kafkaClientConfig=new KafkaClientConfig();
    private final KafkaClientRequestParser kafkaClientRequestParser=new KafkaClientRequestParser();
    private final KafkaClientProcessor kafkaClientProcessor=new KafkaClientProcessor();
    private final Mapper<Long, KafkaClient> kafkaClientMapper=new Mapper<>();
    private final KafkaClientController kafkaClientController=new KafkaClientController(kafkaClientConfig,kafkaClientRequestParser,kafkaClientProcessor,kafkaRouter,kafkaClientMapper,executorService);
    private final KafkaClientServer kafkaClientServer=new KafkaClientServer(kafkaClientConfig,kafkaClientController);

    private final FollowerController kafkaFollowerController=new FollowerController(kafkaServerConfig,follower,serializer,deserializer,kafkaRouter,executorService);

    private final ReplicaController replicaController=new ReplicaController(kafkaServerConfig,new Mapper<>(),new Mapper<>(),new Mapper<>(),kafkaRouter);
//    private final KafkaServer kafkaServer=new KafkaServer();

    public void start() throws IOException {
        kafkaRouter.register(kafkaClientController);
        kafkaRouter.register(kafkaBrokerController);
        kafkaRouter.register(kafkaFollowerController);
        kafkaRouter.register(replicaController);

        broker.load();
        replicaController.load();

        follower.connect(executorService);
//        kafkaServer.register(kafkaClientServer.port(),kafkaClientServer);
//        kafkaServer.start();
//        kafkaClientServer.startServer(new int[]{9999});
    }

    public static void main(String[] args) throws IOException {
        new FollowerApplication().start();
    }
}
