package org.ekbana.server.Application;

import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.leader.*;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.util.Serializer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LeaderApplication {

    private final Serializer serializer=new Serializer();
    private final Deserializer deserializer=new Deserializer();

    private final ExecutorService executorService= Executors.newFixedThreadPool(20);


    private final KafkaServerConfig kafkaServerConfig=new KafkaServerConfig();

    private final NodeClientMapper nodeClientMapper=new NodeClientMapper();
    private final TransactionManager transactionManager=new TransactionManager(new Mapper<>());
    private final Mapper<String, Topic> topicMapper=new Mapper<>();
    private final LeaderController leaderController=new LeaderController(kafkaServerConfig, deserializer, serializer, nodeClientMapper, transactionManager, topicMapper,executorService);

    private final LeaderServer leaderServer=new LeaderServer(kafkaServerConfig,leaderController);

    public void start() throws IOException {
        leaderServer.startServer(executorService);
    }
    public static void main(String[] args) throws IOException {
        new LeaderApplication().start();
    }
}
