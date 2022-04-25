package org.ekbana.server.v2;

import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancer;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancerFactory;
import org.ekbana.server.KafkaLoader;
import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.util.Deserializer;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LeaderApplication {

    public static void main(String[] args) throws IOException {
        final Properties properties = new Properties();
        properties.setProperty("kafka.server.node.id", "node-0");
        properties.setProperty("kafka.storage.data.path", "log2/");
        final KafkaProperties kafkaProperties = new KafkaProperties(properties);

        KafkaLoader kafkaLoader = new KafkaLoader("plugins");
        kafkaLoader.load();

        Serializer serializer=new Serializer();
        Deserializer deserializer=new Deserializer();
        ExecutorService executorService= Executors.newFixedThreadPool(20);

        KafkaClientRequestParser kafkaClientRequestParser=new KafkaClientRequestParser();

        KafkaRouter kafkaRouter=new KafkaRouter();
        KafkaServer kafkaServer=new KafkaServer();

        KafkaClientController kafkaClientController=new KafkaClientController(
                kafkaClientRequestParser,
                kafkaRouter,
                executorService
        );

        kafkaRouter.register(kafkaClientController);

        TransactionManager transactionManager=new TransactionManager(new Mapper<>());

        LoadBalancerFactory<KafkaProperties,Node, LBRequest> nodeLoadBalancerFactory= (LoadBalancerFactory<KafkaProperties, Node, LBRequest>) kafkaLoader.getLoadBalancerFactory("weighted round robin");
        NodeManager nodeManager=new NodeManager(kafkaProperties,nodeLoadBalancerFactory);

        final TopicController topicController = new TopicController(kafkaProperties,nodeManager,nodeLoadBalancerFactory);

        NodeController nodeController=new NodeController(
                transactionManager,
                kafkaRouter,
                serializer,
                deserializer,
                executorService,
                nodeManager,
                topicController
        );

        kafkaRouter.register(nodeController);


        LeaderController leaderController=new LeaderController(kafkaRouter,topicController,nodeManager);

        kafkaRouter.register(leaderController);

        KafkaClientServer kafkaClientServer=new KafkaClientServer(kafkaProperties,kafkaClientController);
        NodeServer nodeServer=new NodeServer(kafkaProperties,nodeController);

        kafkaServer.register(kafkaClientServer.port(),kafkaClientServer);
        kafkaServer.register(nodeServer.port(),nodeServer);

        kafkaServer.start();

    }
}
