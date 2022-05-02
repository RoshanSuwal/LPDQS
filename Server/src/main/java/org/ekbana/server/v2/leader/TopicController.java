package org.ekbana.server.v2.leader;

import org.ekbana.minikafka.common.FileUtil;
import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancer;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancerFactory;
import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.util.KafkaLogger;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.v2.node.NodeManager;

import java.io.IOException;

public class TopicController {
    private Mapper<String, Topic> topicMapper;
    private LoadBalancerFactory<KafkaProperties, Node, LBRequest> loadBalancerFactory;
    private Mapper<String, LoadBalancer> topicLoadBalancerMapper;
    private KafkaProperties kafkaProperties;

    private NodeManager nodeManager;

    public TopicController(KafkaProperties kafkaProperties, NodeManager nodeManager, LoadBalancerFactory loadBalancerFactory) {
        this.topicMapper = new Mapper<>();
        this.loadBalancerFactory = loadBalancerFactory;
        this.topicLoadBalancerMapper = new Mapper<>();
        this.kafkaProperties = kafkaProperties;
        this.nodeManager = nodeManager;
    }

    public boolean hasTopic(String topicName) {
        return topicMapper.has(topicName);
    }

    public void createTopic(Topic topic) {
        createTopic(topic, true);
    }

    public void createTopic(Topic topic, boolean isNew) {
        topicMapper.add(topic.getTopicName(), topic);
        final LoadBalancer<Node, LBRequest> lb = loadBalancerFactory.buildLoadBalancer(kafkaProperties);
        for (Node node : topic.getDataNode()) lb.addNode(node);
        topicLoadBalancerMapper.add(topic.getTopicName(), lb);

        if (isNew) {
            KafkaLogger.topicLogger.info("Created topic : {}", topic.getTopicName());
            saveToFile(topic);
        } else {
            KafkaLogger.topicLogger.info("Loaded topic : {}" ,topic.getTopicName());
        }
    }

    public void removeTopic(String topicName) {
        topicMapper.delete(topicName);
        // remove from file
        removeFromFile(topicName);
    }

    public Topic getTopic(String topicName) {
        return topicMapper.get(topicName);
    }

    public Node getNode(Topic topic, LBRequest lbRequest) {
        // return node using partition if partitionIdExists
        if (lbRequest.getKey() != null) {
            // return node using  hashFunction if key provided
            final Node node = topic.getDataNode()[0];
            if (nodeManager.hasNode(node)) return node;
        }

        for (int i = 0; i < topic.getDataNode().length; i++) {
            final Node assignedNode = (Node) topicLoadBalancerMapper.get(topic.getTopicName()).getAssignedNodeId(lbRequest);
            if (nodeManager.hasNode(assignedNode))
                return assignedNode;
        }
        return null;
    }

    public int getPartitionId(Topic topic, LBRequest lbRequest) {

//        if (lbRequest.getId() >= 0) {
//            if (nodeManager.hasNode(topic.getDataNode()[lbRequest.getId() % topic.getNumberOfPartitions()]))
//                return lbRequest.getId() % topic.getNumberOfPartitions();
//        }
//        if (lbRequest.getKey() != null) {
//            if (nodeManager.hasNode(topic.getDataNode()[0])) return 0;
//        }

        for (int i = 0; i < topic.getDataNode().length; i++) {
            final int assignedNodePartitionId = topicLoadBalancerMapper.get(topic.getTopicName()).getAssignedNodePartitionId(lbRequest);
            if (nodeManager.hasNode(topic.getDataNode()[assignedNodePartitionId]))
                return assignedNodePartitionId;
        }
        return -1;
    }

    public void onStart() {
        // load all topics from file
        KafkaLogger.topicLogger.info("Loading topics..");
        try {
            FileUtil.getFiles(kafkaProperties.getRootPath() + "topic/").forEach(file -> {
                try {
                    createTopic((Topic) FileUtil.readObjectFromFile(file.getPath()), false);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void onClose() {
        // dump all topics to file
        KafkaLogger.topicLogger.info("Saving topics...");
        topicMapper.forEach((topicName, topic) -> saveToFile(topic));
    }

    private void saveToFile(Topic topic) {
        KafkaLogger.topicLogger.info("Topic Saved : {}", topic);
        FileUtil.writeObjectToFile(kafkaProperties.getRootPath() + "topic/" + topic.getTopicName() + ".dat", topic);
    }

    private void removeFromFile(String topicName) {
        FileUtil.deleteFile(kafkaProperties.getRootPath() + "topic/" + topicName + ".dat");
        KafkaLogger.topicLogger.info("Topic Removed : {}", topicName);
    }
}
