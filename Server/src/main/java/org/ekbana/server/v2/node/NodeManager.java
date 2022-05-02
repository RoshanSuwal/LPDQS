package org.ekbana.server.v2.node;

import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancer;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancerFactory;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.util.KafkaLogger;
import org.ekbana.server.util.Mapper;

public class NodeManager {
    private Mapper<String, Node> nodeMapper;

    private KafkaProperties kafkaProperties;
    private LoadBalancerFactory<KafkaProperties,Node, LBRequest> loadBalancerFactory;

    private LoadBalancer loadBalancer;
    public NodeManager(KafkaProperties kafkaProperties,LoadBalancerFactory<KafkaProperties,Node,LBRequest> loadBalancerFactory) {
        nodeMapper=new Mapper<>();
        this.kafkaProperties=kafkaProperties;
        this.loadBalancerFactory=loadBalancerFactory;
        this.loadBalancer=loadBalancerFactory.buildLoadBalancer(kafkaProperties);
    }

    public void registerNode(Node node){
        nodeMapper.add(node.getId(),node);
        loadBalancer.addNode(node);
        KafkaLogger.leaderLogger.info("{} : {}","Registered Node ",node.getId());
    }

    public void unRegisterNode(Node node){
        nodeMapper.delete(node.getId());
        loadBalancer.removeNode(node);
        KafkaLogger.leaderLogger.info("{} : {}","Unregistered Node",node.getId());
    }

    public boolean hasNode(Node node){
        return nodeMapper.has(node.getId());
    }

    public Node[] getNodes(int numberOfPartitions){
        Node[] nodes=new Node[numberOfPartitions];
        for (int i=0;i<numberOfPartitions;i++){
            nodes[i]= (Node) loadBalancer.getAssignedNodeId(LBRequest.builder().requestWeight(1).build());
        }
        return nodes;
//        final int size = nodeMapper.size();
//        Node[] nodes=new Node[numberOfPartitions];
//        final List<Node> allNodes = nodeMapper.getValues().stream().toList();
//        for (int i=0;i<nodes.length;i++){
//            nodes[i]=allNodes.get(i%size);
//        }
//        return nodes;
    }
}
