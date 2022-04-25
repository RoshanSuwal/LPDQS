package org.ekbana.server.cluster;

import org.ekbana.server.util.Mapper;
import org.ekbana.minikafka.common.Node;

import java.util.List;

//@AllArgsConstructor
//@NoArgsConstructor
public class Cluster {
    public Mapper<String , Node> nodeMapper=new Mapper<>();

    // Load balancer
    // balances the node

    public void addNode(Node node){
        nodeMapper.add(node.getId(),node);
    }

    public void deleteNode(Node node){
        nodeMapper.delete(node.getId());
    }

    public Node[] getNodes(int numberOfPartitions){
        final int size = nodeMapper.size();
        Node[] nodes=new Node[numberOfPartitions];
        final List<Node> allNodes = nodeMapper.getValues().stream().toList();
        for (int i=0;i<nodes.length;i++){
            nodes[i]=allNodes.get(i%size);
        }
        return nodes;
    }


}
