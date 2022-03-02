package org.ekbana.minikafka.plugins.loadbalancer;

import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Mapper;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

public class ConsistentHashingLB implements LoadBalancer<Node, LBRequest> {

    private final Map<Node, List<Long>> nodePositions;
    private final ConcurrentSkipListMap<Long,String> nodeMappings;
    private final Function<String,Long> hashFunction;
    private final int pointMultiplier;
    private final Mapper<String,Node> nodeMapper;
    private final ConcurrentHashMap<String,String> hashNodeMapping;

    public ConsistentHashingLB(Mapper<String,Node> nodeMapper, final Function<String, Long> hashFunction, final int pointMultiplier) {
        this.nodeMapper=nodeMapper;
        this.hashFunction = hashFunction;
        this.pointMultiplier = pointMultiplier;
        this.nodePositions=new ConcurrentHashMap<>();
        this.nodeMappings=new ConcurrentSkipListMap<>();
        this.hashNodeMapping=new ConcurrentHashMap<>();

    }

    @Override
    public void addNode(Node node) {
        nodePositions.put(node,new CopyOnWriteArrayList<>());
        for (int i=0;i<pointMultiplier;i++){
            for (int j=0;j<node.getWeight();j++){
                final var point=hashFunction.apply((i*pointMultiplier+j)+node.getHashId());
                nodePositions.get(node).add(point);
                nodeMappings.put(point,node.getHashId());
            }
        }
        hashNodeMapping.put(node.getHashId(),node.getId());
    }

    @Override
    public void removeNode(Node node) {
        for (final Long point:nodePositions.remove(node)){
            nodeMappings.remove(point);
        }
        hashNodeMapping.remove(node.getHashId());
    }

    @Override
    public Node getAssignedNodeId(LBRequest request) {
        final var key=hashFunction.apply(request.getId());
        final var entry=nodeMappings.higherEntry(key);
        if (entry==null) return nodeMapper.get(hashNodeMapping.get(nodeMappings.firstEntry().getValue()));
        else return nodeMapper.get(hashNodeMapping.get(entry.getValue()));
    }
}
