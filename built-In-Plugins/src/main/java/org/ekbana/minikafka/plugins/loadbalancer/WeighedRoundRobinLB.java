package org.ekbana.minikafka.plugins.loadbalancer;

import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Mapper;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancer;

import java.util.ArrayList;
import java.util.List;

public class WeighedRoundRobinLB implements LoadBalancer<Node, LBRequest> {
    private final List<String> nodes;
    private int assignTo;
    private int currentNodeAssignments;
    private final Object lock;

    private final Mapper<String, Node> nodeMapper;

    public WeighedRoundRobinLB() {
        this.nodeMapper = new Mapper<>();
        this.nodes = new ArrayList<>();
        this.lock = new Object();
        this.assignTo = 0;
    }

    @Override
    public void addNode(Node node) {
        synchronized (this.lock) {
            nodes.add(node.getId());
            nodeMapper.add(node.getId(), node);
        }
    }

    @Override
    public void removeNode(Node node) {
        synchronized (this.lock) {
            nodes.remove(node.getId());
            nodeMapper.delete(node.getId());
        }
    }

    @Override
    public Node getAssignedNodeId(LBRequest lbRequest) {
        synchronized (this.lock) {
            assignTo = (assignTo + nodes.size()) % nodes.size();
            final var currentNode = nodes.get(assignTo);
            currentNodeAssignments++;
            if (currentNodeAssignments == nodeMapper.get(currentNode).getWeight()) {
                assignTo++;
                currentNodeAssignments = 0;
            }
            return nodeMapper.get(currentNode);
        }
    }

    @Override
    public int getAssignedNodePartitionId(LBRequest lbRequest) {
        synchronized (this.lock) {
            assignTo = (assignTo + nodes.size()) % nodes.size();
            final var currentNode = assignTo;
            currentNodeAssignments++;
            if (currentNodeAssignments == nodeMapper.get(nodes.get(assignTo)).getWeight()) {
                assignTo++;
                currentNodeAssignments = 0;
            }
            return currentNode;
        }
    }
}
