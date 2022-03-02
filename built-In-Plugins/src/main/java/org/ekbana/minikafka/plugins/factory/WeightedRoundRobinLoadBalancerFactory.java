package org.ekbana.minikafka.plugins.factory;

import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Mapper;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancer;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancerFactory;
import org.ekbana.minikafka.plugins.loadbalancer.WeighedRoundRobinLB;

public class WeightedRoundRobinLoadBalancerFactory implements LoadBalancerFactory<Mapper<String,Node>,Node, LBRequest> {

    @Override
    public String loadBalancerName() {
        return "weighted round robin";
    }

    @Override
    public LoadBalancer<Node, LBRequest> buildLoadBalancer(Mapper<String, Node> nodeMapper) {
        return new WeighedRoundRobinLB(nodeMapper);
    }

}
