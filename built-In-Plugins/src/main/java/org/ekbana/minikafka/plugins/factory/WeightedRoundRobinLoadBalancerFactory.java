package org.ekbana.minikafka.plugins.factory;

import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Mapper;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancer;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancerFactory;
import org.ekbana.minikafka.plugins.loadbalancer.WeighedRoundRobinLB;

import java.util.Properties;

public class WeightedRoundRobinLoadBalancerFactory implements LoadBalancerFactory<Properties,Node, LBRequest> {

    @Override
    public String loadBalancerName() {
        return "weighted round robin";
    }

    @Override
    public LoadBalancer<Node, LBRequest> buildLoadBalancer(Properties properties) {
        return new WeighedRoundRobinLB();
    }

}
