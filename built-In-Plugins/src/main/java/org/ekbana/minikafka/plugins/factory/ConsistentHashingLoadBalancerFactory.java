package org.ekbana.minikafka.plugins.factory;

import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Mapper;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancer;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancerFactory;
import org.ekbana.minikafka.plugins.loadbalancer.ConsistentHashingLB;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ConsistentHashingLoadBalancerFactory implements LoadBalancerFactory<Mapper<String, Node>,Node, LBRequest> {
    @Override
    public String loadBalancerName() {
        return "consistent Hashing";
    }

    @Override
    public LoadBalancer<Node,LBRequest> buildLoadBalancer(Mapper<String, Node> nodeMapper) {
        final List<Long> hashes=new ArrayList<>();
        hashes.add(1L);
        hashes.add(11L);
        hashes.add(21L);
        hashes.add(31L);

        final Function<String,Long> hashFunction= id->{
            if (id.contains("000000")) return hashes.remove(0);
            else return Long.parseLong(id);
        };

        return new ConsistentHashingLB(nodeMapper,hashFunction,1);
    }
}
