package org.ekbana.minikafka.plugins;


import org.ekbana.minikafka.plugin.Plugin;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancerFactory;
import org.ekbana.minikafka.plugin.policy.PolicyFactory;
import org.ekbana.minikafka.plugins.factory.*;

import java.util.Arrays;
import java.util.List;

public class DefaultPlugin implements Plugin {

    @Override
    public String pluginName() {
        return "default plugins";
    }

    @Override
    public String pluginDescription() {
        return "contains all the pre-defined plugins";
    }

    @Override
    public List<PolicyFactory<?>> getPolicyFactories() {
        return Arrays.asList(
                new CountBasedSegmentBatchPolicyFactory(),
                new SizeBasedSegmentBatchPolicyFactory(),
                new TimeBasedSegmentRetentionPolicyFactory(),
                new SizeBasedConsumerRecordBatchPolicyFactory(),
                new CountBasedConsumerRecordBatchPolicyFactory()
        );
    }

    @Override
    public List<LoadBalancerFactory<?, ?,?>> getLoadBalancerFactories() {
        return Arrays.asList(
                new WeightedRoundRobinLoadBalancerFactory(),
                new ConsistentHashingLoadBalancerFactory()
        );
    }
}
