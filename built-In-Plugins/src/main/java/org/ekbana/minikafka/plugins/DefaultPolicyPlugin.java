package org.ekbana.minikafka.plugins;


import org.ekbana.minikafka.plugin.policy.PolicyFactory;
import org.ekbana.minikafka.plugin.policy.PolicyPlugin;
import org.ekbana.minikafka.plugins.factory.*;

import java.util.Arrays;
import java.util.List;

public class DefaultPolicyPlugin implements PolicyPlugin {

    @Override
    public String pluginName() {
        return "default policy plugins";
    }

    @Override
    public String pluginDescription() {
        return "contains all the pre-defined policy plugins";
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
}
