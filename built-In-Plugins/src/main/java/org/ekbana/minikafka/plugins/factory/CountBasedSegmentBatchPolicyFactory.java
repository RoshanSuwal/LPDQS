package org.ekbana.minikafka.plugins.factory;

import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;
import org.ekbana.minikafka.plugin.policy.PolicyFactory;
import org.ekbana.minikafka.plugins.policy.CountBasedSegmentBatchPolicy;

import java.util.Properties;

public class CountBasedSegmentBatchPolicyFactory implements PolicyFactory<SegmentMetaData> {

    @Override
    public String policyName() {
        return "offset-count-based-segment-batch-policy";
    }

    @Override
    public Policy<SegmentMetaData> buildPolicy(Properties properties) {
        int offsetCount=Integer.parseInt(properties.getProperty("broker.segment.batch.count","10"));// 5MB by default
        return new CountBasedSegmentBatchPolicy(offsetCount);
    }
}
