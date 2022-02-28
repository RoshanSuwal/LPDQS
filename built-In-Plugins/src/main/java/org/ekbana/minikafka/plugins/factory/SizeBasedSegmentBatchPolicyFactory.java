package org.ekbana.minikafka.plugins.factory;

import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;
import org.ekbana.minikafka.plugin.policy.PolicyFactory;
import org.ekbana.minikafka.plugins.policy.SizeBasedSegmentBatchPolicy;

import java.util.Properties;

public class SizeBasedSegmentBatchPolicyFactory implements PolicyFactory<SegmentMetaData> {

    @Override
    public String policyName() {
        return "size-based-segment-batch-policy";
    }

    @Override
    public Policy<SegmentMetaData> buildPolicy(Properties properties) {
        long segmentSize=Long.parseLong(properties.getProperty("broker.segment.batch.size","5242880"));// 5MB by default
        return new SizeBasedSegmentBatchPolicy(segmentSize);
    }
}
