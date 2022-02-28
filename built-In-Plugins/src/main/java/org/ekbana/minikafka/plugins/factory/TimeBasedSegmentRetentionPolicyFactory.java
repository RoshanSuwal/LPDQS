package org.ekbana.minikafka.plugins.factory;

import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;
import org.ekbana.minikafka.plugin.policy.PolicyFactory;
import org.ekbana.minikafka.plugins.policy.TimeBasedSegmentRetentionPolicy;

import java.util.Properties;

public class TimeBasedSegmentRetentionPolicyFactory implements PolicyFactory<SegmentMetaData> {

    @Override
    public String policyName() {
        return "time-based-segment-retention-policy";
    }

    @Override
    public Policy<SegmentMetaData> buildPolicy(Properties properties) {
        long segmentLifeSpan=Long.parseLong(properties.getProperty("broker.segment.batch.lifespan","10000"));// 5MB by default
        return new TimeBasedSegmentRetentionPolicy(segmentLifeSpan);
    }
}
