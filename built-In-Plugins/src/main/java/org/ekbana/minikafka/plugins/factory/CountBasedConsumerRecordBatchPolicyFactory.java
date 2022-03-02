package org.ekbana.minikafka.plugins.factory;

import org.ekbana.minikafka.common.ConsumerRecords;
import org.ekbana.minikafka.plugin.policy.Policy;
import org.ekbana.minikafka.plugin.policy.PolicyFactory;
import org.ekbana.minikafka.plugin.policy.PolicyType;
import org.ekbana.minikafka.plugins.policy.CountBasedConsumerRecordBatchPolicy;

import java.util.Properties;

public class CountBasedConsumerRecordBatchPolicyFactory implements PolicyFactory<ConsumerRecords> {

    @Override
    public String policyName() {
        return "offset-count-based-consumer-records-batch-policy";
    }

    @Override
    public PolicyType policyType() {
        return PolicyType.CONSUMER_RECORD_BATCH_POLICY;
    }

    @Override
    public Policy<ConsumerRecords> buildPolicy(Properties properties) {
        int consumerRecordsBatchOffsetCount=Integer.parseInt(properties.getProperty("broker.consumer.records.batch.count","10"));// 5MB by default
        return new CountBasedConsumerRecordBatchPolicy(consumerRecordsBatchOffsetCount);
    }
}
