package org.ekbana.minikafka.plugins.factory;

import org.ekbana.minikafka.common.ConsumerRecords;
import org.ekbana.minikafka.plugin.policy.Policy;
import org.ekbana.minikafka.plugin.policy.PolicyFactory;
import org.ekbana.minikafka.plugin.policy.PolicyType;
import org.ekbana.minikafka.plugins.policy.SizeBasedConsumerRecordBatchPolicy;

import java.util.Properties;

public class SizeBasedConsumerRecordBatchPolicyFactory implements PolicyFactory<ConsumerRecords> {
    @Override
    public String policyName() {
        return "size-based-consumer-records-batch-policy";
    }

    @Override
    public PolicyType policyType() {
        return PolicyType.CONSUMER_RECORD_BATCH_POLICY;
    }

    @Override
    public Policy<ConsumerRecords> buildPolicy(Properties properties) {
        long consumerRecordsBatchSize=Long.parseLong(properties.getProperty("broker.consumer.records.batch.size","5242880"));// 5MB by default
        return new SizeBasedConsumerRecordBatchPolicy(consumerRecordsBatchSize);
    }
}
