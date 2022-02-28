package org.ekbana.minikafka.plugins.policy;

import org.ekbana.minikafka.common.ConsumerRecords;
import org.ekbana.minikafka.plugin.policy.Policy;

public class SizeBasedConsumerRecordBatchPolicy implements Policy<ConsumerRecords> {
    private final long batchSize;

    public SizeBasedConsumerRecordBatchPolicy(long batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public boolean validate(ConsumerRecords consumerRecords) {
        return consumerRecords.getSize()<batchSize;
    }
}
