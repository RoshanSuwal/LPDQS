package org.ekbana.minikafka.plugins.policy;

import org.ekbana.minikafka.common.ConsumerRecords;
import org.ekbana.minikafka.plugin.policy.Policy;

public class CountBasedConsumerRecordBatchPolicy implements Policy<ConsumerRecords> {
    private final int recordCount;

    public CountBasedConsumerRecordBatchPolicy(int recordCount) {
        this.recordCount = recordCount;
    }

    @Override
    public boolean validate(ConsumerRecords consumerRecords) {
        return consumerRecords.getRecordsCount()<recordCount;
    }
}
