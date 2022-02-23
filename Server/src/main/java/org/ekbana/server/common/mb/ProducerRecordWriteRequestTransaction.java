package org.ekbana.server.common.mb;

import lombok.Getter;
import org.ekbana.server.leader.Node;

import java.util.List;

@Getter
public class ProducerRecordWriteRequestTransaction extends RequestTransaction {
    private int partition;
    private List<?> producerRecords;

    public ProducerRecordWriteRequestTransaction(long transactionId,TransactionType.Action transactionType, Topic topic,int partition,List<?> producerRecords) {
        super(transactionId, transactionType, topic, TransactionType.RequestType.PRODUCER_RECORD_WRITE);
        this.partition=partition;
        this.producerRecords=producerRecords;
    }

    @Override
    public Node[] getPartitionNodes() {
        return new Node[]{getTopic().getDataNode()[partition]};
    }
}
