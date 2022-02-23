package org.ekbana.server.common.mb;

import lombok.Getter;
import org.ekbana.server.leader.Node;

@Getter
public class ConsumerRecordReadRequestTransaction extends RequestTransaction {
    private int partition;
    private long offset;
    private boolean isTimeOffset;

    public ConsumerRecordReadRequestTransaction(long transactionId, TransactionType.Action transactionType, Topic topic, TransactionType.RequestType requestType,int partition,long offset,boolean isTimeOffset) {
        super(transactionId, transactionType, topic, requestType);
        this.partition=partition;
        this.offset=offset;
        this.isTimeOffset=isTimeOffset;
    }

    @Override
    public Node[] getPartitionNodes() {
        return new Node[]{getTopic().getDataNode()[partition]};
    }
}
