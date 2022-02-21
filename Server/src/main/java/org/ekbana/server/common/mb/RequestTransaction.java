package org.ekbana.server.common.mb;

import lombok.Getter;
import org.ekbana.server.leader.Node;

@Getter
public abstract class RequestTransaction extends Transaction {

    private final Topic topic;
    private final TransactionType.RequestType requestType;

    public RequestTransaction(long transactionId, TransactionType.Action transactionType, Topic topic, TransactionType.RequestType requestType) {
        super(transactionId, transactionType);
        this.topic = topic;
        this.requestType = requestType;
    }

    public abstract Node[] getPartitionNodes();
}