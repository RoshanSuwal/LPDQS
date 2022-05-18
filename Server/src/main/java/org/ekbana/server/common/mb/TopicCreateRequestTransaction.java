package org.ekbana.server.common.mb;

import lombok.Getter;
import lombok.Setter;
import org.ekbana.minikafka.common.Node;

@Getter
@Setter
public class TopicCreateRequestTransaction extends RequestTransaction{
    private int[] partitions;

    public TopicCreateRequestTransaction( long transactionId, TransactionType.Action transactionType, Topic topic, TransactionType.RequestType requestType) {
        super(transactionId, transactionType, topic, requestType);
    }

    public int[] getPartitions(){
        return partitions;
    }

    @Override
    public Node[] getPartitionNodes() {
        return getTopic().getDataNode();
    }
}
