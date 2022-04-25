package org.ekbana.server.common.mb;

import lombok.Getter;
import org.ekbana.minikafka.common.Node;

import java.util.List;

@Getter
public class ProducerRecordWriteRequestTransaction extends RequestTransaction {
    private int partition;
    private List<?> producerRecords;
    private Node[] partitionNode;

    public ProducerRecordWriteRequestTransaction(long transactionId,TransactionType.Action transactionType, Topic topic,int partition,List<?> producerRecords) {
        super(transactionId, transactionType, topic, TransactionType.RequestType.PRODUCER_RECORD_WRITE);
        this.partition=partition;
        this.producerRecords=producerRecords;
    }
    public  void setPartitionNode(Node node){
        this.partitionNode=new Node[]{node};
    }
    @Override
    public Node[] getPartitionNodes() {
        return partition==-1?null:new Node[]{getTopic().getDataNode()[partition]};
    }
}
