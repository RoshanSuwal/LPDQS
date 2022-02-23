package org.ekbana.server.common.mb;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class ConsumerRecordReadResponseTransaction extends Transaction{

    private ConsumerRecords consumerRecords;

    public ConsumerRecordReadResponseTransaction(long transactionId, TransactionType.Action action,ConsumerRecords consumerRecords) {
        super(transactionId, action);
        this.consumerRecords=consumerRecords;
    }
}
